"""Trino lead TD (pair two, run 41302131902): reading a TINYINT or SMALLINT Parquet column
written with DELTA_BINARY_PACKED fails with "byte overflow" / "short overflow".

Writes the files with pyarrow (INT32 physical, INT(8) or INT(16) logical type, DELTA_BINARY_PACKED,
no dictionary) into BASE (mounted in the Trino container at /qpdata). Each layout is read with
Trino's default session and checked against the values written. Layouts:
  pair       two values whose delta is outside the logical range (one delta in the miniblock)
  tail_jump  33 equal values then a jump: the last miniblock holds one delta, the block minimum
  ramp       small deltas only (negative control)
  random     5,000 uniform values (large input), several page sizes
Prints TRINODELTA lines and a TRINODELTA_SUMMARY line.
Usage: python trino_delta_probe.py BASE
"""
import os
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import trino

base = sys.argv[1]
rng = random.Random(7)
SPECS = {"tinyint": (pa.int8(), -128, 127), "smallint": (pa.int16(), -32768, 32767)}
cur = trino.dbapi.connect(host="localhost", port=8080, user="qp", catalog="hive", schema="qp").cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS hive.qp")
cur.fetchall()
cur.execute("SELECT node_version FROM system.runtime.nodes")
print("TRINO", cur.fetchall()[0][0])
wrong = 0
for sql_type, (pa_type, lo, hi) in SPECS.items():
    layouts = {
        "pair": ([lo, hi], {}),
        "pair_reverse": ([hi, lo], {}),
        "tail_jump": ([hi] * 33 + [lo], {}),
        "ramp": (list(range(0, 100)), {}),
        "random_page_default": ([rng.randint(lo, hi) for _ in range(5000)], {}),
        "random_page_1000": ([rng.randint(lo, hi) for _ in range(5000)], {"data_page_size": 1000}),
    }
    for name, (vals, opts) in layouts.items():
        d = os.path.join(base, "trino_delta", f"{sql_type}_{name}")
        os.makedirs(d, exist_ok=True)
        t = pa.table({"k": pa.array(range(len(vals)), pa.int32()), "b": pa.array(vals, pa_type)})
        pq.write_table(t, os.path.join(d, "part-0.parquet"), use_dictionary=False,
                       column_encoding={"b": "DELTA_BINARY_PACKED"}, data_page_version="2.0", **opts)
        md = pq.ParquetFile(os.path.join(d, "part-0.parquet")).metadata.row_group(0).column(1)
        table = f"d_{sql_type}_{name}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (k INTEGER, b {sql_type.upper()}) "
                    f"WITH (external_location = 'local:///qpdata/trino_delta/{sql_type}_{name}', format = 'PARQUET')")
        cur.fetchall()
        for qid, sql, expected in (("count_sum", f"SELECT count(*), sum(b) FROM {table}", [[len(vals), sum(vals)]]),
                                   ("rows", f"SELECT k, b FROM {table} ORDER BY k", [[i, v] for i, v in enumerate(vals)])):
            try:
                cur.execute(sql)
                got = [list(r) for r in cur.fetchall()]
                res = "same" if got == expected else "WRONG"
            except Exception as exc:  # noqa: BLE001 - the error is the finding
                got, res = None, "ERROR " + str(getattr(exc, "message", exc))[:80]
            wrong += res != "same"
            print("TRINODELTA", sql_type, name, qid, res, "n", len(vals), "encodings", list(md.encodings))
print("TRINODELTA_SUMMARY differences", wrong)
