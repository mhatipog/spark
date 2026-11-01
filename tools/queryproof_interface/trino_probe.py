"""Trino lead T3 (interface run 41286769809): ORC `o.i.c IS NULL` misses a row where o.i is NULL.

Writes small ORC files with pyarrow into BASE (mounted in the Trino container at /qpdata) and
compares Trino's answer with the expected one for several layouts, to tell row-group or
stripe skipping by statistics from a reader error. Prints TRINOPROBE lines.
Usage: python trino_probe.py BASE
"""
import json
import os
import sys

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import trino

base = sys.argv[1]
T = pa.struct([("i", pa.struct([("c", pa.int32())])), ("z", pa.int32())])
SCHEMA = pa.schema([("k", pa.int32()), ("o", T)])
ROWS = {1: None, 2: {"i": None, "z": 1}, 3: {"i": {"c": None}, "z": 2}, 4: {"i": {"c": 7}, "z": None}}
LAYOUTS = {
    "only_row2": [[2]],
    "row2_with_value": [[2, 4]],
    "row2_with_null_c": [[2, 3]],
    "one_file_per_row": [[1], [2], [3], [4]],
    "all_in_one": [[1, 2, 3, 4]],
}
QUERIES = {
    "c_isnull": ("o.i.c IS NULL", lambda r: r is None or r["i"] is None or r["i"]["c"] is None),
    "i_isnull": ("o.i IS NULL", lambda r: r is None or r["i"] is None),
    "c_notnull": ("o.i.c IS NOT NULL", lambda r: r is not None and r["i"] is not None and r["i"]["c"] is not None),
    "z_isnull": ("o.z IS NULL", lambda r: r is None or r["z"] is None),
}
cur = trino.dbapi.connect(host="localhost", port=8080, user="qp", catalog="hive", schema="qp").cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS hive.qp")
cur.fetchall()
wrong = 0
for fmt in ("orc", "parquet"):
    for name, files in LAYOUTS.items():
        d = os.path.join(base, "trino_probe", f"{fmt}_{name}")
        os.makedirs(d, exist_ok=True)
        for i, ks in enumerate(files):
            t = pa.table({"k": pa.array(ks, pa.int32()), "o": pa.array([ROWS[k] for k in ks], T)}, schema=SCHEMA)
            p = os.path.join(d, f"part-{i}.{fmt}")
            orc.write_table(t, p) if fmt == "orc" else pq.write_table(t, p)
        table = f"p_{fmt}_{name}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (k INTEGER, o ROW(i ROW(c INTEGER), z INTEGER)) "
                    f"WITH (external_location = 'local:///qpdata/trino_probe/{fmt}_{name}', format = '{fmt.upper()}')")
        cur.fetchall()
        present = [k for ks in files for k in ks]
        for qid, (pred, fn) in QUERIES.items():
            expected = sorted(k for k in present if fn(ROWS[k]))
            cur.execute(f"SELECT k FROM {table} WHERE {pred}")
            got = sorted(r[0] for r in cur.fetchall())
            bad = got != expected
            wrong += bad
            print("TRINOPROBE", fmt, name, qid, "WRONG" if bad else "same", "expected", expected, "got", got)
        if name == "only_row2":
            cur.execute(f"EXPLAIN SELECT k FROM {table} WHERE o.i.c IS NULL")
            plan = cur.fetchall()[0][0]
            print("TRINOPROBE_PLAN", fmt, json.dumps([ln.strip() for ln in plan.split("\n") if "o" in ln and
                                                       ("constraint" in ln.lower() or "ScanFilter" in ln)][:4]))
print("TRINOPROBE_SUMMARY differences", wrong)
