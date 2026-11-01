"""Baseline lead (run 41293662806, shape R002_026): Spark reads wrong values of `c1.i.c` from
pyarrow-written ORC files, with filter pushdown off.

The exact shape of the baseline (three files, one of them empty, columns k, c0, c1, c2), and
variants that drop one ingredient each, are written with pyarrow (C++ ORC writer). Each is read
with Spark's vectorized and row-based readers; a mismatch with the rows written is WRONG.
Prints ORCREAD lines and an ORCREAD_SUMMARY line.
"""
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.orc as orc
from pyspark.sql import SparkSession

C1 = pa.struct([("i", pa.struct([("c", pa.int32())])), ("z", pa.int32())])
C2 = pa.struct([("x", pa.int32()), ("y", pa.string())])
ROWS = [(1, None, {"i": None, "z": 79}, {"x": None, "y": ""}),
        (2, -113734, {"i": {"c": 52}, "z": -4}, {"x": 25, "y": "czaxa"}),
        (3, 398326, {"i": {"c": 41}, "z": -86}, {"x": -32, "y": None}),
        (4, 854532, {"i": {"c": 95}, "z": -70}, {"x": -1, "y": ""}),
        (5, 343863, None, {"x": None, "y": "bbxc"}),
        (6, 157600, {"i": {"c": 61}, "z": -43}, {"x": 34, "y": ""}),
        (7, None, {"i": {"c": 8}, "z": None}, {"x": -56, "y": "yayba"})]
FILES = [[], [4, 1], [3, 6, 0, 5, 2]]
VARIANTS = {
    "exact": dict(files=FILES, cols=("k", "c0", "c1", "c2")),
    "no_empty_file": dict(files=FILES[1:], cols=("k", "c0", "c1", "c2")),
    "one_file": dict(files=[FILES[1] + FILES[2]], cols=("k", "c0", "c1", "c2")),
    "no_c2": dict(files=FILES, cols=("k", "c0", "c1")),
    "no_c0": dict(files=FILES, cols=("k", "c1", "c2")),
    "only_k_c1": dict(files=FILES, cols=("k", "c1")),
}
TYPES = {"k": pa.int32(), "c0": pa.int64(), "c1": C1, "c2": C2}
DDL = {"k": "k INT", "c0": "c0 BIGINT", "c1": "c1 STRUCT<i: STRUCT<c: INT>, z: INT>",
       "c2": "c2 STRUCT<x: INT, y: STRING>"}

spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.orc.filterPushdown", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PYSPARK", spark.version, "pyarrow", pa.__version__)
base = tempfile.mkdtemp(prefix="qp_orcread_")
wrong = 0
for name, v in VARIANTS.items():
    d = os.path.join(base, name)
    os.makedirs(d)
    cols = v["cols"]
    present = []
    for fi, idx in enumerate(v["files"]):
        data = {c: pa.array([ROWS[i][("k", "c0", "c1", "c2").index(c)] for i in idx], TYPES[c]) for c in cols}
        orc.write_table(pa.table(data), os.path.join(d, f"part-{fi}.orc"))
        present += idx
    expected = sorted((ROWS[i][0], None if ROWS[i][2] is None or ROWS[i][2]["i"] is None else ROWS[i][2]["i"]["c"],
                       None if ROWS[i][2] is None else ROWS[i][2]["z"]) for i in present)
    for vec in ("true", "false"):
        spark.conf.set("spark.sql.orc.enableVectorizedReader", vec)
        df = spark.read.schema(", ".join(DDL[c] for c in cols)).orc(d)
        got = sorted(tuple(r) for r in df.selectExpr("k", "c1.i.c", "c1.z").collect())
        bad = got != expected
        wrong += bad
        print("ORCREAD", name, "vectorized=" + vec, "WRONG" if bad else "same", "got", got,
              "" if not bad else f"expected {expected}")
print("ORCREAD_SUMMARY", spark.version, "differences", wrong)
spark.stop()
sys.exit(0)
