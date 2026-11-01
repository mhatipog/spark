"""Baseline lead (run 41293662806, shape R002_026): Spark reads wrong values of `c1.i.c` from a
pyarrow-written ORC file where `c1 STRUCT<i: STRUCT<c: INT>, z: INT>` has a NULL `c1.i` in one row
and a NULL `c1` in another, with filter pushdown off.

Writes the rows with pyarrow (C++ ORC writer) and with Spark (ORC Java writer), then reads
each file with Spark's vectorized and row-based readers and with the ORC Java API directly.
Prints ORCREAD lines and an ORCREAD_SUMMARY line.
"""
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.orc as orc
from pyspark.sql import SparkSession

T = pa.struct([("i", pa.struct([("c", pa.int32())])), ("z", pa.int32())])
ROWS = [(3, {"i": {"c": 41}, "z": -86}), (6, {"i": {"c": 61}, "z": -43}), (1, {"i": None, "z": 79}),
        (5, None), (2, {"i": {"c": 52}, "z": -4})]
EXPECTED = sorted((k, None if v is None or v["i"] is None else v["i"]["c"], None if v is None else v["z"])
                  for k, v in ROWS)
DDL = "k INT, c1 STRUCT<i: STRUCT<c: INT>, z: INT>"

spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.orc.filterPushdown", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PYSPARK", spark.version, "pyarrow", pa.__version__)
base = tempfile.mkdtemp(prefix="qp_orcread_")
paths = {"pyarrow": os.path.join(base, "pyarrow"), "spark": os.path.join(base, "spark")}
os.makedirs(paths["pyarrow"])
orc.write_table(pa.table({"k": pa.array([r[0] for r in ROWS], pa.int32()), "c1": pa.array([r[1] for r in ROWS], T)}),
                os.path.join(paths["pyarrow"], "a.orc"))
spark.createDataFrame([(k, None if v is None else (None if v["i"] is None else (v["i"]["c"],), v["z"]))
                       for k, v in ROWS], DDL).coalesce(1).write.orc(paths["spark"])
wrong = 0
for writer, path in paths.items():
    for vec in ("true", "false"):
        spark.conf.set("spark.sql.orc.enableVectorizedReader", vec)
        got = sorted(tuple(r) for r in spark.read.schema(DDL).orc(path).selectExpr("k", "c1.i.c", "c1.z").collect())
        bad = got != EXPECTED
        wrong += bad
        print("ORCREAD", writer, "vectorized=" + vec, "WRONG" if bad else "same", "got", got)
    # The ORC Java API itself, column vectors of the first batch.
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    it = p.getFileSystem(conf).listFiles(p, False)
    while it.hasNext():
        f = it.next().getPath()
        if not f.getName().endswith(".orc"):
            continue
        reader = jvm.org.apache.orc.OrcFile.createReader(f, jvm.org.apache.orc.OrcFile.readerOptions(conf))
        batch = reader.getSchema().createRowBatch()
        rr = reader.rows()
        rr.nextBatch(batch)
        k, c1 = batch.cols[0], batch.cols[1]
        i, z = c1.fields[0], c1.fields[1]
        c = i.fields[0]
        out = []
        for r in range(batch.size):
            def get(col, idx):
                return None if (not col.noNulls and col.isNull[idx]) else col.vector[idx]
            c1null = not c1.noNulls and c1.isNull[r]
            inull = c1null or (not i.noNulls and i.isNull[r])
            out.append((get(k, r), None if inull else get(c, r), None if c1null else get(z, r)))
        rr.close()
        got = sorted(out)
        bad = got != EXPECTED
        wrong += bad
        print("ORCREAD", writer, "orc-java-api", "WRONG" if bad else "same", "got", got,
              "file_version", reader.getFileVersion().getName(), "writer", reader.getSoftwareVersion())
print("ORCREAD_EXPECTED", EXPECTED)
print("ORCREAD_SUMMARY", spark.version, "differences", wrong)
spark.stop()
sys.exit(0)
