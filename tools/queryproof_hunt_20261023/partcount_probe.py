"""Round 8 lead: aggregate pushdown and COUNT/MIN/MAX of a partition column whose value is NULL.

A partitioned table whose partition column `a` is NULL in some rows
(__HIVE_DEFAULT_PARTITION__). COUNT(a) must not count those rows. Each query runs:
  v1          the V1 file source (no aggregate pushdown)
  v2          the V2 file source, aggregate pushdown off
  v2_agg      the V2 file source with {parquet,orc}.aggregatePushdown=true
for Parquet and ORC. Prints PARTCOUNT lines: format, query id, config, rows, pushed aggregation.
"""
import json
import os
import sys
import tempfile

from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PYSPARK", spark.version)
base = tempfile.mkdtemp(prefix="qp_partcount_")
rows = [(1, 10), (1, None), (None, 20), (None, None), (None, 30), (2, 40)]
df = spark.createDataFrame(rows, "a INT, v INT")
QUERIES = {
    "grouped": "SELECT a, COUNT(a), COUNT(*), COUNT(v) FROM t GROUP BY a",
    "global": "SELECT COUNT(a), COUNT(*), COUNT(v) FROM t",
    "global_a_only": "SELECT COUNT(a) FROM t",
    "where_null": "SELECT COUNT(a), COUNT(*) FROM t WHERE a IS NULL",
    "minmax": "SELECT MIN(a), MAX(a), COUNT(a) FROM t",
}
V2 = {"spark.sql.sources.useV1SourceList": ""}
wrong = 0
for fmt in ("parquet", "orc"):
    loc = os.path.join(base, fmt)
    df.write.format(fmt).partitionBy("a").save(loc)
    configs = {"v1": {}, "v2": V2, "v2_agg": {**V2, f"spark.sql.{fmt}.aggregatePushdown": "true"}}
    for qid, sql in QUERIES.items():
        ref = None
        for cname, conf in configs.items():
            for k, v in conf.items():
                spark.conf.set(k, v)
            spark.read.format(fmt).schema("v INT, a INT").load(loc).createOrReplaceTempView("t")
            q = spark.sql(sql)
            try:
                got = sorted(json.dumps(list(r)) for r in q.collect())
                plan = q._jdf.queryExecution().executedPlan().toString()
                pushed = [ln.strip() for ln in plan.split(",") if "PushedAggregation" in ln]
            except Exception as exc:  # noqa: BLE001
                got, pushed = ["error: " + str(exc).split("\n")[0][:160]], []
            ref = got if ref is None else ref
            bad = got != ref
            wrong += bad
            print("PARTCOUNT", fmt, qid, cname, "WRONG" if bad else "same", got, pushed[:1])
            spark.catalog.dropTempView("t")
            for k in conf:
                spark.conf.unset(k)
print("PARTCOUNT_SUMMARY", spark.version, "differences", wrong)
spark.stop()
sys.exit(0)
