"""Z1 across Spark versions: Parquet pushdown on a TIMESTAMP_MILLIS column with a literal that has
sub-millisecond digits (guide 2m). Spark writes the files itself, one row per file, so each file's
min == max. Controls: pushdown off, the same rows as TIMESTAMP_MICROS, literals on the millisecond
grid. Prints one line per case; never fails the job."""
import shutil
import tempfile

import pyspark
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
print("Z1 PYSPARK", pyspark.__version__)
root = tempfile.mkdtemp()
dirs = {}
for unit in ("TIMESTAMP_MILLIS", "TIMESTAMP_MICROS"):
    d = f"{root}/z1_{unit.lower()}"
    spark.conf.set("spark.sql.parquet.outputTimestampType", unit)
    for i, t in enumerate(("1900-01-01 00:00:00", "2020-01-01 00:00:00")):
        spark.sql(f"SELECT TIMESTAMP'{t}' AS t").coalesce(1).write.mode("append" if i else "overwrite").parquet(d)
    dirs[unit] = d
preds = [("lt", "t < TIMESTAMP'1900-01-01 00:00:00.000001'", 1), ("ne", "t <> TIMESTAMP'2020-01-01 00:00:00.000001'", 2),
         ("not_in", "t NOT IN (TIMESTAMP'2020-01-01 00:00:00.000001')", 2),
         ("le", "t <= TIMESTAMP'1900-01-01 00:00:00.000001'", 1), ("gt", "t > TIMESTAMP'2019-12-31 23:59:59.999999'", 1),
         ("lt_ms_grid", "t < TIMESTAMP'1900-01-01 00:00:00.001'", 1), ("ne_ms_grid", "t <> TIMESTAMP'2020-01-01 00:00:00.001'", 2)]
for v1 in ("parquet", ""):
    spark.conf.set("spark.sql.sources.useV1SourceList", v1)
    for unit, d in dirs.items():
        for vec in ("true", "false"):
            for push in ("true", "false"):
                spark.conf.set("spark.sql.parquet.enableVectorizedReader", vec)
                spark.conf.set("spark.sql.parquet.filterPushdown", push)
                for tag, p, want in preds:
                    try:
                        n = spark.read.parquet(d).where(p).count()
                        res = f"{n} (expected {want})" + ("" if n == want else " WRONG")
                    except Exception as exc:  # noqa: BLE001
                        res = "error: " + str(exc).split("\n")[0][:160]
                    print(f"Z1 {'v1' if v1 else 'v2'} {unit} vectorized={vec} pushdown={push} {tag}: {res}")
spark.conf.set("spark.sql.sources.useV1SourceList", "parquet")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
plan = spark.read.parquet(dirs["TIMESTAMP_MILLIS"]).where(preds[0][1])._jdf.queryExecution().executedPlan().toString()
print("Z1 PLAN", [x for x in plan.split("\n") if "PushedFilters" in x or "Scan" in x][:2])
spark.stop()
shutil.rmtree(root, ignore_errors=True)
