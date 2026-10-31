"""Aggregate pushdown (DSv2 Parquet/ORC) over a table with no rows (round 7 lead).

Usage: python agg_empty_probe.py
A global aggregate always returns one row: COUNT is 0 and MIN/MAX are NULL over no rows.
Checks plain and partitioned layouts, empty from the start and empty after a partition
filter, with spark.sql.{parquet,orc}.aggregatePushdown on and off.
"""
import shutil
import tempfile

from pyspark.sql import SparkSession

AGGS = ["COUNT(*)", "COUNT(b)", "MAX(b)", "MIN(b)", "COUNT(*), MAX(b)"]


def main():
    base = tempfile.mkdtemp(prefix="qp_agge_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.sources.useV1SourceList", "").getOrCreate())
    print("SPARK_VERSION", spark.version)
    schema = "a INT, b INT"
    empty = spark.createDataFrame([], schema)
    rows = spark.createDataFrame([(1, 10), (2, None)], schema)
    for fmt in ("parquet", "orc"):
        layouts = {
            "plain_empty": (empty, None, None),
            "partitioned_empty": (empty, "a", None),
            "partitioned_all_pruned": (rows, "a", "a = 99"),
        }
        for lname, (df, part, where) in layouts.items():
            loc = f"{base}/{fmt}_{lname}"
            w = df.write.format(fmt).mode("overwrite")
            (w.partitionBy(part) if part else w).save(loc)
            spark.read.format(fmt).schema(schema).load(loc).createOrReplaceTempView("t")
            for agg in AGGS:
                sql = f"SELECT {agg} FROM t" + (f" WHERE {where}" if where else "")
                res = {}
                for push in ("false", "true"):
                    spark.conf.set(f"spark.sql.{fmt}.aggregatePushdown", push)
                    try:
                        res[push] = [tuple(r) for r in spark.sql(sql).collect()]
                    except Exception as exc:  # noqa: BLE001
                        res[push] = "ERROR " + str(exc).split("\n")[0][:120]
                spark.conf.unset(f"spark.sql.{fmt}.aggregatePushdown")
                verdict = "ok" if res["true"] == res["false"] else "WRONG"
                print(f"AGGEMPTY {fmt:7s} {lname:22s} {agg:17s} off={res['false']} on={res['true']} {verdict}")
    spark.stop()
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
