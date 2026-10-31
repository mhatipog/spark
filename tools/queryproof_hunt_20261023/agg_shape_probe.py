"""Which data shapes make a pushed down global COUNT return NULL (N5 test design).

Usage: python agg_shape_probe.py
Prints the result with aggregate pushdown off and on, and the optimized plan with it on.
"""
import shutil
import tempfile

from pyspark.sql import SparkSession


def main():
    base = tempfile.mkdtemp(prefix="qp_aggs_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.sources.useV1SourceList", "").getOrCreate())
    print("SPARK_VERSION", spark.version)
    for fmt in ("parquet", "orc"):
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").format(fmt) \
            .save(f"{base}/{fmt}_range_rows")
        spark.range(0).selectExpr("id", "id % 3 as p").write.partitionBy("p").format(fmt) \
            .save(f"{base}/{fmt}_range_empty")
        spark.createDataFrame([(1, 10), (2, None)], "a INT, b INT").write.partitionBy("a") \
            .format(fmt).save(f"{base}/{fmt}_local_rows")
        cases = {
            "range_rows_p5": (spark.read.format(fmt).load(f"{base}/{fmt}_range_rows"),
                              "SELECT count(*), count(id) FROM t WHERE p = 5"),
            "range_rows_p5_schema": (spark.read.format(fmt).schema("id LONG, p LONG")
                                     .load(f"{base}/{fmt}_range_rows"),
                                     "SELECT count(*), count(id) FROM t WHERE p = 5"),
            "range_empty_schema": (spark.read.format(fmt).schema("id LONG, p LONG")
                                   .load(f"{base}/{fmt}_range_empty"),
                                   "SELECT count(*), count(id) FROM t"),
            "local_rows_a99_schema": (spark.read.format(fmt).schema("a INT, b INT")
                                      .load(f"{base}/{fmt}_local_rows"),
                                      "SELECT count(*), count(b) FROM t WHERE a = 99"),
            "local_rows_a99": (spark.read.format(fmt).load(f"{base}/{fmt}_local_rows"),
                               "SELECT count(*), count(b) FROM t WHERE a = 99"),
        }
        for name, (df, sql) in cases.items():
            df.createOrReplaceTempView("t")
            res = {}
            for push in ("false", "true"):
                spark.conf.set(f"spark.sql.{fmt}.aggregatePushdown", push)
                q = spark.sql(sql)
                res[push] = [tuple(r) for r in q.collect()]
                if push == "true":
                    plan = q._jdf.queryExecution().optimizedPlan().toString().replace("\n", " | ")
            spark.conf.unset(f"spark.sql.{fmt}.aggregatePushdown")
            print(f"AGGSHAPE {fmt:7s} {name:22s} off={res['false']} on={res['true']}")
            print(f"AGGPLAN  {fmt:7s} {name:22s} {plan[:500]}")
    spark.stop()
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
