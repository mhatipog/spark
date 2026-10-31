"""Standalone, minimized reproduction of the CBO FilterEstimation NumberFormatException.

Usage: python cbo_standalone.py OUT.json
Starts from the rows of R036_00669 (dataset 1) for ta (partitioned by a) and tc,
shrinks them while the query still fails under spark.sql.cbo.enabled=true, then
reports which ingredients matter (bucketing, ANALYZE scope, partitioning).
"""
import json
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

TA = [[1, 3, -3], [-1, 2, -2], [None, 0, 3], [-2, -2, None], [3, 1, -1], [3, 1, None], [0, -1, None],
      [0, None, 0], [-3, -1, -3], [None, 1, 2], [0, 1, 3], [-3, -3, None], [1, 1, 1], [-3, -1, 3],
      [None, -2, None]]
TC = [[-3, 0], [3, -2], [-3, 0], [-3, 0], [-3, 0]]
Q = "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0"


def setup(spark, ta, tc, part=True, bucket=True, analyze="all"):
    spark.sql("DROP TABLE IF EXISTS p")
    spark.sql("DROP TABLE IF EXISTS t")
    spark.sql("CREATE TABLE p (b INT, c INT, a INT) USING parquet" + (" PARTITIONED BY (a)" if part else ""))
    spark.sql("CREATE TABLE t (k INT, v INT) USING parquet"
              + (" CLUSTERED BY (k) SORTED BY (k) INTO 2 BUCKETS" if bucket else ""))
    if ta:
        spark.createDataFrame([(r[1], r[2], r[0]) for r in ta], "b int, c int, a int").write.insertInto("p")
    if tc:
        spark.createDataFrame([tuple(r) for r in tc], "k int, v int").write.insertInto("t")
    for name in ("p", "t"):
        if analyze == "all":
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR ALL COLUMNS")
        elif analyze == "table":
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS")


def fails(spark):
    spark.conf.set("spark.sql.cbo.enabled", "true")
    try:
        spark.sql(Q).collect()
        return None
    except Exception as exc:  # noqa: BLE001
        return str(exc).split("\n")[0][:200]
    finally:
        spark.conf.unset("spark.sql.cbo.enabled")


def main(out_path):
    wh = tempfile.mkdtemp(prefix="qp_cbos_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    ta, tc = [list(r) for r in TA], [list(r) for r in TC]
    setup(spark, ta, tc)
    first = fails(spark)
    print("start:", first)
    report = {"start": first}
    if first:
        for which in ("ta", "tc"):
            rows = ta if which == "ta" else tc
            i = 0
            while i < len(rows):
                trial = rows[:i] + rows[i + 1:]
                setup(spark, trial if which == "ta" else ta, trial if which == "tc" else tc)
                if fails(spark):
                    rows[:] = trial
                else:
                    i += 1
        report["ta"], report["tc"] = ta, tc
        print("minimal ta (a, b, c):", ta)
        print("minimal tc (k, v):", tc)
        for label, kw in {"no_bucketing": {"bucket": False}, "no_partitioning": {"part": False},
                          "analyze_table_only": {"analyze": "table"}, "no_analyze": {"analyze": None}}.items():
            setup(spark, ta, tc, **kw)
            report[label] = fails(spark)
            print(f"{label:20s}", report[label] or "ok")
        setup(spark, ta, tc)
        spark.conf.set("spark.sql.cbo.enabled", "true")
        try:
            report["explain_cost"] = spark.sql("EXPLAIN COST " + Q).collect()[0][0][:3000]
        except Exception as exc:  # noqa: BLE001
            report["explain_cost"] = "EXPLAIN failed: " + str(exc).split("\n")[0][:200]
        print(report["explain_cost"][:1500])
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:2])
