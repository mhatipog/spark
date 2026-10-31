"""Minimal reproduction search for the CBO NumberFormatException in FilterEstimation.

Usage: python cbo_min.py OUT.json
A partitioned Parquet table, analyzed FOR ALL COLUMNS, queried with a range filter on
its partition column under spark.sql.cbo.enabled=true. Variants change the data (NULL
partition or not, one or several partition values), the statistics collected and the
predicate, to find what the failure needs.
"""
import json
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

DATA = {
    "mixed_with_null": "(1, -3), (2, -1), (3, 0), (4, 3), (5, NULL), (6, NULL)",
    "mixed_no_null": "(1, -3), (2, -1), (3, 0), (4, 3)",
    "single_value": "(1, 2), (2, 2)",
    "single_value_and_null": "(1, 2), (2, NULL)",
    "only_null": "(1, NULL), (2, NULL)",
}
STATS = {
    "all_columns": "ANALYZE TABLE p COMPUTE STATISTICS FOR ALL COLUMNS",
    "table_only": "ANALYZE TABLE p COMPUTE STATISTICS",
    "none": None,
}
PREDICATES = ["a < 1", "a > 0", "a <= 2", "a >= 2", "a = 2", "b < 3", "a < 1 AND b > 0"]


def run(spark, sql):
    try:
        spark.sql(sql).collect()
        return "ok"
    except Exception as exc:  # noqa: BLE001
        return str(exc).split("\n")[0][:160]


def main(out_path):
    wh = tempfile.mkdtemp(prefix="qp_cbomin_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    report = {}
    for dname, values in DATA.items():
        for sname, analyze in STATS.items():
            for partitioned in (True, False):
                spark.sql("DROP TABLE IF EXISTS p")
                spark.sql("CREATE TABLE p (b INT, a INT) USING parquet"
                          + (" PARTITIONED BY (a)" if partitioned else ""))
                spark.sql(f"INSERT INTO p VALUES {values}")
                if analyze:
                    spark.sql(analyze)
                spark.conf.set("spark.sql.cbo.enabled", "true")
                for pred in PREDICATES:
                    key = f"{dname} | {sname} | {'partitioned' if partitioned else 'plain'} | {pred}"
                    report[key] = run(spark, f"SELECT * FROM p WHERE {pred}")
                spark.conf.unset("spark.sql.cbo.enabled")
                fails = {k.split(' | ')[-1]: v for k, v in report.items()
                         if k.startswith(f"{dname} | {sname} | {'partitioned' if partitioned else 'plain'}")
                         and v != "ok"}
                print(dname, sname, "partitioned" if partitioned else "plain", "->", fails or "all ok")
    spark.sql("DROP TABLE IF EXISTS p")
    spark.sql("CREATE TABLE p (b INT, a INT) USING parquet PARTITIONED BY (a)")
    spark.sql(f"INSERT INTO p VALUES {DATA['mixed_with_null']}")
    spark.sql(STATS["all_columns"])
    report["describe_a"] = [list(r) for r in spark.sql("DESCRIBE EXTENDED p a").collect()]
    spark.conf.set("spark.sql.cbo.enabled", "true")
    report["explain_cost"] = spark.sql("EXPLAIN COST SELECT * FROM p").collect()[0][0][:3000]
    print("stats a:", report["describe_a"])
    print(report["explain_cost"][:1500])
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:2])
