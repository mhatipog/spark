"""Minimal reproduction of the CBO FilterEstimation NumberFormatException, with variants.

Usage: python cbo_final.py
"""
import shutil
import tempfile

from pyspark.sql import SparkSession

VARIANTS = {
    "repro": "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0",
    "explicit_both_filters": "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND p.a > 0 AND t.k > 0",
    "p_filter_only": "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1",
    "t_filter_only": "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE t.k > 0",
    "no_join": "SELECT a FROM p WHERE a < 1 AND a > 0",
    "left_join": "SELECT p.a FROM p LEFT JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0",
    "nonempty_range": "SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 2 AND t.k > 0",
}


def main():
    wh = tempfile.mkdtemp(prefix="qp_cbof_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    print("SPARK_VERSION", spark.version)
    for s in ["CREATE TABLE p (b INT, c INT, a INT) USING parquet PARTITIONED BY (a)",
              "INSERT INTO p VALUES (1, 1, 1), (-1, 3, -3)",
              "CREATE TABLE t (k INT, v INT) USING parquet",
              "ANALYZE TABLE p COMPUTE STATISTICS FOR ALL COLUMNS",
              "ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS"]:
        spark.sql(s)
    for cbo in ("false", "true"):
        spark.conf.set("spark.sql.cbo.enabled", cbo)
        for name, sql in VARIANTS.items():
            try:
                res = f"ok {[tuple(r) for r in spark.sql(sql).collect()]}"
            except Exception as exc:  # noqa: BLE001
                res = "ERROR " + str(exc).split("\n")[0][:140]
            print(f"CBO_RESULT cbo={cbo} {name:22s} {res}")
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)


if __name__ == "__main__":
    main()
