"""N1 under the default configuration: DPP pushed into a UNION branch whose key is a lambda.

Usage: python dpp_default_probe.py
Higher-order functions are evaluated without whole-stage codegen, so the DPP predicate on
the non-partitioned branch is evaluated per row in tasks even with default settings.
"""
import shutil
import tempfile

from pyspark.sql import SparkSession

SETUP = [
    "CREATE TABLE p (x INT, k INT) USING parquet PARTITIONED BY (k)",
    "INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, NULL)",
    "CREATE TABLE n (arr ARRAY<INT>) USING parquet",
    "INSERT INTO n VALUES (array(1)), (array(1, 1)), (array(2, 3)), (NULL)",
    "CREATE TABLE d (k INT) USING parquet",
    "INSERT INTO d VALUES (1), (2), (7)",
]
QUERIES = {
    "lambda_key": ("SELECT u.k FROM (SELECT k FROM p UNION ALL "
                   "SELECT aggregate(arr, 0, (acc, e) -> acc + e) AS k FROM n) u "
                   "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
    "transform_key": ("SELECT u.k FROM (SELECT k FROM p UNION ALL "
                      "SELECT element_at(transform(arr, e -> e + 0), 1) AS k FROM n) u "
                      "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
    "plain_key": ("SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT size(arr) AS k FROM n) u "
                  "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
}
CONFIGS = {"default": {}, "aqe_off": {"spark.sql.adaptive.enabled": "false"},
           "no_dpp": {"spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"}}


def main():
    wh = tempfile.mkdtemp(prefix="qp_dppd_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    print("SPARK_VERSION", spark.version)
    for s in SETUP:
        spark.sql(s)
    for qname, sql in QUERIES.items():
        for cname, conf in CONFIGS.items():
            for k, v in conf.items():
                spark.conf.set(k, v)
            try:
                res = "ok " + str(sorted([tuple(r) for r in spark.sql(sql).collect()], key=str))
            except Exception as exc:  # noqa: BLE001
                c = getattr(exc, "java_exception", None)
                while c is not None and c.getCause() is not None:
                    c = c.getCause()
                res = "ERROR " + (c.toString() if c is not None else str(exc)).split("\n")[0][:160]
            print(f"DPPDEFAULT {qname:14s} {cname:8s} {res}")
            for k in conf:
                spark.conf.unset(k)
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)


if __name__ == "__main__":
    main()
