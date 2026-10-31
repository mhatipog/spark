"""Minimal reproduction search: dynamic partition pruning filter left in an interpreted Filter.

Usage: python dpp_probe.py OUT.json
Hunt cases R034_00754/R034_00886 fail with codegen off ("... IN dynamicpruning#N has
not finished") where a DPP filter, pushed through a UNION ALL, lands on a scan of a
non-partitioned table and stays in a FilterExec. Each variant runs with the default
configuration, with codegen off, and with codegen off and DPP off.
"""
import json
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

SETUP = [
    "CREATE TABLE p (x INT, k INT) USING parquet PARTITIONED BY (k)",
    "INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, NULL)",
    "CREATE TABLE b (k INT, v INT) USING parquet CLUSTERED BY (k) INTO 2 BUCKETS",
    "INSERT INTO b VALUES (1, 10), (2, 20), (5, 50)",
    "CREATE TABLE n (k INT, v INT) USING parquet",
    "INSERT INTO n VALUES (1, 10), (2, 20), (5, 50)",
    "CREATE TABLE d (k INT) USING parquet",
    "INSERT INTO d VALUES (1), (2), (7)",
]
VARIANTS = {
    "union_partitioned_bucketed": ("SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT k FROM b) u "
                                   "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
    "union_partitioned_plain": ("SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT k FROM n) u "
                                "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
    "union_expr_key": ("SELECT u.c FROM (SELECT k - 1 AS c FROM p UNION ALL SELECT k + 1 AS c FROM n) u "
                       "JOIN d ON u.c = d.k WHERE d.k IN (1, 2)"),
    "union_distinct": ("SELECT u.k FROM (SELECT k FROM p UNION SELECT k FROM n) u "
                       "JOIN d ON u.k = d.k WHERE d.k IN (1, 2)"),
    "union_agg_side": ("SELECT d.k FROM d JOIN (SELECT c, SUM(c) AS s FROM (SELECT k AS c FROM p UNION "
                       "SELECT k AS c FROM n) q GROUP BY c) a ON d.k = a.c WHERE d.k IN (1, 2)"),
    "partitioned_only": "SELECT p.k FROM p JOIN d ON p.k = d.k WHERE d.k IN (1, 2)",
}
CONFIGS = {
    "default": {},
    "no_codegen": {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN"},
    "wholestage_off": {"spark.sql.codegen.wholeStage": "false"},
    "no_codegen_aqe_off": {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN",
                           "spark.sql.adaptive.enabled": "false"},
    "no_codegen_no_dpp": {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN",
                          "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
}


def run(spark, sql):
    try:
        return {"rows": sorted([list(r) for r in spark.sql(sql).collect()], key=str)}
    except Exception as exc:  # noqa: BLE001
        c = getattr(exc, "java_exception", None)
        while c is not None and c.getCause() is not None:
            c = c.getCause()
        return {"error": (c.toString() if c is not None else str(exc)).split("\n")[0][:300]}


def main(out_path):
    wh = tempfile.mkdtemp(prefix="qp_dpp_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    for s in SETUP:
        spark.sql(s)
    report = {}
    for name, sql in VARIANTS.items():
        report[name] = {"sql": sql}
        for cname, conf in CONFIGS.items():
            for k, v in conf.items():
                spark.conf.set(k, v)
            report[name][cname] = run(spark, sql)
            if cname == "no_codegen":
                report[name]["plan"] = spark.sql("EXPLAIN FORMATTED " + sql).collect()[0][0][:5000]
            for k in conf:
                spark.conf.unset(k)
        print(name, {c: report[name][c].get("rows", report[name][c].get("error", ""))
                     for c in CONFIGS})
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:2])
