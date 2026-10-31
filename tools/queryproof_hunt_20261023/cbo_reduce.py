"""Hand-reduced variants of R036_00669 on its own tables, under spark.sql.cbo.enabled=true.

Usage: python cbo_reduce.py CASES.json OUT.json CASE:DATASET
"""
import json
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

import run_meta as M

VARIANTS = {
    "original": None,
    "ta_branch_join": ("SELECT d2.a FROM ta d2 JOIN tc d5 ON d2.a = d5.k WHERE d2.a < 1 AND d5.k > 0"),
    "ta_empty_range": "SELECT * FROM ta WHERE a < 1 AND a > 0",
    "ta_empty_range_b": "SELECT * FROM ta WHERE a > 0 AND a < 1",
    "ta_empty_range_le": "SELECT * FROM ta WHERE a <= 0 AND a >= 1",
    "ta_range_nonempty": "SELECT * FROM ta WHERE a < 2 AND a > 0",
    "ta_eq_and_range": "SELECT * FROM ta WHERE a = 2 AND a < 1",
    "ta_then_count": "SELECT COUNT(*) FROM ta WHERE a < 1 AND a > 0",
    "tb_branch_join": ("SELECT d1.a + 1 FROM tb d1 JOIN tc d5 ON d1.a + 1 = d5.k WHERE d5.k > 0"),
    "union_ta_tc": ("SELECT u.c0 FROM (SELECT a AS c0 FROM ta WHERE a < 1 UNION ALL SELECT v AS c0 FROM tc) u "
                    "JOIN tc d5 ON u.c0 = d5.k WHERE d5.k > 0"),
    "nonpartition_empty_range": "SELECT * FROM ta WHERE b < 1 AND b > 0",
    "tc_empty_range": "SELECT * FROM tc WHERE k < 1 AND k > 0",
}


def run(spark, sql):
    try:
        spark.sql(sql).collect()
        return "ok"
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        i = msg.find("at org.apache.spark")
        return msg.split("\n")[0][:140] + (" @ " + msg[i:i + 160].split("\n")[0] if i >= 0 else "")


def main(cases_path, out_path, spec):
    cid, d = spec.split(":")
    case = {c["id"]: c for c in json.loads(open(cases_path).read())["cases"]}[cid]
    wh = tempfile.mkdtemp(prefix="qp_cbor_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh)
             .config("spark.sql.pyspark.jvmStacktrace.enabled", "true").getOrCreate())
    M.write_tables(spark, case["datasets"][int(d)])
    report = {}
    for cbo in ("false", "true"):
        spark.conf.set("spark.sql.cbo.enabled", cbo)
        for name, sql in VARIANTS.items():
            report[f"cbo={cbo} {name}"] = run(spark, sql or case["sql"])
            print(f"cbo={cbo} {name:26s}", report[f"cbo={cbo} {name}"])
    spark.conf.set("spark.sql.cbo.enabled", "true")
    report["explain_cost_nonempty"] = spark.sql(
        "EXPLAIN COST SELECT * FROM ta WHERE a < 2 AND a > 0").collect()[0][0][:2000]
    print(report["explain_cost_nonempty"][:1200])
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
