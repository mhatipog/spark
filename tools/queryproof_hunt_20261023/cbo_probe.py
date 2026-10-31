"""Isolate the CBO NumberFormatException ("Character N is neither a decimal digit ...").

Usage: python cbo_probe.py CASES.json OUT.json CASE:DATASET
Loads the case's tables exactly as run_meta.py does, then runs the query and smaller
variants under subsets of the CBO settings, with PySpark JVM stack traces enabled, and
also with the tables re-written without partitioning and without ANALYZE.
"""
import json
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

import run_meta as M

CBO = {"spark.sql.cbo.enabled": "true"}
SUBSETS = {
    "cbo": CBO,
    "cbo+planStats": {**CBO, "spark.sql.cbo.planStats.enabled": "true"},
    "cbo+joinReorder": {**CBO, "spark.sql.cbo.joinReorder.enabled": "true"},
    "cbo+starSchema": {**CBO, "spark.sql.cbo.starSchemaDetection": "true"},
    "all": M.VARIANTS.get("cbo_reorder") or M.WIDE_VARIANTS["cbo_reorder"],
}


def run(spark, sql):
    try:
        spark.sql(sql).collect()
        return "ok"
    except Exception as exc:  # noqa: BLE001
        return str(exc)[:6000]


def main(cases_path, out_path, spec):
    cid, d = spec.split(":")
    case = {c["id"]: c for c in json.loads(open(cases_path).read())["cases"]}[cid]
    wh = tempfile.mkdtemp(prefix="qp_cbo_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").config("spark.sql.warehouse.dir", wh)
             .config("spark.sql.pyspark.jvmStacktrace.enabled", "true").getOrCreate())
    dataset = case["datasets"][int(d)]
    M.write_tables(spark, dataset)
    sql = case["sql"]
    report = {"sql": sql, "runs": {}}
    for name, conf in SUBSETS.items():
        for k, v in conf.items():
            spark.conf.set(k, v)
        report["runs"][name] = run(spark, sql)
        print(name, "->", report["runs"][name][:200].replace("\n", " "))
        for k in conf:
            spark.conf.unset(k)
    # Show the column statistics the CBO reads.
    for t in ("ta", "tb", "tc"):
        for col in [c["name"] for c in dataset[t]["columns"]]:
            rows = spark.sql(f"DESCRIBE EXTENDED {t} {col}").collect()
            report.setdefault("stats", {})[f"{t}.{col}"] = {r[0]: r[1] for r in rows}
    print("stats:", json.dumps(report["stats"], default=str)[:3000])
    for k, v in SUBSETS["all"].items():
        spark.conf.set(k, v)
    full = report["runs"]["all"]
    if full != "ok":
        print("FULL JVM ERROR:\n" + full[:5000])
    # Same data, plain (unpartitioned, unbucketed) tables, with and without ANALYZE.
    saved = dict(M.LAYOUT)
    for label, analyze in (("plain+analyze", True), ("plain_no_analyze", False)):
        M.LAYOUT.clear()
        for name in dataset:
            M.LAYOUT[name] = ("plain", None)
        if analyze:
            M.write_tables(spark, dataset)
        else:
            for name, t in dataset.items():
                schema = ", ".join(f"{c['name']} {M.SPARK_TYPES[c['type']]}" for c in t["columns"])
                spark.sql(f"DROP TABLE IF EXISTS {name}")
                spark.createDataFrame(M.typed_rows(t), schema).write.format("parquet").saveAsTable(name)
        report["runs"][label] = run(spark, sql)
        print(label, "->", report["runs"][label][:200].replace("\n", " "))
    M.LAYOUT.update(saved)
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1, default=str)


if __name__ == "__main__":
    main(*sys.argv[1:4])
