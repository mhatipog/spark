"""QueryProof bug hunt runner: four oracles per query and dataset.

Usage: python run_hunt.py CASES.json OUT.json [SHARD COUNT]
  FULL      Spark with its full optimizer
  MINIMAL   Spark with every excludable optimizer rule excluded
  NOCODEGEN Spark with its full optimizer, whole-stage codegen off and the
            interpreted expression path (spark.sql.codegen.factoryMode=NO_CODEGEN)
  SQLITE    independent engine (`<=>` translated to IS)
RewriteWithExpression stays enabled in MINIMAL: excluding it alone crashes NULLIF in
4.2.0, a known issue fixed upstream by SPARK-59019 (non-excludable on master).
One SparkSession for the batch. When the JVM was started with the QueryProof
overlay (QUERYPROOF_OVERLAY), the FULL run on the first dataset of each query
is traced with its query id, for kernel and event-level differential triage.
"""
import json
import os
import sqlite3
import sys
import time
from collections import Counter

from pyspark.sql import SparkSession

SPARK_CONFIGS = ("full", "minimal", "nocodegen")


def bag(rows):
    return Counter(tuple(r) for r in rows)


def sql_type(column, engine):
    """Column type for the engine: integer by default, string when declared."""
    if column.get("type") == "string":
        return "string" if engine == "spark" else "TEXT"
    return "int" if engine == "spark" else "INTEGER"


def spark_schema(t):
    return ", ".join(f"{c['name']} {sql_type(c, 'spark')}" for c in t["columns"])


def spark_rows(spark, sql):
    try:
        return {"rows": [list(r) for r in spark.sql(sql).collect()]}
    except Exception as exc:                       # noqa: BLE001 - recorded, not hidden
        return {"error": type(exc).__name__ + ": " + str(exc).split("\n")[0][:240]}


def sqlite_rows(dataset, sql):
    db = sqlite3.connect(":memory:")
    for name, t in dataset.items():
        cols = [c["name"] for c in t["columns"]]
        db.execute(f"CREATE TABLE {name} ({', '.join(c['name'] + ' ' + sql_type(c, 'sqlite') for c in t['columns'])})")
        if t["rows"]:
            db.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(cols))})", t["rows"])
    try:
        return {"rows": [list(r) for r in db.execute(sql.replace("<=>", "IS")).fetchall()]}
    except sqlite3.Error as exc:
        return {"error": "sqlite: " + str(exc)[:240]}


def main(cases_path, out_path, shard=0, count=1):
    shard, count = int(shard), int(count)
    overlay = os.environ.get("QUERYPROOF_OVERLAY")
    builder = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2"))
    if overlay:
        builder = builder.config("spark.driver.extraClassPath", overlay)
    spark = builder.getOrCreate()
    jvm = spark._jvm
    optimizer = spark._jsparkSession.sessionState().optimizer()
    names = set()
    batches = optimizer.defaultBatches()
    for i in range(batches.size()):
        rules = batches.apply(i).rules()
        for j in range(rules.size()):
            names.add(rules.apply(j).ruleName())
    non_ex = optimizer.nonExcludableRules()
    keep = {non_ex.apply(i) for i in range(non_ex.size())}
    keep.add("org.apache.spark.sql.catalyst.optimizer.RewriteWithExpression")  # SPARK-59019
    excluded = sorted(names - keep)
    doc = json.loads(open(cases_path).read())
    results = []
    t0 = time.time()
    for case in doc["cases"][shard::count]:
        print("QUERYPROOF_CASE", case["id"], file=sys.stderr, flush=True)  # locates a crash
        rec = {"id": case["id"], "family": case["family"], "sql": case["sql"], "runs": []}
        for d, dataset in enumerate(case["datasets"]):
            for name, t in dataset.items():
                spark.createDataFrame([tuple(r) for r in t["rows"]], spark_schema(t)).createOrReplaceTempView(name)
            trace = bool(overlay) and d == 0
            jvm.java.lang.System.setProperty("queryproof.query.id", case["id"])
            jvm.java.lang.System.setProperty("queryproof.ast.enabled", "true" if trace else "false")
            spark.conf.unset("spark.sql.optimizer.excludedRules")
            out = {"full": spark_rows(spark, case["sql"])}
            jvm.java.lang.System.setProperty("queryproof.ast.enabled", "false")
            spark.conf.set("spark.sql.optimizer.excludedRules", ",".join(excluded))
            out["minimal"] = spark_rows(spark, case["sql"])
            spark.conf.unset("spark.sql.optimizer.excludedRules")
            spark.conf.set("spark.sql.codegen.wholeStage", "false")
            spark.conf.set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
            out["nocodegen"] = spark_rows(spark, case["sql"])
            spark.conf.unset("spark.sql.codegen.wholeStage")
            spark.conf.unset("spark.sql.codegen.factoryMode")
            out["sqlite"] = (sqlite_rows(dataset, case["sql"]) if case.get("sqlite", True)
                             else {"error": "skipped: Spark-only case"})
            run = {"dataset": d}
            for label, res in out.items():
                run[label] = res.get("error", "ok")
            full = out["full"]
            for other in ("minimal", "nocodegen", "sqlite"):
                run[f"full_vs_{other}"] = (bag(full["rows"]) == bag(out[other]["rows"])
                                           if "rows" in full and "rows" in out[other] else None)
            if (False in (run["full_vs_minimal"], run["full_vs_nocodegen"], run["full_vs_sqlite"])
                    or len({run[k] == "ok" for k in SPARK_CONFIGS}) > 1):
                run["rows"] = out
            rec["runs"].append(run)
        results.append(rec)
    spark.stop()
    runs = [(r, x) for r in results for x in r["runs"]]
    summary = {
        "queries": len(results), "runs": len(runs),
        "seconds": round(time.time() - t0, 1), "excluded_rules": len(excluded),
        "full_vs_minimal_mismatch": sum(1 for _, x in runs if x["full_vs_minimal"] is False),
        "full_vs_nocodegen_mismatch": sum(1 for _, x in runs if x["full_vs_nocodegen"] is False),
        "full_vs_sqlite_mismatch": sum(1 for _, x in runs if x["full_vs_sqlite"] is False),
        "error_disagreement": sum(1 for _, x in runs if len({x[k] == "ok" for k in SPARK_CONFIGS}) > 1),
        "spark_errors": dict(Counter(x["full"].split(":")[0] for _, x in runs if x["full"] != "ok")),
        "sqlite_errors": sum(1 for _, x in runs if x["sqlite"] != "ok"),
        "full_ok_sqlite_error_by_family": dict(Counter(r["family"] for r, x in runs
                                                       if x["full"] == "ok" and x["sqlite"] != "ok")),
    }
    with open(out_path, "w", newline="\n") as f:
        json.dump({"summary": summary, "excluded_rules": excluded, "results": results}, f, indent=1)
    print("QUERYPROOF_HUNT", json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:5])
