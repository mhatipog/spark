"""Metamorphic hunt over partitioned Parquet tables: configurations that must not change results.

Usage: python run_meta.py CASES.json OUT.json [SHARD COUNT]
Each database is written once as catalog Parquet tables:
  ta, tb  partitioned by `a` (NULLs land in __HIVE_DEFAULT_PARTITION__)
  te      partitioned by `k`
  tc      bucketed by `k` into 2 buckets, sorted
  td      plain
followed by ANALYZE TABLE ... FOR ALL COLUMNS. Every query then runs under the default
configuration and under VARIANTS, each documented as semantics-preserving; it is also
checked by ternary logic partitioning (TLP) and, where the case allows, against DuckDB
and SQLite on the same rows. Any bag that differs from the default, or a variant that
fails where the default succeeds, is recorded with its rows.
"""
import datetime
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from decimal import Decimal

from pyspark.sql import SparkSession

from run_hunt import bag, spark_rows

try:
    import duckdb
except ImportError:  # DuckDB is optional; its oracle is then skipped
    duckdb = None

VARIANTS = {
    "aqe_off": {"spark.sql.adaptive.enabled": "false"},
    "no_broadcast": {"spark.sql.autoBroadcastJoinThreshold": "-1"},
    "shuffle_hash": {"spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.join.preferSortMergeJoin": "false"},
    "no_pushdown": {"spark.sql.parquet.filterPushdown": "false"},
    "no_vectorized": {"spark.sql.parquet.enableVectorizedReader": "false"},
    "no_nested_pruning": {"spark.sql.optimizer.nestedSchemaPruning.enabled": "false"},
    "no_dpp": {"spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
    "no_bucketing": {"spark.sql.sources.bucketing.enabled": "false"},
    "cbo_reorder": {"spark.sql.cbo.enabled": "true", "spark.sql.cbo.joinReorder.enabled": "true",
                    "spark.sql.cbo.planStats.enabled": "true", "spark.sql.cbo.starSchemaDetection": "true"},
    "parts1": {"spark.sql.shuffle.partitions": "1", "spark.sql.adaptive.coalescePartitions.enabled": "false"},
    "parts7": {"spark.sql.shuffle.partitions": "7", "spark.sql.adaptive.coalescePartitions.enabled": "false"},
    "no_codegen": {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN"},
}
LAYOUT = {"ta": ("partition", "a"), "tb": ("partition", "a"), "te": ("partition", "k"),
          "tc": ("bucket", "k"), "td": ("plain", None)}
SPARK_TYPES = {"integer": "int", "string": "string", "decimal(5,2)": "decimal(5,2)", "date": "date"}
SQL_TYPES = {"integer": "INTEGER", "string": "TEXT", "decimal(5,2)": "DECIMAL(5,2)", "date": "DATE"}


def py_value(v, ty):
    if v is None:
        return None
    if ty == "decimal(5,2)":
        return Decimal(v)
    if ty == "date":
        return datetime.date.fromisoformat(v)
    return v


def typed_rows(t):
    types = [c["type"] for c in t["columns"]]
    return [tuple(py_value(v, ty) for v, ty in zip(r, types)) for r in t["rows"]]


def write_tables(spark, dataset):
    for name, t in dataset.items():
        schema = ", ".join(f"{c['name']} {SPARK_TYPES[c['type']]}" for c in t["columns"])
        df = spark.createDataFrame(typed_rows(t), schema)
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        kind, col = LAYOUT.get(name, ("plain", None))
        w = df.write.format("parquet").mode("overwrite")
        if kind == "partition":
            w = w.partitionBy(col)
        elif kind == "bucket":
            w = w.bucketBy(2, col).sortBy(col)
        w.saveAsTable(name)
        spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR ALL COLUMNS")


def duck_rows(dataset, sql):
    con = duckdb.connect()
    for name, t in dataset.items():
        cols = ", ".join(f"{c['name']} {SQL_TYPES[c['type']]}" for c in t["columns"])
        con.execute(f"CREATE TABLE {name} ({cols})")
        if t["rows"]:
            con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(t['columns']))})", typed_rows(t))
    try:
        return {"rows": [list(r) for r in con.execute(sql.replace("<=>", "IS NOT DISTINCT FROM")).fetchall()]}
    except duckdb.Error as exc:
        return {"error": "duckdb: " + str(exc).split("\n")[0][:200]}


def sqlite_rows(dataset, sql):
    db = sqlite3.connect(":memory:")
    for name, t in dataset.items():
        cols = ", ".join(f"{c['name']} {SQL_TYPES[c['type']]}" for c in t["columns"])
        db.execute(f"CREATE TABLE {name} ({cols})")
        if t["rows"]:
            db.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(t['columns']))})", t["rows"])
    try:
        return {"rows": [list(r) for r in db.execute(sql.replace("<=>", "IS")).fetchall()]}
    except sqlite3.Error as exc:
        return {"error": "sqlite: " + str(exc)[:200]}


def tlp_sql(case):
    q, p = case["sql"], case["tlp"]
    return (f"SELECT * FROM ({q}) t WHERE {p} UNION ALL SELECT * FROM ({q}) t WHERE NOT ({p}) "
            f"UNION ALL SELECT * FROM ({q}) t WHERE ({p}) IS NULL")


def main(cases_path, out_path, shard=0, count=1):
    shard, count = int(shard), int(count)
    wh = tempfile.mkdtemp(prefix="qp_wh_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").config("spark.sql.warehouse.dir", wh)
             .getOrCreate())
    cases = json.loads(open(cases_path).read())["cases"]
    groups = sorted({c["group"] for c in cases})[shard::count]
    mine = [c for c in cases if c["group"] in set(groups)]
    results, counts, t0 = [], Counter(), time.time()
    loaded = None
    for d in range(len(mine[0]["datasets"]) if mine else 0):
        for case in mine:
            print("QUERYPROOF_CASE", case["id"], d, file=sys.stderr, flush=True)
            key = (case["group"], d)
            dataset = case["datasets"][d]
            if loaded != key:
                write_tables(spark, dataset)
                loaded = key
            out = {"default": spark_rows(spark, case["sql"])}
            for name, conf in VARIANTS.items():
                for k, v in conf.items():
                    spark.conf.set(k, v)
                out[name] = spark_rows(spark, case["sql"])
                for k in conf:
                    spark.conf.unset(k)
            out["tlp"] = spark_rows(spark, tlp_sql(case))
            if duckdb is not None and case.get("duckdb", True):
                out["duckdb"] = duck_rows(dataset, case["sql"])
            if case.get("sqlite", True):
                out["sqlite"] = sqlite_rows(dataset, case["sql"])
            ref = out["default"]
            counts["runs"] += 1
            if "rows" not in ref:
                counts["default_error"] += 1
            diff = []
            for name, res in out.items():
                if name == "default":
                    continue
                if "rows" in ref and "rows" in res and bag(res["rows"]) != bag(ref["rows"]):
                    diff.append(name)
                elif "rows" in ref and "rows" not in res and name not in ("duckdb", "sqlite"):
                    diff.append(name + ":error")
                elif "rows" not in ref and "rows" in res and name not in ("duckdb", "sqlite", "tlp"):
                    diff.append(name + ":succeeds")
            for n in diff:
                counts["diff:" + n] += 1
            if diff:
                shown = {"default", "duckdb", "sqlite"} | {n.split(":")[0] for n in diff}
                results.append({"case": case["id"], "family": case["family"], "dataset": d,
                                "sql": case["sql"], "tlp": case["tlp"], "diff": diff,
                                "results": {k: (v if k in shown else "same") for k, v in out.items()}})
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    summary = {"cases": len(mine), "seconds": round(time.time() - t0, 1), **dict(counts),
               "with_diff": len(results)}
    with open(out_path, "w", newline="\n") as f:
        json.dump({"summary": summary, "variants": VARIANTS, "findings": results}, f, indent=1, default=str)
    print("QUERYPROOF_META", json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:5])
