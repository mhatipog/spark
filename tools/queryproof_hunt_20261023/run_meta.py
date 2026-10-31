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

from run_hunt import bag

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
# Second wave (QP_VARIANTS=wide): execution knobs around the DPP finding.
NO_CG = {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN"}
DPP_AGGRESSIVE = {"spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly": "false",
                  "spark.sql.optimizer.dynamicPartitionPruning.useStats": "false",
                  "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio": "1.0"}
BLOOM = {"spark.sql.optimizer.runtime.bloomFilter.enabled": "true",
         "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold": "0",
         "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold": "100MB"}
WIDE_VARIANTS = {
    "aqe_off": VARIANTS["aqe_off"],
    "no_broadcast": VARIANTS["no_broadcast"],
    "cbo_reorder": VARIANTS["cbo_reorder"],
    "no_pushdown": VARIANTS["no_pushdown"],
    "dpp_aggressive": DPP_AGGRESSIVE,
    "dpp_aggressive_no_broadcast": {**DPP_AGGRESSIVE, "spark.sql.autoBroadcastJoinThreshold": "-1"},
    "bloom": BLOOM,
    "bloom_no_broadcast": {**BLOOM, "spark.sql.autoBroadcastJoinThreshold": "-1"},
    "no_reuse": {"spark.sql.exchange.reuse": "false", "spark.sql.execution.reuseSubquery": "false"},
    "skew_forced": {"spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1",
                    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "1",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1"},
    "partition_evaluator": {"spark.sql.execution.usePartitionEvaluator": "true"},
    "no_subexpr": {"spark.sql.subexpressionElimination.enabled": "false"},
    "no_codegen": NO_CG,
    "no_codegen_no_dpp": {**NO_CG, "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
    "bloom_no_codegen_no_dpp": {**BLOOM, **NO_CG, "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
    "no_reuse_no_codegen_no_dpp": {"spark.sql.exchange.reuse": "false", "spark.sql.execution.reuseSubquery": "false",
                                   **NO_CG, "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
}
# Third set (QP_VARIANTS=feat): the base set plus nested-type reader and pruning switches.
FEAT_VARIANTS = {
    **VARIANTS,
    "no_nested_vectorized": {"spark.sql.parquet.enableNestedColumnVectorizedReader": "false"},
    "no_nested_predicate_pushdown": {"spark.sql.optimizer.nestedPredicatePushdownSupportedFileSources": ""},
    "no_nested_expr_pruning": {"spark.sql.optimizer.expression.nestedPruning.enabled": "false"},
    "no_codegen_no_dpp": WIDE_VARIANTS["no_codegen_no_dpp"],
}
# Fourth set (QP_VARIANTS=round6): user-facing switches only (no internal factoryMode, which
# trips the by-design ScalarCastHelper limitation), whole-stage codegen combinations, and
# path-based reads of the same files through the V1 and the V2 file source.
WS_OFF = {"spark.sql.codegen.wholeStage": "false"}
ROUND6_VARIANTS = {
    "aqe_off": VARIANTS["aqe_off"],
    "no_broadcast": VARIANTS["no_broadcast"],
    "shuffle_hash": VARIANTS["shuffle_hash"],
    "cbo_reorder": VARIANTS["cbo_reorder"],
    "no_pushdown": VARIANTS["no_pushdown"],
    "no_vectorized": VARIANTS["no_vectorized"],
    "no_nested_vectorized": FEAT_VARIANTS["no_nested_vectorized"],
    "no_nested_pruning": VARIANTS["no_nested_pruning"],
    "dpp_aggressive": DPP_AGGRESSIVE,
    "bloom": BLOOM,
    "no_reuse": WIDE_VARIANTS["no_reuse"],
    "parts7": VARIANTS["parts7"],
    "wholestage_off": WS_OFF,
    "wholestage_off_aqe_off": {**WS_OFF, "spark.sql.adaptive.enabled": "false"},
    "wholestage_off_no_broadcast": {**WS_OFF, "spark.sql.autoBroadcastJoinThreshold": "-1"},
    "wholestage_off_no_dpp": {**WS_OFF, "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"},
    "@paths_v1": {},
    "@paths_v2": {"spark.sql.sources.useV1SourceList": ""},
    "@paths_v2_no_pushdown": {"spark.sql.sources.useV1SourceList": "", "spark.sql.parquet.filterPushdown": "false"},
}
# Fifth set (QP_VARIANTS=round7): the same rows read as ORC, JSON and CSV files by path
# (V1 and V2 file sources, each format's own reader switches) and as cached tables
# (in-memory columnar scans with and without batch pruning). Prefix "@fmt_<format>_".
ORC_V2 = {"spark.sql.sources.useV1SourceList": ""}
ROUND7_VARIANTS = {
    "@fmt_orc_v1": {},
    "@fmt_orc_v2": ORC_V2,
    "@fmt_orc_no_pushdown": {"spark.sql.orc.filterPushdown": "false"},
    "@fmt_orc_no_vectorized": {"spark.sql.orc.enableVectorizedReader": "false"},
    "@fmt_orc_v2_no_vectorized": {**ORC_V2, "spark.sql.orc.enableVectorizedReader": "false"},
    "@fmt_orc_v2_no_pushdown": {**ORC_V2, "spark.sql.orc.filterPushdown": "false"},
    "@fmt_orc_aggregate_pushdown": {**ORC_V2, "spark.sql.orc.aggregatePushdown": "true"},
    "@fmt_orc_wholestage_off": WS_OFF,
    "@fmt_parquet_aggregate_pushdown": {**ORC_V2, "spark.sql.parquet.aggregatePushdown": "true"},
    "@fmt_json_v1": {},
    "@fmt_json_v2": ORC_V2,
    "@fmt_json_no_pushdown": {"spark.sql.json.filterPushdown.enabled": "false"},
    "@fmt_csv_v1": {},
    "@fmt_csv_v2": ORC_V2,
    "@fmt_csv_no_pushdown": {"spark.sql.csv.filterPushdown.enabled": "false"},
    "@cache": {},
    "@cache_no_batch_pruning": {"spark.sql.inMemoryColumnarStorage.partitionPruning": "false"},
    "@cache_no_vectorized": {"spark.sql.inMemoryColumnarStorage.enableVectorizedReader": "false"},
    "@cache_wholestage_off": WS_OFF,
}
# Formats that cannot hold a column type keep the catalog Parquet table for that table.
FORMAT_TYPES = {"orc": None, "json": None, "csv": {"integer", "string", "decimal(5,2)", "date"}}
# A NULL marker distinct from the empty string, which the CSV writer quotes as "".
FORMAT_OPTIONS = {"csv": {"nullValue": "\\N"}}
if os.environ.get("QP_VARIANTS") == "wide":
    VARIANTS = WIDE_VARIANTS
elif os.environ.get("QP_VARIANTS") == "feat":
    VARIANTS = FEAT_VARIANTS
elif os.environ.get("QP_VARIANTS") == "round6":
    VARIANTS = ROUND6_VARIANTS
elif os.environ.get("QP_VARIANTS") == "round7":
    VARIANTS = ROUND7_VARIANTS
KNOWN_SIGNATURES = {"has not finished": "N1: DPP through UNION, codegen off (this hunt; fix branch ready)",
                    "Character N is neither": "N2: CBO FilterEstimation NaN (this hunt; fix branch ready)",
                    "exprId on unresolved object": "V2 file source + VARIANT regression in 4.1/4.2 (fixed on master)"}
TABLE_PATHS = {}  # table name -> (location, schema) of the currently loaded database
FORMAT_PATHS = {}  # format -> {table name -> (location, schema)} of copies in other formats


def run_over_paths(spark, sql, fmt="parquet"):
    """Run sql with catalog tables shadowed by temp views reading their files by path."""
    paths = TABLE_PATHS if fmt == "parquet" else FORMAT_PATHS[fmt]
    for name, (loc, schema) in paths.items():
        spark.read.format(fmt).options(**FORMAT_OPTIONS.get(fmt, {})).schema(schema).load(loc) \
            .createOrReplaceTempView(name)
    try:
        return spark_rows(spark, sql)
    finally:
        for name in paths:
            spark.catalog.dropTempView(name)


def run_cached(spark, sql):
    """Run sql with every table cached (in-memory columnar scans), then drop the cache."""
    for name in TABLE_PATHS:
        spark.sql(f"CACHE LAZY TABLE {name}")
    try:
        return spark_rows(spark, sql)
    finally:
        spark.catalog.clearCache()


def run_variant(spark, name, sql):
    if name.startswith("@paths"):
        return run_over_paths(spark, sql)
    if name.startswith("@fmt_"):
        return run_over_paths(spark, sql, name.split("_")[1])
    if name.startswith("@cache"):
        return run_cached(spark, sql)
    return spark_rows(spark, sql)


def write_format_copies(spark, dataset, base):
    """Copy each table into every needed format, keeping its partition layout."""
    FORMAT_PATHS.clear()
    fmts = sorted({n.split("_")[1] for n in VARIANTS if n.startswith("@fmt_")} - {"parquet"})
    for fmt in fmts:
        FORMAT_PATHS[fmt] = {}
        for name, t in dataset.items():
            allowed = FORMAT_TYPES[fmt]
            if allowed is not None and any(c["type"] not in allowed for c in t["columns"]):
                continue
            kind, col = LAYOUT.get(name, ("plain", None))
            loc = os.path.join(base, fmt, name)
            w = spark.table(name).write.format(fmt).options(**FORMAT_OPTIONS.get(fmt, {})).mode("overwrite")
            (w.partitionBy(col) if kind == "partition" else w).save(loc)
            FORMAT_PATHS[fmt][name] = (loc, spark.table(name).schema)
LAYOUT = {"ta": ("partition", "a"), "tb": ("partition", "a"), "te": ("partition", "k"),
          "tc": ("bucket", "k"), "td": ("plain", None), "tf": ("partition", "k")}
SPARK_TYPES = {"integer": "int", "string": "string", "decimal(5,2)": "decimal(5,2)", "date": "date",
               "struct<x:int,y:string>": "struct<x:int,y:string>", "array<int>": "array<int>"}
SQL_TYPES = {"integer": "INTEGER", "string": "TEXT", "decimal(5,2)": "DECIMAL(5,2)", "date": "DATE"}


def py_value(v, ty):
    if v is None:
        return None
    if ty == "decimal(5,2)":
        return Decimal(v)
    if ty == "date":
        return datetime.date.fromisoformat(v)
    if ty.startswith("struct<"):
        return tuple(v)
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
        atomic = [c["name"] for c in t["columns"] if c["type"] in SQL_TYPES]
        if len(atomic) == len(t["columns"]):
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR ALL COLUMNS")
        else:  # ANALYZE ... FOR COLUMNS rejects STRUCT/ARRAY columns
            spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS FOR COLUMNS {', '.join(atomic)}")
    if "tf" in dataset:  # VARIANT table derived from tf.js (gen_feat.py)
        spark.sql("DROP TABLE IF EXISTS tv")
        spark.sql("CREATE TABLE tv USING parquet AS SELECT k, parse_json(js) AS v FROM tf WHERE js IS NOT NULL")
    TABLE_PATHS.clear()
    for name in list(dataset) + (["tv"] if "tf" in dataset else []):
        loc = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").where("col_name = 'Location'").collect()[0][1]
        TABLE_PATHS[name] = (loc, spark.table(name).schema)


def duck_rows(dataset, sql):
    con = duckdb.connect()
    for name, t in dataset.items():
        cols = ", ".join(f"{c['name']} {SQL_TYPES[c['type']]}" for c in t["columns"])
        con.execute(f"CREATE TABLE {name} ({cols})")
        if t["rows"]:
            con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(t['columns']))})", typed_rows(t))
    try:
        duck_sql = sql.replace("<=>", "IS NOT DISTINCT FROM").replace("LEFT SEMI JOIN", "SEMI JOIN")
        return {"rows": [list(r) for r in con.execute(duck_sql).fetchall()]}
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


def spark_rows(spark, sql):
    """Rows, or the Java root cause of the failure (needed to tell known failures apart)."""
    try:
        return {"rows": [list(r) for r in spark.sql(sql).collect()]}
    except Exception as exc:  # noqa: BLE001 - recorded, not hidden
        c = getattr(exc, "java_exception", None)
        while c is not None and c.getCause() is not None:
            c = c.getCause()
        root = (c.toString() if c is not None else type(exc).__name__ + ": " + str(exc)).split("\n")[0]
        return {"error": root[:300]}


def materialized_checks(spark, case):
    """Default-configuration oracles that need no second engine.

    norec      COUNT(*) WHERE p  vs  SUM(CASE WHEN p THEN 1 ELSE 0 END) over the same query
    mat_filter Q's rows filtered by p with p pushed into Q, vs the same filter over Q's
               result pinned with localCheckpoint (the optimizer cannot cross it)
    mat_agg    COUNT(*), COUNT(DISTINCT c0), MIN/MAX(c0) likewise
    Each returns rows from both sides; the caller compares the "pushed" side to "pinned".
    """
    q, p = case["sql"], case["tlp"]
    out = {}
    try:
        # A fresh DataFrame from the collected rows: as opaque to the optimizer as a
        # checkpoint, without leaving persisted blocks behind (localCheckpoint ran a shard
        # out of driver heap after a few hundred cases).
        df = spark.sql(q)
        pinned = spark.createDataFrame(df.collect(), df.schema)
    except Exception as exc:  # noqa: BLE001
        return {"mat_error": str(exc).split("\n")[0][:200]}
    pinned.createOrReplaceTempView("qp_pinned")
    out["norec"] = (spark_rows(spark, f"SELECT COUNT(*) FROM ({q}) t WHERE {p}"),
                    spark_rows(spark, f"SELECT COALESCE(SUM(CASE WHEN {p} THEN 1 ELSE 0 END), 0) FROM ({q}) t"))
    out["mat_filter"] = (spark_rows(spark, f"SELECT * FROM ({q}) t WHERE {p}"),
                         spark_rows(spark, f"SELECT * FROM qp_pinned t WHERE {p}"))
    agg = "COUNT(*), COUNT(DISTINCT t.c0), MIN(t.c0), MAX(t.c0)"
    out["mat_agg"] = (spark_rows(spark, f"SELECT {agg} FROM ({q}) t"),
                      spark_rows(spark, f"SELECT {agg} FROM qp_pinned t"))
    spark.catalog.dropTempView("qp_pinned")
    return out


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
                write_format_copies(spark, dataset, os.path.join(wh, "_formats"))
                loaded = key
            out = {"default": spark_rows(spark, case["sql"])}
            for name, conf in VARIANTS.items():
                for k, v in conf.items():
                    spark.conf.set(k, v)
                out[name] = run_variant(spark, name, case["sql"])
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
                    known = any(s in res["error"] for s in KNOWN_SIGNATURES)
                    diff.append(name + (":known_error" if known else ":error"))
                elif "rows" not in ref and "rows" in res and name not in ("duckdb", "sqlite", "tlp"):
                    diff.append(name + ":succeeds")
            if "rows" in ref:
                for name, pair in materialized_checks(spark, case).items():
                    if name == "mat_error":
                        diff.append("mat:error")
                        out["mat"] = {"error": pair}
                        continue
                    pushed, pinned = pair
                    out[name] = {"pushed": pushed, "pinned": pinned}
                    if "rows" in pushed and "rows" in pinned and bag(pushed["rows"]) != bag(pinned["rows"]):
                        diff.append(name)
                    elif ("rows" in pushed) != ("rows" in pinned):
                        diff.append(name + ":error")
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
