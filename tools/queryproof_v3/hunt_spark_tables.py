"""QueryProof v3 hunt HE (families F3, F5): tables written by the reference Java implementations.

Spark writes Iceberg (format v2 merge-on-read position deletes, format v3 deletion vectors) and
Delta (deletion vectors, column mapping) tables, applies DELETE / UPDATE / schema evolution, and
is the reference answer. pyiceberg, Polars (native and pyiceberg readers, metadata statistics
and fast deletion counts on and off), DuckDB (iceberg_scan, delta_scan) and deltalake read the
same tables. Every difference is printed as a FINDING line and saved to findings.json.
"""
import glob, json, os, shutil, sys, traceback

WH = "/tmp/qpv3"
shutil.rmtree(WH, ignore_errors=True); os.makedirs(WH)
ICEBERG_PKG = os.environ.get("QP_ICEBERG_PKG", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
DELTA_PKG = os.environ.get("QP_DELTA_PKG", "io.delta:delta-spark_2.13:4.0.0")

from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[2]")
         .config("spark.jars.packages", f"{ICEBERG_PKG},{DELTA_PKG}")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.ice.type", "hadoop").config("spark.sql.catalog.ice.warehouse", f"file://{WH}/ice")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("spark", spark.version, ICEBERG_PKG, DELTA_PKG, flush=True)

import polars as pl, duckdb, pyarrow as pa
from pyiceberg.table import StaticTable
from deltalake import DeltaTable, QueryBuilder
print("polars", pl.__version__, "duckdb", duckdb.__version__, flush=True)
con = duckdb.connect(); con.sql("INSTALL iceberg; LOAD iceberg; INSTALL delta; LOAD delta; SET TimeZone='UTC'")

findings = []
def rec(**k):
    findings.append(k); print("FINDING", json.dumps(k, default=str)[:600], flush=True)

def batches(n_batches=3, per=6, start=0):
    rows = []
    for b in range(n_batches):
        for i in range(start + b * per, start + (b + 1) * per):
            rows.append((i, i % 7, b % 2, None if i % 5 == 0 else ("x" if i % 3 == 0 else "y")))
    return rows

# (name, setup SQL list); the table name placeholder is {T}
ICE_SCEN = {
  "v2_mor_delete": ["DELETE FROM {T} WHERE v = 3", "DELETE FROM {T} WHERE id < 2"],
  "v2_mor_update": ["UPDATE {T} SET v = v + 10 WHERE id % 4 = 0", "DELETE FROM {T} WHERE s IS NULL"],
  "v2_mor_partition_drop": ["DELETE FROM {T} WHERE g = 1", "DELETE FROM {T} WHERE v = 5"],
  "v2_mor_all_rows_of_file": ["DELETE FROM {T} WHERE id >= 6 AND id < 12"],
  "v2_evolve": ["DELETE FROM {T} WHERE v = 3", "ALTER TABLE {T} ALTER COLUMN v TYPE bigint", "ALTER TABLE {T} RENAME COLUMN s TO s2",
                "INSERT INTO {T} VALUES (100, 3000000000, 0, 'z'), (101, -3000000000, 1, NULL)"],
  "v3_dv_delete": ["DELETE FROM {T} WHERE v = 3", "DELETE FROM {T} WHERE id < 2", "DELETE FROM {T} WHERE id = 7"],
  "v3_dv_update": ["UPDATE {T} SET v = v + 10 WHERE id % 4 = 0", "DELETE FROM {T} WHERE s IS NULL"],
  "v3_dv_all_rows_of_file": ["DELETE FROM {T} WHERE id >= 6 AND id < 12"],
}
DELTA_SCEN = {
  "dv_delete": ["DELETE FROM {T} WHERE v = 3", "DELETE FROM {T} WHERE id < 2", "DELETE FROM {T} WHERE id = 7"],
  "dv_update": ["UPDATE {T} SET v = v + 10 WHERE id % 4 = 0", "DELETE FROM {T} WHERE s IS NULL"],
  "dv_all_rows_of_file": ["DELETE FROM {T} WHERE id >= 6 AND id < 12"],
  "dv_partition": ["DELETE FROM {T} WHERE g = 1 AND v > 2"],
  "colmap_rename": ["ALTER TABLE {T} RENAME COLUMN s TO s2", "DELETE FROM {T} WHERE v = 3", "INSERT INTO {T} VALUES (100, 9, 0, 'z')"],
  "colmap_drop_readd": ["ALTER TABLE {T} DROP COLUMN s", "ALTER TABLE {T} ADD COLUMNS (s STRING)", "INSERT INTO {T} VALUES (100, 9, 0, 'z')"],
}

def scol(cols): return "s2" if "s2" in cols else "s"
def queries(cols):
    s = scol(cols)
    return {
        "count": "SELECT count(*) FROM {T}",
        "agg": f"SELECT count(*), min(v), max(v), count(v), count({s}) FROM {{T}}",
        "v>=3": "SELECT count(*) FROM {T} WHERE v >= 3",
        "v=13": "SELECT count(*) FROM {T} WHERE v = 13",
        "g=1": "SELECT count(*) FROM {T} WHERE g = 1",
        "s null": f"SELECT count(*) FROM {{T}} WHERE {s} IS NULL",
        "s=x": f"SELECT count(*) FROM {{T}} WHERE {s} = 'x'",
        "id<5": "SELECT count(*) FROM {T} WHERE id < 5",
        "v>2^31": "SELECT count(*) FROM {T} WHERE v > 2147483647",
        "v<-2^31": "SELECT count(*) FROM {T} WHERE v < -2147483648",
        "ids": "SELECT id FROM {T} ORDER BY id",
    }

def norm(rows): return [tuple(None if x is None else (int(x) if isinstance(x, (int,)) or (hasattr(x, "is_integer") and x == int(x)) else x) for x in r) for r in rows]

def polars_answers(lf_factory, cols):
    s = scol(cols); out = {}
    def go(k, f):
        try: out[k] = f(lf_factory())
        except Exception as e: out[k] = "ERROR " + repr(e)[:160]
    go("count", lambda lf: [(lf.select(pl.len()).collect().item(),)])
    go("agg", lambda lf: [lf.select(pl.len(), pl.col("v").min().alias("a"), pl.col("v").max().alias("b"), pl.col("v").count().alias("c"), pl.col(s).count().alias("d")).collect().row(0)])
    flt = {"v>=3": pl.col("v") >= 3, "v=13": pl.col("v") == 13, "g=1": pl.col("g") == 1, "s null": pl.col(s).is_null(), "s=x": pl.col(s) == "x",
           "id<5": pl.col("id") < 5, "v>2^31": pl.col("v") > 2147483647, "v<-2^31": pl.col("v") < -2147483648}
    for k, e in flt.items(): go(k, lambda lf, e=e: [(lf.filter(e).select(pl.len()).collect().item(),)])
    go("ids", lambda lf: [(i,) for i in sorted(lf.select("id").collect()["id"].to_list())])
    return out

def compare(ctx, ref, got, reader):
    for k, v in ref.items():
        g = got.get(k)
        if isinstance(g, str) and g.startswith("ERROR"):
            rec(kind="reader_error", reader=reader, query=k, **ctx, err=g); continue
        if g is None: continue
        if norm(g) != norm(v): rec(kind="wrong_answer", reader=reader, query=k, **ctx, got=g, want=v)

def run_iceberg(name, stmts):
    fmt = "3" if name.startswith("v3") else "2"
    T = f"ice.db.{name}"
    spark.sql(f"CREATE TABLE {T} (id BIGINT, v INT, g INT, s STRING) USING iceberg PARTITIONED BY (g) TBLPROPERTIES ("
              f"'format-version'='{fmt}', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")
    for b in range(3):
        rows = batches()[b * 6:(b + 1) * 6]
        spark.createDataFrame(rows, "id BIGINT, v INT, g INT, s STRING").coalesce(1).writeTo(T).append()
    for st in stmts: spark.sql(st.format(T=T))
    cols = spark.table(T).columns
    ref = {k: [tuple(r) for r in spark.sql(q.format(T=T)).collect()] for k, q in queries(cols).items()}
    meta = sorted(glob.glob(f"{WH}/ice/db/{name}/metadata/v*.metadata.json"), key=lambda p: int(os.path.basename(p)[1:].split(".")[0]))[-1]
    snap = spark.sql(f"SELECT summary FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
    ctx = dict(table=name, format="iceberg-v" + fmt, delete_files=snap.get("total-delete-files"), pos_deletes=snap.get("total-position-deletes"))
    print("table", name, ctx, flush=True)
    # pyiceberg
    try:
        st = StaticTable.from_metadata(meta)
        compare(ctx, ref, polars_answers(lambda: pl.from_arrow(st.scan().to_arrow()).lazy(), cols), "pyiceberg.scan")
    except Exception as e: rec(kind="reader_error", reader="pyiceberg.scan", **ctx, err=repr(e)[:300])
    for opts in ({}, {"use_metadata_statistics": True}, {"fast_deletion_count": True}, {"use_metadata_statistics": True, "fast_deletion_count": True},
                 {"use_metadata_statistics": False, "fast_deletion_count": False}, {"reader_override": "pyiceberg"}):
        try:
            compare(dict(ctx, opts=opts), ref, polars_answers(lambda: pl.scan_iceberg(meta, **opts), cols), "polars.scan_iceberg")
        except Exception as e: rec(kind="reader_error", reader="polars.scan_iceberg", opts=opts, **ctx, err=repr(e)[:300])
    try:
        got = {k: [tuple(r) for r in con.sql(q.format(T=f"iceberg_scan('{meta}')")).fetchall()] for k, q in queries(cols).items()}
        compare(ctx, ref, got, "duckdb.iceberg_scan")
    except Exception as e: rec(kind="reader_error", reader="duckdb.iceberg_scan", **ctx, err=repr(e)[:300])

def run_delta(name, stmts):
    path = f"{WH}/delta/{name}"
    props = "'delta.enableDeletionVectors'='true'"
    if name.startswith("colmap"): props += ", 'delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5'"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT, v INT, g INT, s STRING) USING delta PARTITIONED BY (g) TBLPROPERTIES ({props})")
    for b in range(3):
        rows = batches()[b * 6:(b + 1) * 6]
        spark.createDataFrame(rows, "id BIGINT, v INT, g INT, s STRING").coalesce(1).write.format("delta").mode("append").save(path)
    T = f"delta.`{path}`"
    for st in stmts: spark.sql(st.format(T=T))
    cols = spark.read.format("delta").load(path).columns
    ref = {k: [tuple(r) for r in spark.sql(q.format(T=T)).collect()] for k, q in queries(cols).items()}
    dvs = sum(1 for f in glob.glob(f"{path}/**/*.bin", recursive=True))
    ctx = dict(table=name, format="delta", dv_files=dvs)
    print("table", name, ctx, flush=True)
    try:
        dt = DeltaTable(path)
        compare(ctx, ref, polars_answers(lambda: pl.from_arrow(dt.to_pyarrow_table()).lazy(), cols), "deltalake.to_pyarrow_table")
    except Exception as e: rec(kind="reader_error", reader="deltalake.to_pyarrow_table", **ctx, err=repr(e)[:300])
    try:
        qb = QueryBuilder().register("t", DeltaTable(path))
        got = {k: [tuple(r.values()) for r in pa.table(qb.execute(q.format(T="t")).read_all()).to_pylist()] for k, q in queries(cols).items()}
        compare(ctx, ref, got, "deltalake.QueryBuilder")
    except Exception as e: rec(kind="reader_error", reader="deltalake.QueryBuilder", **ctx, err=repr(e)[:300])
    try:
        compare(ctx, ref, polars_answers(lambda: pl.scan_delta(path), cols), "polars.scan_delta")
    except Exception as e: rec(kind="reader_error", reader="polars.scan_delta", **ctx, err=repr(e)[:300])
    try:
        got = {k: [tuple(r) for r in con.sql(q.format(T=f"delta_scan('{path}')")).fetchall()] for k, q in queries(cols).items()}
        compare(ctx, ref, got, "duckdb.delta_scan")
    except Exception as e: rec(kind="reader_error", reader="duckdb.delta_scan", **ctx, err=repr(e)[:300])

spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
for n, s in ICE_SCEN.items():
    try: run_iceberg(n, s)
    except Exception as e: rec(kind="setup_error", table=n, err=traceback.format_exc()[-600:])
for n, s in DELTA_SCEN.items():
    try: run_delta(n, s)
    except Exception as e: rec(kind="setup_error", table=n, err=traceback.format_exc()[-600:])

out = os.environ.get("QP_OUT", "qpv3_out"); os.makedirs(out, exist_ok=True)
json.dump(findings, open(f"{out}/findings.json", "w"), default=str, indent=1)
from collections import Counter
print("SUMMARY", len(findings), Counter((f["kind"], f.get("reader")) for f in findings))
