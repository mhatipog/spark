"""v3 hunt HD (family F3): answers computed from Iceberg metadata on tables with deletes."""
import os, shutil, sys, json, itertools, warnings
import pyarrow as pa, polars as pl, duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, IntegerType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyiceberg.expressions as E
warnings.filterwarnings("ignore")
R = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "del"); shutil.rmtree(R, ignore_errors=True); os.makedirs(R)
cat = SqlCatalog("d", uri=f"sqlite:///{R}/c.db", warehouse=R.replace(os.sep, "/")); cat.create_namespace("n")
findings = []
def rec(**k): findings.append(k); print("FINDING", json.dumps(k, default=str)[:400], flush=True)

DELETES = [E.EqualTo("v", 3), E.LessThan("v", 2), E.EqualTo("g", 1), E.AlwaysTrue(), E.In("v", {0, 5}), E.IsNull("s"), E.EqualTo("s", "x")]
n = 0
for mode, part, (d1, d2) in itertools.product(("copy-on-write", "merge-on-read"), (False, True), itertools.combinations(range(len(DELETES)), 2)):
    n += 1
    name = f"n.t{n}"
    sch = Schema(NestedField(1, "id", LongType()), NestedField(2, "v", IntegerType()), NestedField(3, "g", IntegerType()), NestedField(4, "s", StringType()))
    spec = PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "g")) if part else PartitionSpec()
    try:
        t = cat.create_table(name, sch, partition_spec=spec, properties={"format-version": "2", "write.delete.mode": mode})
        for chunk in range(3):
            ids = list(range(chunk * 4, chunk * 4 + 4))
            t.append(pa.table({"id": pa.array(ids, pa.int64()), "v": pa.array([i % 6 for i in ids], pa.int32()),
                               "g": pa.array([chunk % 2] * 4, pa.int32()), "s": pa.array([None if i % 5 == 0 else ("x" if i % 3 == 0 else "y") for i in ids])}))
        t.delete(DELETES[d1]); t = cat.load_table(name); t.delete(DELETES[d2]); t = cat.load_table(name)
    except Exception as e:
        rec(kind="write_error", table=name, mode=mode, part=part, deletes=[str(DELETES[d1]), str(DELETES[d2])], err=repr(e)[:250]); continue
    truth = pl.from_arrow(t.scan().to_arrow())
    files = t.inspect.delete_files().num_rows if hasattr(t.inspect, "delete_files") else -1
    ctx = dict(table=name, mode=mode, part=part, deletes=[str(DELETES[d1]), str(DELETES[d2])], delete_files=files)
    agg = lambda lf: lf.select(pl.len().alias("n"), pl.col("v").min().alias("mn"), pl.col("v").max().alias("mx"), pl.col("s").null_count().alias("ns"), pl.col("v").count().alias("cv")).collect().row(0)
    want = agg(truth.lazy())
    for opts in ({}, {"use_metadata_statistics": True}, {"fast_deletion_count": True}, {"use_metadata_statistics": True, "fast_deletion_count": True},
                 {"reader_override": "pyiceberg"}):
        try:
            got = agg(pl.scan_iceberg(t, **opts))
            if got != want: rec(kind="polars_agg", opts=opts, **ctx, got=got, want=want)
            for flt in (pl.col("v") >= 3, pl.col("g") == 1, pl.col("s").is_null()):
                g2 = pl.scan_iceberg(t, **opts).filter(flt).select(pl.len()).collect().item(); w2 = truth.filter(flt).height
                if g2 != w2: rec(kind="polars_filtered_count", opts=opts, filter=str(flt), **ctx, got=g2, want=w2)
        except Exception as e: rec(kind="polars_error", opts=opts, **ctx, err=repr(e)[:250])
    try:
        con = duckdb.connect(); con.sql("LOAD iceberg")
        got = con.sql(f"SELECT count(*), min(v), max(v), count(*) - count(s), count(v) FROM iceberg_scan('{t.metadata_location}')").fetchall()[0]
        if tuple(got) != want: rec(kind="duckdb_agg", **ctx, got=got, want=want)
    except Exception as e: rec(kind="duckdb_error", **ctx, err=repr(e)[:250])
print("TOTAL", n, "tables", len(findings), "findings")
json.dump(findings, open(f"{R}/findings.json", "w"), default=str, indent=1)
