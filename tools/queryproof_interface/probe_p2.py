"""Phase-two triage probe (QP_PROBE=probe_p2.py on line 6 of P2_REQUEST). Runs after the shard's
files are written; prints facts only, no data values.

Y1: Spark's row-based Parquet reader on DataFusion-written DELTA_BYTE_ARRAY pages, with the
datafusion-python default created_by ("datafusion-python", no version) and, as the negative
control, the same bytes with created_by rewritten to "datafusion-python version <v>".
C1: chDB nested columns: the column index and size statistics chDB writes, next to the
definition levels actually in the pages (via the auditor).

Usage: python probe_p2.py BASE
"""
import glob
import os
import shutil
import sys

base = sys.argv[1]


def y1():
    import datafusion
    from pyspark.sql import SparkSession
    from audit_selftest import footer, with_footer
    src = os.path.join(base, "O2_long_prefix", "df_rg2_pages1_bloom")
    if not os.path.isdir(src):
        print("Y1 skipped: no", src)
        return
    ctl = os.path.join(base, "_y1_control")
    shutil.rmtree(ctl, ignore_errors=True)
    shutil.copytree(src, ctl)
    for p in glob.glob(os.path.join(ctl, "**", "*.parquet"), recursive=True):
        buf = open(p, "rb").read()
        fm, _ = footer(buf)
        print("Y1 created_by before:", repr(fm.created_by))
        fm.created_by = f"datafusion-python version {datafusion.__version__}"
        open(p, "wb").write(with_footer(buf, fm))
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    for vec in ("true", "false"):
        for push in ("true", "false"):
            spark.conf.set("spark.sql.parquet.enableVectorizedReader", vec)
            spark.conf.set("spark.sql.parquet.filterPushdown", push)
            for name, d in (("default created_by", src), ("control created_by", ctl)):
                try:
                    n = spark.read.parquet(d).where("s IS NOT NULL").count()
                    res = f"ok, {n} rows"
                except Exception as exc:  # noqa: BLE001
                    res = "error: " + str(exc).split("\n")[0][:200]
                print(f"Y1 vectorized={vec} filterPushdown={push} {name}: {res}")
    spark.stop()


def y1_min():
    """The filing guide's minimal Y1 reproduction (2l), default created_by vs versioned."""
    import datafusion
    from pyspark.sql import SparkSession
    # the filing guide's minimal reproduction (2l), with and without the versioned created_by
    import pyarrow as pa
    from datafusion import ParquetWriterOptions
    t = pa.table({"k": pa.array(range(5), pa.int32()), "s": pa.array(["x" * 80 + str(i) for i in range(5)])})
    mini = {}
    for tag, cb in (("minimal default", None), ("minimal control", f"datafusion-python version {datafusion.__version__}")):
        d = os.path.join(base, "_y1_" + tag.replace(" ", "_"))
        shutil.rmtree(d, ignore_errors=True)
        opts = dict(writer_version="2.0", dictionary_enabled=False, **({"created_by": cb} if cb else {}))
        datafusion.SessionContext().from_arrow(t).write_parquet(d, ParquetWriterOptions(**opts))
        mini[tag] = d
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    for tag, d in mini.items():
        try:
            res = f"ok, {spark.read.parquet(d).where('s IS NOT NULL').count()} rows"
        except Exception as exc:  # noqa: BLE001
            res = "error: " + str(exc).split("\n")[0][:200]
        print(f"Y1 {tag}: {res}")


def c1():
    import parquet_audit as A
    from audit_selftest import footer
    for shape in ("N1_struct_null_whole_file", "N6_two_levels"):
        for w in ("chdb_default", "chdb_rg2_index_bloom"):
            for p in sorted(glob.glob(os.path.join(base, shape, w, "**", "*.parquet"), recursive=True)):
                buf = open(p, "rb").read()
                fm, _ = footer(buf)
                print("C1", shape, w, os.path.basename(p), "created_by", repr(fm.created_by))
                print("C1   schema", [(e.name, e.repetition_type, e.num_children) for e in fm.schema])
                for gi, rg in enumerate(fm.row_groups):
                    for cc in rg.columns:
                        md = cc.meta_data
                        st = md.statistics
                        ss = md.size_statistics
                        print("C1   rg", gi, ".".join(md.path_in_schema), "nulls", st.null_count if st else None,
                              "def_hist", ss.definition_level_histogram if ss else None,
                              "ci", cc.column_index_offset is not None)
                        if cc.column_index_offset is not None:
                            ci = A.read_struct(A.PT.ColumnIndex, buf, cc.column_index_offset)[0]
                            print("C1     column_index null_pages", ci.null_pages, "null_counts", ci.null_counts,
                                  "boundary_order", ci.boundary_order, "def_hist", ci.definition_level_histograms,
                                  "mins", [x.hex()[:16] for x in ci.min_values],
                                  "maxs", [x.hex()[:16] for x in ci.max_values])
                        if st:
                            print("C1     chunk stats min", (st.min_value or b"").hex()[:16], "max",
                                  (st.max_value or b"").hex()[:16])
                v = A.audit_file(p)
                print("C1   audit", [(x["claim"], x.get("col"), x["detail"][:80]) for x in v][:12])


def c1_trino():
    """The exception behind Trino's "Corrupted column index" on chDB nested columns: the cause
    chain and top frames from the query's failure info; pyarrow-written sets as the control."""
    import json
    from engines import Trino
    from shapes import SHAPES
    by_id = {s["id"]: s for s in SHAPES}
    entries = json.load(open(os.path.join(base, "manifest.json")))
    tr = Trino(base)
    tr.attempts = 90000  # the matrix already holds t1, t2, ... in schema qp
    queries = {"N1_struct_null_whole_file": "SELECT k FROM t WHERE (st.y IS NULL)",
               "N6_two_levels": "SELECT k FROM t WHERE (o.i.c IS NULL)"}
    for e in entries:
        if e["shape"] not in queries or e["format"] != "parquet":
            continue
        if not (e["writer_set"].startswith("chdb") or e["writer_set"] in ("pa_rg2_plain_v2", "pa_pages1_dictfallback_bloom")):
            continue
        name = tr._table(e, by_id[e["shape"]])
        cur = tr._conn({}).cursor()
        try:
            cur.execute(queries[e["shape"]].replace(" FROM t", f" FROM {name}"))
            print("C1T", e["shape"], e["writer_set"], "ok", len(cur.fetchall()), "rows")
        except Exception as exc:  # noqa: BLE001
            fi = getattr(exc, "failure_info", None) or {}
            depth = 0
            while fi and depth < 6:
                print("C1T", e["shape"], e["writer_set"], "cause", depth, fi.get("type"), str(fi.get("message"))[:160].replace("\n", " | "))
                for fr in (fi.get("stack") or [])[:6]:
                    print("C1T       at", fr[:160])
                fi = fi.get("cause") or {}
                depth += 1
            if depth == 0:
                print("C1T", e["shape"], e["writer_set"], "error", str(exc)[:200])


for name, fn in (("Y1", y1), ("Y1min", y1_min), ("C1", c1), ("C1T", c1_trino)):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        print(name, "probe failed:", type(exc).__name__, str(exc)[:300])
