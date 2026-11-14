"""v3 hunt HB: truncated BYTE_ARRAY statistics and column indexes (families F1/F4).

Writers: arrow-rs (DataFusion write_parquet, truncation 8), parquet-java (Spark, stats truncation 8,
column index truncation 8), pyarrow (no truncation, control).
Layouts: one value per row group (rg) and one value per page in one row group (page).
Oracle: per reader, pruning on vs off, and against the Python answer.
"""
import json, os, shutil, sys, glob
from collections import Counter
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.dataset as pds, duckdb, datafusion, polars as pl

ROOT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "trunc")
USE_SPARK = "--spark" in sys.argv
shutil.rmtree(ROOT, ignore_errors=True); os.makedirs(ROOT)
L = 8
M = "\U0010ffff"
STR = sorted(set([
    "", "a", "a" * L, "a" * L + "\x00", "a" * L + "b", "a" * (L - 1) + "b", "a" * (L - 1) + M + "x",
    "a" * (L - 1) + M, "a" * (L - 2) + "\U0001f600" + "b", "a" * (L - 3) + "\U0001f600" + "b", "a" * (L - 1) + "éb",
    M * 3, M * 3 + "a", "b" * (L + 4), "߿" * L, "￿" * L + "a", "z", "z" * (L + 1),
]))
BIN = sorted(set([b"", b"\x00", b"a" * L + b"\x01", b"\xff" * L, b"\xff" * L + b"\x01", b"\xff" * (L + 3),
                  b"a" * (L - 1) + b"\xff" + b"z", b"a" * (L - 1) + b"\xfe\xff", b"\x7f" * (L + 2), b"\x80" * (L + 2)]))

def lits_str():
    out = set(STR)
    for v in STR:
        for k in (L - 1, L, L + 1):
            if 0 < k < len(v): out.add(v[:k])
        out.add(v + "\x00"); out.add(v + "\U0010ffff")
    return sorted(out)
def lits_bin():
    out = set(BIN)
    for v in BIN:
        for k in (L - 1, L, L + 1):
            if 0 < k < len(v): out.add(v[:k])
        out.add(v + b"\x00"); out.add(v + b"\xff")
    return sorted(out)

def utf8key(s): return s.encode("utf-8")
OPS = ["eq", "ne", "lt", "le", "gt", "ge", "sw"]
def ref(vals, op, lit, isstr):
    k = utf8key if isstr else (lambda b: b)
    out = []
    for i, v in enumerate(vals):
        a, b = k(v), k(lit)
        ok = {"eq": a == b, "ne": a != b, "lt": a < b, "le": a <= b, "gt": a > b, "ge": a >= b, "sw": a.startswith(b)}[op]
        if ok: out.append(i)
    return Counter(out)

def table(vals, isstr):
    return pa.table({"id": pa.array(range(len(vals)), pa.int32()), "v": pa.array(vals, pa.string() if isstr else pa.binary())})

def write_all(name, vals, isstr):
    t = table(vals, isstr); out = {}
    for layout in ("rg", "page"):
        d = f"{ROOT}/{name}/pyarrow_{layout}"; os.makedirs(d)
        pq.write_table(t, f"{d}/f.parquet", row_group_size=1 if layout == "rg" else 1 << 20, data_page_size=1,
                       write_batch_size=1, use_dictionary=False, write_page_index=True)
        out[f"pyarrow_{layout}"] = d
        d = f"{ROOT}/{name}/arrowrs_{layout}"; os.makedirs(d)
        ctx = datafusion.SessionContext()
        opts = datafusion.ParquetWriterOptions(statistics_truncate_length=L, column_index_truncate_length=L,
                                               max_row_group_size=1 if layout == "rg" else 1 << 20,
                                               data_page_row_count_limit=1, write_batch_size=1, dictionary_enabled=False,
                                               statistics_enabled="page")
        ctx.from_arrow(t).repartition(1).write_parquet(f"{d}/f.parquet", opts)
        out[f"arrowrs_{layout}"] = d
    if USE_SPARK:
        sdf = SPARK.createDataFrame(t.to_pandas()).coalesce(1)
        for layout in ("rg", "page"):
            d = f"{ROOT}/{name}/parquetjava_{layout}"
            (sdf.write.option("parquet.block.size", 1 if layout == "rg" else 1 << 26).option("parquet.page.size", 1)
             .option("parquet.enable.dictionary", "false").option("parquet.page.row.count.limit", 1)
             .option("parquet.statistics.truncate.length", L).option("parquet.columnindex.truncate.length", L)
             .option("parquet.block.size.row.check.min", 1).option("parquet.block.size.row.check.max", 1)
             .option("parquet.page.size.row.check.min", 1).option("parquet.page.size.row.check.max", 1)
             .parquet(d.replace(os.sep, "/")))
            out[f"parquetjava_{layout}"] = d
    return out

def files(d): return sorted(glob.glob(f"{d}/**/*.parquet", recursive=True))

def sql_lit(lit, isstr):
    return ("'" + lit.replace("'", "''") + "'") if isstr else f"from_hex('{lit.hex()}')"
def sql_pred(op, lit, isstr, eng):
    l = sql_lit(lit, isstr) if eng == "duckdb" else (("'" + lit.replace("'", "''") + "'") if isstr else f"X'{lit.hex()}'")
    if op == "sw":
        if isstr: return f"starts_with(v, {l})"
        return f"v >= {l} AND hex(v) LIKE '{lit.hex().upper()}%'" if eng == "duckdb" else None
    return f"v {dict(eq='=', ne='<>', lt='<', le='<=', gt='>', ge='>=')[op]} {l}"

def df_ctx(**cfg):
    c = datafusion.SessionConfig()
    for k, v in cfg.items(): c = c.set(k, v)
    return datafusion.SessionContext(c)
DF_MODES = {"default": {}, "pushdown": {"datafusion.execution.parquet.pushdown_filters": "true", "datafusion.execution.parquet.reorder_filters": "true"},
            "nopage": {"datafusion.execution.parquet.enable_page_index": "false"},
            "off": {"datafusion.execution.parquet.pruning": "false", "datafusion.execution.parquet.enable_page_index": "false", "datafusion.execution.parquet.bloom_filter_on_read": "false"}}

findings = []
def rec(**k): findings.append(k); print("FINDING", json.dumps(k, default=str)[:350], flush=True)

def check(name, vals, isstr, dirs):
    lits = lits_str() if isstr else lits_bin()
    con = duckdb.connect()
    for wl, d in dirs.items():
        fs = files(d)
        ctxs = {}
        for m, cfg in DF_MODES.items():
            c = df_ctx(**cfg); c.register_parquet("t", d, file_extension=".parquet"); ctxs[m] = c
        for op in OPS:
            for lit in lits:
                want = ref(vals, op, lit, isstr)
                tag = dict(case=name, writer_layout=wl, op=op, lit=lit if isstr else lit.hex())
                p = sql_pred(op, lit, isstr, "duckdb")
                try:
                    got = Counter(r[0] for r in con.sql(f"SELECT id FROM read_parquet({fs!r}) WHERE {p}").fetchall())
                    if got != want: rec(kind="duckdb", **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                except Exception as e: rec(kind="duckdb_error", **tag, err=repr(e)[:200])
                p2 = sql_pred(op, lit, isstr, "df")
                if p2:
                    for m, c in ctxs.items():
                        try:
                            got = Counter(c.sql(f"SELECT id FROM t WHERE {p2}").to_arrow_table().column("id").to_pylist())
                            if got != want: rec(kind="datafusion", mode=m, **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                        except Exception as e: rec(kind="datafusion_error", mode=m, **tag, err=repr(e)[:200])
                col = pl.col("v"); L_ = pl.lit(lit, dtype=pl.String if isstr else pl.Binary)
                ex = {"eq": col == L_, "ne": col != L_, "lt": col < L_, "le": col <= L_, "gt": col > L_, "ge": col >= L_,
                      "sw": col.str.starts_with(lit) if isstr else col.bin.starts_with(lit)}[op]
                for us in (True, False):
                    try:
                        got = Counter(pl.scan_parquet(fs, use_statistics=us).filter(ex).collect()["id"].to_list())
                        if got != want: rec(kind="polars", use_statistics=us, **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                    except Exception as e: rec(kind="polars_error", use_statistics=us, **tag, err=repr(e)[:200])
                if op != "sw":
                    try:
                        f = {"eq": pds.field("v") == lit, "ne": pds.field("v") != lit, "lt": pds.field("v") < lit, "le": pds.field("v") <= lit,
                             "gt": pds.field("v") > lit, "ge": pds.field("v") >= lit}[op]
                        got = Counter(pds.dataset(fs).to_table(filter=f).column("id").to_pylist())
                        if got != want: rec(kind="pyarrow", **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                    except Exception as e: rec(kind="pyarrow_error", **tag, err=repr(e)[:200])
                if USE_SPARK:
                    sp = (("'" + lit.replace("\\", "\\\\").replace("'", "\\'") + "'") if isstr else f"X'{lit.hex()}'")
                    sexpr = f"startswith(v, {sp})" if op == "sw" else f"v {dict(eq='=', ne='<>', lt='<', le='<=', gt='>', ge='>=')[op]} {sp}"
                    if op == "sw" and not isstr: sexpr = None
                    if sexpr:
                        for mode, conf in SPARK_MODES.items():
                            for k, v in conf.items(): SPARK.conf.set(k, v)
                            try:
                                got = Counter(r[0] for r in SPARK.read.parquet(d.replace(os.sep, "/")).where(sexpr).select("id").collect())
                                if got != want: rec(kind="spark", mode=mode, **tag, sql=sexpr, got=sorted(got.elements()), want=sorted(want.elements()))
                            except Exception as e: rec(kind="spark_error", mode=mode, **tag, err=repr(e)[:200])
        print("checked", name, wl, len(findings), flush=True)

if USE_SPARK:
    from pyspark.sql import SparkSession
    SPARK = (SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    SPARK.sparkContext.setLogLevel("ERROR")
    SPARK_MODES = {"default": {"spark.sql.parquet.filterPushdown": "true", "spark.sql.parquet.enableVectorizedReader": "true", "spark.hadoop.parquet.filter.columnindex.enabled": "true"},
                   "rowbased": {"spark.sql.parquet.filterPushdown": "true", "spark.sql.parquet.enableVectorizedReader": "false"},
                   "off": {"spark.sql.parquet.filterPushdown": "false", "spark.sql.parquet.enableVectorizedReader": "true"}}

for name, vals, isstr in (("str", STR, True), ("bin", BIN, False)):
    dirs = write_all(name, vals, isstr)
    for wl, d in dirs.items():
        for f in files(d):
            md = pq.ParquetFile(f).metadata
            for i in range(min(md.num_row_groups, 40)):
                st = md.row_group(i).column(1).statistics
                if st is not None and st.has_min_max:
                    rv = sorted(utf8key(x) if isstr else x for x in pq.read_table(f).column("v").to_pylist())
                    pass
    check(name, vals, isstr, dirs)

json.dump(findings, open(f"{ROOT}/findings.json", "w"), default=str, indent=1)
print("TOTAL", len(findings), Counter((f["kind"], f.get("writer_layout")) for f in findings))
