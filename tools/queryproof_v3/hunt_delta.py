"""v3 hunt HC: Delta Lake log statistics (families F1, F3, F4, F8).

Writer: delta-rs (write_deltalake), one file per row and one file for all rows; optional partition column.
Readers: deltalake (to_pyarrow_table with filters, QueryBuilder SQL), Polars scan_delta, DuckDB delta_scan.
Oracle: each reader's filtered read == the same engine filtering the full data in memory.
"""
import datetime as dt, decimal as D, json, math, os, shutil, sys, warnings
from collections import Counter
import pyarrow as pa, polars as pl, duckdb
from deltalake import write_deltalake, DeltaTable, QueryBuilder

warnings.filterwarnings("ignore")
ROOT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "delta"); shutil.rmtree(ROOT, ignore_errors=True); os.makedirs(ROOT)
ONLY = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
UTC = dt.timezone.utc
T = lambda s: dt.datetime.fromisoformat(s)
M = "\U0010ffff"
TYPES = {
  "ts": (pa.timestamp("us", tz="UTC"), [T(x).replace(tzinfo=UTC) for x in ["1969-12-31 23:59:59.999999", "1970-01-01 00:00:00", "1970-01-01 00:00:00.000500", "2020-01-01 00:00:00.000999", "2020-01-01 00:00:00.001", "2020-01-01 00:00:00.001001", "1899-12-31 23:59:59.999500"]] + [None]),
  "tsntz": (pa.timestamp("us"), [T(x) for x in ["1969-12-31 23:59:59.999999", "1970-01-01 00:00:00.000500", "2020-01-01 00:00:00.000999", "2020-01-01 00:00:00.001001"]] + [None]),
  "str": (pa.string(), ["", "a", "a" * 32, "a" * 32 + "b", "a" * 31 + M, "a" * 31 + M + "x", "a" * 30 + "\U0001f600b", "a" * 32 + "�", "a" * 32 + "\U0001f600", M * 40, "�" * 33, "z" * 40, None]),
  "dbl": (pa.float64(), [-0.0, 0.0, 0.1, float("nan"), float("inf"), float("-inf"), -1.5, None]),
  "flt": (pa.float32(), [-0.0, 0.0, 0.1, float("nan"), 1.0, None]),
  "dec": (pa.decimal128(38, 18), [D.Decimal(x) for x in ["-1.000000000000000001", "0.000000000000000001", "12345678901234567890.123456789012345678", "-0.5"]] + [None]),
  "i64": (pa.int64(), [-2**63, -1, 0, 2**53 + 1, 2**63 - 1, None]),
  "date": (pa.date32(), [dt.date(1, 1, 1), dt.date(1582, 10, 10), dt.date(1969, 12, 31), dt.date(9999, 12, 31), None]),
}
PLT = {"ts": pl.Datetime("us", "UTC"), "tsntz": pl.Datetime("us"), "str": pl.String, "dbl": pl.Float64, "flt": pl.Float32,
       "dec": pl.Decimal(38, 18), "i64": pl.Int64, "date": pl.Date}

def neigh(t, v):
    if v is None: return []
    o = [v]
    if t in ("ts", "tsntz"): o += [v - dt.timedelta(microseconds=1), v + dt.timedelta(microseconds=1), v.replace(microsecond=(v.microsecond // 1000) * 1000), v + dt.timedelta(microseconds=999)]
    elif t == "str": o += [v[:k] for k in (31, 32, 33) if k < len(v)] + [v + "\x00"]
    elif t == "dbl" and math.isfinite(v): o += [math.nextafter(v, -math.inf), math.nextafter(v, math.inf)]
    elif t == "i64": o += [x for x in (v - 1, v + 1) if -2**63 <= x < 2**63]
    elif t == "dec": o += [v + D.Decimal("0.000000000000000001")]
    return o

def sql_lit(t, v, eng):
    if t in ("ts",): return f"TIMESTAMPTZ '{v.isoformat(sep=' ')}'" if eng == "duckdb" else f"TIMESTAMP '{v.isoformat()}'"
    if t == "tsntz": return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if t == "str": return "'" + v.replace("'", "''") + "'"
    if t in ("dbl", "flt"):
        s = "'nan'" if math.isnan(v) else ("'inf'" if v == math.inf else ("'-inf'" if v == -math.inf else repr(v)))
        return f"CAST({s} AS {'DOUBLE' if t == 'dbl' else ('FLOAT' if eng == 'duckdb' else 'REAL')})"
    if t == "dec": return f"CAST('{v}' AS DECIMAL(38,18))"
    if t == "i64": return f"CAST({v} AS BIGINT)"
    if t == "date": return f"DATE '{v.isoformat()}'"
OPS = ["eq", "ne", "lt", "le", "gt", "ge", "in", "notin", "isnull", "notnull"]
def sql_pred(op, t, lit, eng):
    if op == "isnull": return "v IS NULL"
    if op == "notnull": return "v IS NOT NULL"
    if op in ("in", "notin"): return f"v {'NOT ' if op == 'notin' else ''}IN ({', '.join(sql_lit(t, x, eng) for x in lit)})"
    return f"v {dict(eq='=', ne='<>', lt='<', le='<=', gt='>', ge='>=')[op]} {sql_lit(t, lit, eng)}"
def pl_pred(op, t, lit):
    c = pl.col("v"); L = lambda x: pl.lit(x, dtype=PLT[t])
    if op == "isnull": return c.is_null()
    if op == "notnull": return c.is_not_null()
    if op == "in": return c.is_in(pl.Series(list(lit), dtype=PLT[t]))
    if op == "notin": return ~c.is_in(pl.Series(list(lit), dtype=PLT[t]))
    return {"eq": c == L(lit), "ne": c != L(lit), "lt": c < L(lit), "le": c <= L(lit), "gt": c > L(lit), "ge": c >= L(lit)}[op]
def dl_filter(op, lit):
    if op in ("isnull", "notnull"): return None
    return [("v", {"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">=", "in": "in", "notin": "not in"}[op], list(lit) if op in ("in", "notin") else lit)]

def preds(t, vals):
    lits = []
    for v in vals:
        for n in neigh(t, v):
            if not any(type(n) == type(x) and (n == x or (isinstance(n, float) and math.isnan(n) and math.isnan(x))) and str(n) == str(x) for x in lits): lits.append(n)
    out = [("isnull", None), ("notnull", None)]
    nn = [l for l in lits if not (isinstance(l, float) and math.isnan(l))]
    for op in ("eq", "ne", "lt", "le", "gt", "ge"): out += [(op, l) for l in nn]
    for op in ("in", "notin"): out += [(op, (nn[i], nn[(i * 5 + 2) % len(nn)])) for i in range(0, len(nn), 2)]
    return out

findings = []
def rec(**k): findings.append(k); print("FINDING", json.dumps(k, default=str)[:350], flush=True)
ids = lambda xs: Counter(xs)

for t, (atype, vals) in TYPES.items():
    if ONLY and t not in ONLY: continue
    for layout in ("row_per_file", "one_file", "partitioned"):
        path = f"{ROOT}/{t}_{layout}"
        tbl = pa.table({"id": pa.array(range(len(vals)), pa.int64()), "v": pa.array(vals, atype),
                        "g": pa.array([i % 2 for i in range(len(vals))], pa.int32())})
        try:
            if layout == "row_per_file":
                for i in range(len(vals)): write_deltalake(path, tbl.slice(i, 1), mode="append")
            elif layout == "one_file": write_deltalake(path, tbl)
            else: write_deltalake(path, tbl, partition_by=["g"])
            dtab = DeltaTable(path)
        except Exception as e:
            rec(kind="write_error", table=f"{t}_{layout}", err=repr(e)[:300]); continue
        full = pl.from_arrow(tbl)
        con = duckdb.connect(); con.sql("LOAD delta; SET TimeZone='UTC'"); con.register("mem", tbl)
        qb = QueryBuilder().register("t", dtab)
        from datafusion import SessionContext
        dfm = SessionContext(); dfm.register_record_batches("mem", [tbl.to_batches()])
        for op, lit in preds(t, vals):
            tag = dict(table=f"{t}_{layout}", op=op, lit=lit)
            try:
                want = ids(full.filter(pl_pred(op, t, lit))["id"].to_list())
                got = ids(pl.scan_delta(path).filter(pl_pred(op, t, lit)).collect()["id"].to_list())
                if got != want: rec(kind="polars", **tag, got=sorted(got.elements()), want=sorted(want.elements()))
            except Exception as e: rec(kind="polars_error", **tag, err=repr(e)[:200])
            try:
                p = sql_pred(op, t, lit, "duckdb")
                want = ids(r[0] for r in con.sql(f"SELECT id FROM mem WHERE {p}").fetchall())
                got = ids(r[0] for r in con.sql(f"SELECT id FROM delta_scan('{path}') WHERE {p}").fetchall())
                if got != want: rec(kind="duckdb", **tag, sql=p, got=sorted(got.elements()), want=sorted(want.elements()))
            except Exception as e: rec(kind="duckdb_error", **tag, err=repr(e)[:200])
            try:
                p = sql_pred(op, t, lit, "df")
                want = ids(dfm.sql(f"SELECT id FROM mem WHERE {p}").to_arrow_table().column("id").to_pylist())
                got = ids(pa.table(qb.execute(f"SELECT id FROM t WHERE {p}").read_all()).column("id").to_pylist())
                if got != want: rec(kind="deltalake_sql", **tag, sql=p, got=sorted(got.elements()), want=sorted(want.elements()))
            except Exception as e: rec(kind="deltalake_sql_error", **tag, err=repr(e)[:200])
            f = dl_filter(op, lit)
            if f and layout == "partitioned": pass
        # aggregates possibly answered from the log
        try:
            a = pl.scan_delta(path).select(pl.len(), pl.col("v").min().alias("mn"), pl.col("v").max().alias("mx"), pl.col("v").null_count().alias("nc")).collect().row(0)
            b = full.select(pl.len(), pl.col("v").min().alias("mn"), pl.col("v").max().alias("mx"), pl.col("v").null_count().alias("nc")).row(0)
            if repr(a) != repr(b): rec(kind="polars_agg", table=f"{t}_{layout}", got=a, want=b)
            a = con.sql(f"SELECT count(*), min(v), max(v), count(v) FROM delta_scan('{path}')").fetchall()[0]
            b = con.sql("SELECT count(*), min(v), max(v), count(v) FROM mem").fetchall()[0]
            if repr(a) != repr(b): rec(kind="duckdb_agg", table=f"{t}_{layout}", got=a, want=b)
        except Exception as e: rec(kind="agg_error", table=f"{t}_{layout}", err=repr(e)[:200])
        print("done", t, layout, len(findings), flush=True)

json.dump(findings, open(f"{ROOT}/findings.json", "w"), default=str, indent=1)
print("TOTAL", len(findings), Counter(f["kind"] for f in findings))
