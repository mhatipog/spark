"""v3 hunt HA: Iceberg partition transforms, truncated metrics, float bounds.

Oracle: for each reader, (filtered read through Iceberg metadata) must equal
(the same reader's filter over the full data held in memory). Row ids compared as multisets.
"""
import datetime as dt, decimal as D, json, math, os, shutil, struct, sys, traceback, warnings
from collections import Counter
import pyarrow as pa, pyarrow.compute as pc, polars as pl, duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, IntegerType, LongType, FloatType, DoubleType, DecimalType, DateType,
                             TimestampType, TimestamptzType, StringType, BinaryType)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import (IdentityTransform, BucketTransform, TruncateTransform, DayTransform,
                                  HourTransform, MonthTransform, YearTransform)
import pyiceberg.expressions as E

warnings.filterwarnings("ignore")
ROOT = sys.argv[1] if len(sys.argv) > 1 else "iceroot"
ONLY = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
shutil.rmtree(ROOT, ignore_errors=True); os.makedirs(ROOT)
cat = SqlCatalog("qp", uri=f"sqlite:///{ROOT}/cat.db", warehouse=os.path.abspath(ROOT).replace(os.sep, "/"))
cat.create_namespace("h")

UTC = dt.timezone.utc
def ts(s): return dt.datetime.fromisoformat(s)
f32 = lambda x: struct.unpack("f", struct.pack("f", x))[0]
def nxt32(x, d): return float(__import__("numpy").nextafter(__import__("numpy").float32(x), __import__("numpy").float32(d)))

TYPES = {
  "int":  (IntegerType(), pa.int32(), [-2**31, -11, -10, -9, -5, -1, 0, 1, 4, 5, 9, 10, 11, 2**31 - 1, None]),
  "long": (LongType(), pa.int64(), [-2**63, -10, -1, 0, 1, 10, 10**15, 2**63 - 1, None]),
  "float": (FloatType(), pa.float32(), [-0.0, 0.0, f32(0.1), -f32(0.1), 1.0, f32(3.4e38), float("inf"), float("-inf"), float("nan"), 1e-45, None]),
  "double": (DoubleType(), pa.float64(), [-0.0, 0.0, 0.1, -0.1, 1.0, float("inf"), float("-inf"), float("nan"), 5e-324, None]),
  "dec": (DecimalType(9, 2), pa.decimal128(9, 2), [D.Decimal(x) for x in ["-9999999.99", "-10.00", "-1.05", "-0.01", "0.00", "0.01", "1.05", "4.99", "5.00", "9999999.99"]] + [None]),
  "date": (DateType(), pa.date32(), [dt.date(1, 1, 1), dt.date(1582, 10, 4), dt.date(1582, 10, 15), dt.date(1969, 12, 31), dt.date(1970, 1, 1), dt.date(2020, 2, 29), dt.date(2020, 3, 1), dt.date(9999, 12, 31), None]),
  "ts": (TimestampType(), pa.timestamp("us"), [ts(x) for x in ["1899-12-31 23:59:59.500000", "1969-12-31 23:00:00", "1969-12-31 23:59:59.999999", "1970-01-01 00:00:00", "1970-01-01 00:00:00.000001", "2020-01-01 00:59:59.999999", "2020-01-01 01:00:00", "2020-12-31 23:59:59.999999"]] + [None]),
  "tstz": (TimestamptzType(), pa.timestamp("us", tz="UTC"), [ts(x).replace(tzinfo=UTC) for x in ["1969-12-31 23:59:59.999999", "1970-01-01 00:00:00", "2020-01-01 00:59:59.999999", "2020-01-01 01:00:00"]] + [None]),
  "str": (StringType(), pa.string(), ["", "a", "a\x00", "ab", "b", "Ä", "￿", "\U0001f600", "\U0010ffff", "p" * 16, "p" * 16 + "a", "p" * 16 + "b", "p" * 15 + "\U0010ffff" * 2, "\U0010ffff" * 17, "x" * 15 + "\U0001f600z", "z" * 40, None]),
  "bin": (BinaryType(), pa.binary(), [b"", b"\x00", b"\x7f", b"\x80", b"\xff", b"\xff" * 20, b"\xff" * 16 + b"\x00", b"a" * 17, b"a" * 16 + b"\xff", None]),
}
TRANSFORMS = {
  "int": ["none", "identity", "bucket", "truncate"], "long": ["none", "identity", "bucket", "truncate"],
  "float": ["none", "identity"], "double": ["none", "identity"], "dec": ["none", "identity", "bucket", "truncate"],
  "date": ["none", "identity", "day", "month", "year", "bucket"],
  "ts": ["none", "identity", "hour", "day", "month", "year", "bucket"], "tstz": ["none", "hour", "day", "month", "year"],
  "str": ["none", "identity", "bucket", "truncate"], "bin": ["none", "identity", "truncate", "bucket"],
}
def mk_transform(name):
    return {"identity": IdentityTransform(), "bucket": BucketTransform(3), "truncate": TruncateTransform(4),
            "day": DayTransform(), "hour": HourTransform(), "month": MonthTransform(), "year": YearTransform()}[name]

def neighbours(t, v):
    if v is None: return []
    out = [v]
    if t in ("int", "long"):
        lo, hi = (-2**31, 2**31 - 1) if t == "int" else (-2**63, 2**63 - 1)
        out += [x for x in (v - 1, v + 1) if lo <= x <= hi]
    elif t == "float":
        if not math.isnan(v) and not math.isinf(v): out += [nxt32(v, -1e39), nxt32(v, 1e39), v + 1e-9 if v == 0 else v * (1 + 1e-12)]
    elif t == "double":
        if not math.isnan(v) and not math.isinf(v): out += [math.nextafter(v, -math.inf), math.nextafter(v, math.inf)]
    elif t == "dec": out += [x for x in (v - D.Decimal("0.01"), v + D.Decimal("0.01")) if abs(x) <= D.Decimal("9999999.99")]
    elif t == "date":
        for d in (-1, 1):
            try: out.append(v + dt.timedelta(days=d))
            except OverflowError: pass
    elif t in ("ts", "tstz"): out += [v - dt.timedelta(microseconds=1), v + dt.timedelta(microseconds=1), v + dt.timedelta(microseconds=500)]
    elif t == "str": out += [v + "\x00"] + [v[:k] for k in (1, 3, 4, 15, 16) if k < len(v)]
    elif t == "bin": out += [v + b"\x00"] + [v[:k] for k in (1, 4, 16) if k < len(v)]
    return out

OPS = ["eq", "ne", "lt", "le", "gt", "ge", "in", "notin", "isnull", "notnull", "sw", "nsw", "isnan", "notnan"]

def ice_expr(op, c, lit, t):
    if op == "isnull": return E.IsNull(c)
    if op == "notnull": return E.NotNull(c)
    if op == "isnan": return E.IsNaN(c)
    if op == "notnan": return E.NotNaN(c)
    if op in ("sw", "nsw"): return (E.StartsWith if op == "sw" else E.NotStartsWith)(c, lit)
    if op in ("in", "notin"): return (E.In if op == "in" else E.NotIn)(c, set(lit))
    L = lit
    if t in ("ts",): L = lit.isoformat()
    if t == "tstz": L = lit.isoformat()
    if t == "date": L = lit.isoformat()
    return {"eq": E.EqualTo, "ne": E.NotEqualTo, "lt": E.LessThan, "le": E.LessThanOrEqual, "gt": E.GreaterThan, "ge": E.GreaterThanOrEqual}[op](c, L)

PLT = {"int": pl.Int32, "long": pl.Int64, "float": pl.Float32, "double": pl.Float64, "dec": pl.Decimal(9, 2), "date": pl.Date,
       "ts": pl.Datetime("us"), "tstz": pl.Datetime("us", "UTC"), "str": pl.String, "bin": pl.Binary}
def pl_expr(op, lit, t):
    col = pl.col("v")
    L = (lambda x: pl.lit(x, dtype=PLT[t]))
    if op == "isnull": return col.is_null()
    if op == "notnull": return col.is_not_null()
    if op == "isnan": return col.is_nan()
    if op == "notnan": return col.is_not_nan()
    if op == "sw": return col.str.starts_with(lit) if t == "str" else col.bin.starts_with(lit)
    if op == "nsw": return ~(col.str.starts_with(lit) if t == "str" else col.bin.starts_with(lit))
    if op == "in": return col.is_in(pl.Series(list(lit), dtype=PLT[t]))
    if op == "notin": return ~col.is_in(pl.Series(list(lit), dtype=PLT[t]))
    return {"eq": col == L(lit), "ne": col != L(lit), "lt": col < L(lit), "le": col <= L(lit), "gt": col > L(lit), "ge": col >= L(lit)}[op]

def sql_lit(t, v):
    if t in ("int", "long"): return f"CAST({v} AS {'INTEGER' if t == 'int' else 'BIGINT'})"
    if t in ("float", "double"):
        s = "'nan'" if math.isnan(v) else ("'inf'" if v == math.inf else ("'-inf'" if v == -math.inf else repr(v)))
        return f"CAST({s} AS {'FLOAT' if t == 'float' else 'DOUBLE'})"
    if t == "dec": return f"CAST('{v}' AS DECIMAL(9,2))"
    if t == "date": return f"DATE '{v.isoformat()}'"
    if t == "ts": return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if t == "tstz": return f"TIMESTAMPTZ '{v.isoformat(sep=' ')}'"
    if t == "str": return "'" + v.replace("'", "''") + "'"
    if t == "bin": return "from_hex('" + v.hex() + "')"
def sql_pred(op, lit, t):
    if op == "isnull": return "v IS NULL"
    if op == "notnull": return "v IS NOT NULL"
    if op == "isnan": return "isnan(v)"
    if op == "notnan": return "NOT isnan(v)"
    if op == "sw": return f"starts_with(v, {sql_lit(t, lit)})" if t == "str" else f"v >= {sql_lit(t, lit)} AND substr(v, 1, {len(lit)}) = {sql_lit(t, lit)}"
    if op == "nsw": return f"NOT starts_with(v, {sql_lit(t, lit)})"
    if op in ("in", "notin"): return f"v {'NOT ' if op == 'notin' else ''}IN ({', '.join(sql_lit(t, x) for x in lit)})"
    return f"v {dict(eq='=', ne='<>', lt='<', le='<=', gt='>', ge='>=')[op]} {sql_lit(t, lit)}"

def preds(t, vals):
    lits = []
    for v in vals:
        for n in neighbours(t, v):
            if n not in lits and not (isinstance(n, float) and math.isnan(n) and any(isinstance(x, float) and math.isnan(x) for x in lits)): lits.append(n)
    out = []
    for op in OPS:
        if op in ("isnull", "notnull"): out.append((op, None))
        elif op in ("isnan", "notnan"):
            if t in ("float", "double"): out.append((op, None))
        elif op in ("sw", "nsw"):
            if t in ("str",): out += [(op, l) for l in lits if l != ""]
        elif op in ("in", "notin"):
            ok = [l for l in lits if not (isinstance(l, float) and math.isnan(l))]
            out += [(op, (ok[i], ok[(i * 7 + 3) % len(ok)])) for i in range(0, len(ok), 2)]
        else:
            out += [(op, l) for l in lits if not (isinstance(l, float) and math.isnan(l))]
    return out

findings = []
def record(**k):
    findings.append(k); print("FINDING", json.dumps(k, default=str)[:400], flush=True)

def ids(tbl): return Counter(tbl.column("id").to_pylist()) if tbl is not None else None

for t, (itype, atype, vals) in TYPES.items():
    if ONLY and t not in ONLY: continue
    for tr in TRANSFORMS[t]:
        for layout in ("row_per_file", "one_append"):
            name = f"h.{t}_{tr}_{layout}"
            schema = Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "v", itype, required=False))
            spec = PartitionSpec() if tr == "none" else PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=mk_transform(tr), name="v_p"))
            props = {"write.metadata.metrics.default": "truncate(16)", "format-version": "2"}
            try:
                tab = cat.create_table(name, schema=schema, partition_spec=spec, properties=props)
                rows = pa.table({"id": pa.array(range(len(vals)), pa.int64()), "v": pa.array(vals, atype)})
                if layout == "row_per_file":
                    for i in range(len(vals)): tab.append(rows.slice(i, 1))
                else: tab.append(rows)
                tab = cat.load_table(name)
            except Exception as e:
                record(kind="write_error", table=name, err=repr(e)[:300]); continue
            full = tab.scan().to_arrow()
            if ids(full) != Counter(range(len(vals))):
                record(kind="full_scan_mismatch", table=name, got=sorted(ids(full).elements())); continue
            fullpl = pl.from_arrow(full)
            con = duckdb.connect(); con.sql("LOAD iceberg; SET TimeZone='UTC'")
            con.register("mem", full)
            meta = tab.metadata_location
            n = 0
            for op, lit in preds(t, vals):
                n += 1
                tag = dict(table=name, op=op, lit=lit)
                # reader 1: pyiceberg scan (manifest + partition pruning + pyarrow residual)
                try:
                    want = Counter(full.filter(pc.field("v").isin(pa.array(list(lit), atype)) if op == "in" else None).column("id").to_pylist()) if False else None
                    ref = ids(pl.from_arrow(full).filter(pl_expr(op, lit, t)).to_arrow())  # polars semantics as a sanity anchor
                except Exception as e:
                    ref = None
                try:
                    got = ids(tab.scan(row_filter=ice_expr(op, "v", lit, t)).to_arrow())
                    # pyiceberg's own reference: same expression over an unpartitioned in-memory evaluation
                    from pyiceberg.io.pyarrow import expression_to_pyarrow
                    from pyiceberg.expressions.visitors import bind
                    bound = bind(tab.schema(), ice_expr(op, "v", lit, t), case_sensitive=True)
                    want = ids(full.filter(expression_to_pyarrow(bound)))
                    if got != want: record(kind="pyiceberg", **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                except Exception as e:
                    record(kind="pyiceberg_error", **tag, err=repr(e)[:200])
                # reader 2: polars scan_iceberg, native reader, with and without metadata statistics
                for opts in ({"reader_override": "native"}, {"reader_override": "native", "use_metadata_statistics": False}, {"reader_override": "pyiceberg"}):
                    try:
                        want = ids(fullpl.filter(pl_expr(op, lit, t)).to_arrow())
                        got = ids(pl.scan_iceberg(tab, **opts).filter(pl_expr(op, lit, t)).collect().to_arrow())
                        if got != want: record(kind="polars", opts=opts, **tag, got=sorted(got.elements()), want=sorted(want.elements()))
                    except Exception as e:
                        record(kind="polars_error", opts=opts, **tag, err=repr(e)[:200])
                # reader 3: duckdb iceberg_scan
                try:
                    p = sql_pred(op, lit, t)
                    want = Counter(r[0] for r in con.sql(f"SELECT id FROM mem WHERE {p}").fetchall())
                    got = Counter(r[0] for r in con.sql(f"SELECT id FROM iceberg_scan('{meta}') WHERE {p}").fetchall())
                    if got != want: record(kind="duckdb", **tag, sql=p, got=sorted(got.elements()), want=sorted(want.elements()))
                except Exception as e:
                    record(kind="duckdb_error", **tag, err=repr(e)[:200])
            # aggregates answered from metadata
            try:
                for opts in ({"reader_override": "native"}, {"reader_override": "native", "use_metadata_statistics": False}):
                    a = pl.scan_iceberg(tab, **opts).select(pl.len().alias("n"), pl.col("v").min().alias("mn"), pl.col("v").max().alias("mx"), pl.col("v").null_count().alias("nc")).collect().row(0)
                    b = fullpl.select(pl.len().alias("n"), pl.col("v").min().alias("mn"), pl.col("v").max().alias("mx"), pl.col("v").null_count().alias("nc")).row(0)
                    if repr(a) != repr(b): record(kind="polars_agg", table=name, opts=opts, got=a, want=b)
                a = con.sql(f"SELECT count(*), min(v), max(v), count(v) FROM iceberg_scan('{meta}')").fetchall()[0]
                b = con.sql("SELECT count(*), min(v), max(v), count(v) FROM mem").fetchall()[0]
                if repr(a) != repr(b): record(kind="duckdb_agg", table=name, got=a, want=b)
            except Exception as e:
                record(kind="agg_error", table=name, err=repr(e)[:200])
            print(f"done {name}: {n} predicates, findings so far {len(findings)}", flush=True)

json.dump(findings, open(f"{ROOT}/findings.json", "w"), default=str, indent=1)
print("TOTAL", len(findings), Counter(f["kind"] for f in findings))
