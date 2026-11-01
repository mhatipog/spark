"""Fixed predicate and aggregate templates per concept, rendered per SQL dialect and evaluated in Python.

A query is a tuple tree. Expressions:
  ('col', path)                   path: tuple of field names; a map lookup is ('key', 'a')
  ('lit', value, type)            type in the shapes.py language; rendered as CAST('..' AS T)
  ('isnull', e) ('notnull', e) ('not', e) ('and', a, b) ('or', a, b)
  ('cmp', op, a, b)               op in = <> < <= > >=
  ('nseq', a, b)                  NULL-safe equality
  ('between', e, lo, hi) ('in', e, [lit...]) ('coalesce', e, lit) ('card', e) ('contains', e, lit)
  ('lcase_eq', e, lit)            Spark UTF8_LCASE collated equality (Spark only)
Queries: ('rows', pred) -> SELECT k FROM t WHERE pred
         ('agg', path, pred|None) -> SELECT count(*), count(x), min(x), max(x)[, sum(x)] FROM t [WHERE]
         ('grouped', pred) -> SELECT k, count(*) FROM t WHERE pred GROUP BY k
Python evaluation follows SQL three-valued logic. It returns None (no expected answer) for
anything whose semantics differ between engines by design: NaN, signed zero in results,
array membership with NULL elements, collations.
"""
import datetime
import math
from decimal import Decimal, getcontext

getcontext().prec = 100  # DECIMAL(38) sums and normalize() must not round

from shapes import is_nan, parse_type, sql_type

NUMERIC = ("int8", "int16", "int32", "int64", "float32", "float64", "decimal")


class NoExpected(Exception):
    """The expected answer is engine-specific."""


# ---------------------------------------------------------------- rendering
def lit_sql(v, ty, dialect):
    t = parse_type(ty)
    if v is None:
        return f"CAST(NULL AS {sql_type(t, dialect)})"
    if t[0] in ("float32", "float64"):
        s = "NaN" if is_nan(v) else ("Infinity" if v == math.inf else "-Infinity" if v == -math.inf else repr(v))
        if dialect == "trino" and s in ("NaN", "Infinity", "-Infinity"):
            return {"NaN": "nan()", "Infinity": "infinity()", "-Infinity": "-infinity()"}[s] if t[0] == "float64" \
                else f"CAST({ {'NaN': 'nan()', 'Infinity': 'infinity()', '-Infinity': '-infinity()'}[s]} AS REAL)"
        return f"CAST('{s}' AS {sql_type(t, dialect)})"
    if t[0] == "string":
        return "'" + v.replace("'", "''") + "'"
    if t[0] == "ts":  # an aware UTC datetime
        text = v.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"CAST('{text}{'+00' if dialect == 'duckdb' else ''}' AS {sql_type(t, dialect)})"
    return f"CAST('{v}' AS {sql_type(t, dialect)})"


def col_sql(path, dialect):
    out = path[0]
    for p in path[1:]:
        if isinstance(p, tuple):
            out = {"spark": f"try_element_at({out}, '{p[1]}')", "trino": f"element_at({out}, '{p[1]}')"}.get(
                dialect, f"{out}['{p[1]}']")
        else:
            out = f"{out}.{p}"
    return out


def expr_sql(e, d):
    k = e[0]
    if k == "col":
        return col_sql(e[1], d)
    if k == "lit":
        return lit_sql(e[1], e[2], d)
    if k == "isnull":
        return f"({expr_sql(e[1], d)} IS NULL)"
    if k == "notnull":
        return f"({expr_sql(e[1], d)} IS NOT NULL)"
    if k == "not":
        return f"(NOT {expr_sql(e[1], d)})"
    if k in ("and", "or"):
        return f"({expr_sql(e[1], d)} {k.upper()} {expr_sql(e[2], d)})"
    if k == "cmp":
        return f"({expr_sql(e[2], d)} {e[1]} {expr_sql(e[3], d)})"
    if k == "nseq":
        op = "<=>" if d == "spark" else "IS NOT DISTINCT FROM"
        return f"({expr_sql(e[1], d)} {op} {expr_sql(e[2], d)})"
    if k == "between":
        return f"({expr_sql(e[1], d)} BETWEEN {expr_sql(e[2], d)} AND {expr_sql(e[3], d)})"
    if k == "in":
        return f"({expr_sql(e[1], d)} IN ({', '.join(expr_sql(x, d) for x in e[2])}))"
    if k == "coalesce":
        return f"(coalesce({expr_sql(e[1], d)}, {expr_sql(e[2], d)}) = {expr_sql(e[2], d)})"
    if k == "card":
        return f"cardinality({expr_sql(e[1], d)})"
    if k == "contains":
        fn = {"spark": "array_contains", "duckdb": "list_contains", "datafusion": "array_has",
              "trino": "contains"}[d]
        return f"{fn}({expr_sql(e[1], d)}, {expr_sql(e[2], d)})"
    if k == "lcase_eq":
        return f"({expr_sql(e[1], d)} COLLATE UTF8_LCASE = {expr_sql(e[2], d)})"
    raise ValueError(k)


def query_sql(q, d, leaf_types):
    if q[0] == "rows":
        return f"SELECT k FROM t WHERE {expr_sql(q[1], d)}"
    if q[0] == "grouped":
        return f"SELECT k, count(*) FROM t WHERE {expr_sql(q[1], d)} GROUP BY k"
    path, pred = q[1], q[2]
    c = col_sql(path, d)
    aggs = [f"coalesce(count(*), -1)", f"coalesce(count({c}), -1)", f"min({c})", f"max({c})"]
    if parse_type(leaf_types[path])[0] in NUMERIC:
        aggs.append(f"sum({c})")
    return f"SELECT {', '.join(aggs)} FROM t" + (f" WHERE {expr_sql(pred, d)}" if pred else "")


def supported(q, d):
    """lcase_eq is Spark-only; everything else renders everywhere."""
    def walk(e):
        if isinstance(e, tuple):
            if e and e[0] == "lcase_eq" and d != "spark":
                return False
            return all(walk(x) for x in e[1:] if isinstance(x, (tuple, list)))
        if isinstance(e, list):
            return all(walk(x) for x in e)
        return True
    return walk(q)


# ---------------------------------------------------------------- evaluation
def get_path(row, path):
    v = row
    for p in path:
        if v is None:
            return None
        if isinstance(p, tuple):
            v = dict(v).get(p[1])
        else:
            v = v[p]
    return v


def _cmp(op, a, b):
    if a is None or b is None:
        return None
    if is_nan(a) or is_nan(b):
        raise NoExpected
    return {"=": a == b, "<>": a != b, "<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]


def ev(e, row):
    k = e[0]
    if k == "col":
        return get_path(row, e[1])
    if k == "lit":
        return e[1]
    if k == "isnull":
        return ev(e[1], row) is None
    if k == "notnull":
        return ev(e[1], row) is not None
    if k == "not":
        v = ev(e[1], row)
        return None if v is None else not v
    if k == "and":
        a, b = ev(e[1], row), ev(e[2], row)
        if a is False or b is False:
            return False
        return None if a is None or b is None else True
    if k == "or":
        a, b = ev(e[1], row), ev(e[2], row)
        if a is True or b is True:
            return True
        return None if a is None or b is None else False
    if k == "cmp":
        return _cmp(e[1], ev(e[2], row), ev(e[3], row))
    if k == "nseq":
        a, b = ev(e[1], row), ev(e[2], row)
        if a is None or b is None:
            return a is None and b is None
        return _cmp("=", a, b)
    if k == "between":
        return ev(("and", ("cmp", ">=", e[1], e[2]), ("cmp", "<=", e[1], e[3])), row)
    if k == "in":
        v = ev(e[1], row)
        if v is None:
            return None
        res = [_cmp("=", v, ev(x, row)) for x in e[2]]
        return True if True in res else (None if None in res else False)
    if k == "coalesce":
        v = ev(e[1], row)
        return _cmp("=", e[2][1] if v is None else v, e[2][1])
    if k == "card":
        v = ev(e[1], row)
        return None if v is None else len(v)
    raise NoExpected  # contains, lcase_eq: engine-specific by design


def expected(q, rows, leaf_types):
    """Normalized expected bag, or None when it is engine-specific."""
    try:
        if q[0] in ("rows", "grouped"):
            keep = [r for r in rows if ev(q[1], r) is True]
            if q[0] == "rows":
                return sorted(norm([r["k"]]) for r in keep)
            from collections import Counter
            return sorted(norm(list(kv)) for kv in Counter(r["k"] for r in keep).items())
        path, pred = q[1], q[2]
        ty = parse_type(leaf_types[path])
        if ty[0] in ("float32", "float64"):
            raise NoExpected
        keep = [r for r in rows if pred is None or ev(pred, r) is True]
        vals = [get_path(r, path) for r in keep]
        nn = [v for v in vals if v is not None]
        out = [len(keep), len(nn), min(nn) if nn else None, max(nn) if nn else None]
        if ty[0] in NUMERIC:
            out.append(sum(nn) if nn else None)
        return [norm(out)]
    except NoExpected:
        return None


def norm(v):
    """A JSON-stable value for comparing bags across engines."""
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    if isinstance(v, dict):
        return {k: norm(x) for k, x in sorted(v.items())}
    if isinstance(v, bool):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        # 12 significant digits: a floating-point SUM depends on the order of its inputs.
        return 0.0 if v == 0 else float(f"{v:.12g}")
    if isinstance(v, Decimal):
        return str(v.normalize()) if v != 0 else "0"
    if isinstance(v, int):
        return v
    if isinstance(v, datetime.datetime) and v.tzinfo is not None:  # compare instants in UTC
        return v.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def bag(rows):
    import json
    return sorted(json.dumps(norm(list(r)), sort_keys=True, default=str) for r in rows)


# ---------------------------------------------------------------- generation
def leaves(columns, prefix=()):
    """Atomic leaf paths with their types; map values are reached through key 'a'."""
    out = {}
    for name, ty in columns:
        t = parse_type(ty) if isinstance(ty, str) else ty
        path = prefix + (name,)
        if t[0] == "struct":
            out.update(leaves(t[1], path))
        elif t[0] == "map":
            out[path + (("key", "a"),)] = _unparse(t[1])
        elif t[0] != "list":
            out[path] = _unparse(t)
    return out


def containers(columns, prefix=()):
    out = {}
    for name, ty in columns:
        t = parse_type(ty) if isinstance(ty, str) else ty
        path = prefix + (name,)
        if t[0] in ("struct", "list", "map"):
            out[path] = t
        if t[0] == "struct":
            out.update(containers(t[1], path))
    return out


def _unparse(t):
    if t[0] == "decimal":
        return f"decimal({t[1]},{t[2]})"
    return t[0]


def neighbours(vals, ty):
    """Literal values at, just below and just above each value, plus the type's specials."""
    t = parse_type(ty)
    out = []
    for v in sorted(set(v for v in vals if v is not None and not is_nan(v)), key=repr):
        if t[0] in ("int8", "int16", "int32", "int64"):
            out += [v - 1, v, v + 1]
        elif t[0] == "ts":
            for delta in (datetime.timedelta(microseconds=-1), datetime.timedelta(0),
                          datetime.timedelta(microseconds=1), datetime.timedelta(days=1)):
                try:
                    out.append(v + delta)
                except OverflowError:
                    pass
        elif t[0] in ("float32", "float64"):
            out += [math.nextafter(v, -math.inf), v, math.nextafter(v, math.inf)] if math.isfinite(v) else [v]
        elif t[0] == "decimal":
            step = Decimal(1).scaleb(-t[2])
            out += [v - step, v, v + step]
        elif t[0] == "date":
            for dd in (-1, 0, 1):
                try:
                    out.append(v + datetime.timedelta(days=dd))
                except OverflowError:
                    pass
        elif t[0] == "string":
            out += [v, v[:-1], v + "\x00"]
    if t[0] in ("float32", "float64"):
        out += [math.nan, -0.0, 0.0, math.inf, -math.inf]
    if t[0] == "string":
        out += ["", "￿", "\U0010ffff"]
    bits = {"int8": 8, "int16": 16, "int32": 32, "int64": 64}.get(t[0])
    if bits:
        out = [v for v in out if -2 ** (bits - 1) <= v < 2 ** (bits - 1)]
    if t[0] == "decimal":
        lim = Decimal(10) ** (t[1] - t[2])
        out = [v for v in out if -lim < v < lim]
    if t[0] == "date":
        out = [v for v in out if datetime.date(1, 1, 2) <= v <= datetime.date(9999, 12, 31)]
    seen, uniq = set(), []
    for v in out:
        key = repr(v)
        if key not in seen:
            seen.add(key)
            uniq.append(v)
    return uniq


def build_queries(shape, max_lits=12):
    """Every query for one shape, as (qid, query) pairs; qids are stable across runs."""
    cols = shape["read_as"] or shape["columns"]
    lt = leaves(cols)
    if "random_seed" in shape:  # the matched random baseline (random_gen.py)
        import random_gen
        return random_gen.random_queries(shape, lt), lt
    rows = shape_rows(shape)
    qs = []
    for path, ty in lt.items():
        if path == ("k",):
            continue
        col = ("col", path)
        name = ".".join(p if isinstance(p, str) else f"[{p[1]}]" for p in path)
        vals = [get_path(r, path) for r in rows]
        lits = neighbours(vals, ty)
        if len(lits) > max_lits:  # keep both ends and an even spread
            step = (len(lits) - 1) / (max_lits - 1)
            lits = [lits[round(i * step)] for i in range(max_lits)]
        L = lambda v: ("lit", v, ty)  # noqa: E731
        for p_id, pred in [("isnull", ("isnull", col)), ("notnull", ("notnull", col)),
                           ("nseq_null", ("nseq", col, L(None))), ("not_notnull", ("not", ("notnull", col)))]:
            qs.append((f"{name}:{p_id}", ("rows", pred)))
        for i, v in enumerate(lits):
            for op in ("=", "<>", "<", "<=", ">", ">="):
                qs.append((f"{name}:{op}:{i}", ("rows", ("cmp", op, col, L(v)))))
            qs.append((f"{name}:isnull_or_eq:{i}", ("rows", ("or", ("isnull", col), ("cmp", "=", col, L(v))))))
            qs.append((f"{name}:coalesce:{i}", ("rows", ("coalesce", col, L(v)))))
            qs.append((f"{name}:nseq:{i}", ("rows", ("nseq", col, L(v)))))
            qs.append((f"{name}:agg_lt:{i}", ("agg", path, ("cmp", "<", col, L(v)))))
            if ty == "string":
                qs.append((f"{name}:lcase:{i}", ("rows", ("lcase_eq", col, L(v)))))
        if lits:
            qs.append((f"{name}:in", ("rows", ("in", col, [L(v) for v in lits[::2]]))))
            qs.append((f"{name}:between", ("rows", ("between", col, L(lits[0]), L(lits[len(lits) // 2])))))
        qs.append((f"{name}:agg", ("agg", path, None)))
        qs.append((f"{name}:agg_none", ("agg", path, ("cmp", "<", ("col", ("k",)), ("lit", -1000, "int32")))))
        qs.append((f"{name}:agg_isnull", ("agg", path, ("isnull", col))))
        qs.append((f"{name}:agg_notnull", ("agg", path, ("notnull", col))))
    for path, t in containers(cols).items():
        col, name = ("col", path), ".".join(path)
        qs.append((f"{name}:c_isnull", ("rows", ("isnull", col))))
        qs.append((f"{name}:c_notnull", ("rows", ("notnull", col))))
        if t[0] in ("list", "map"):
            for n in (0, 1, 2):
                qs.append((f"{name}:card_eq:{n}", ("rows", ("cmp", "=", ("card", col), ("lit", n, "int32")))))
        if t[0] == "list":
            qs.append((f"{name}:contains", ("rows", ("contains", col, ("lit", 1, "int32")))))
    qs.append(("k:grouped_none", ("grouped", ("cmp", "<", ("col", ("k",)), ("lit", -1000, "int32")))))
    qs.append(("k:grouped_all", ("grouped", ("cmp", ">", ("col", ("k",)), ("lit", -1000, "int32")))))
    return qs, lt


def shape_rows(shape):
    """The logical table: the union of the file contents, as dicts under the read schema."""
    names = [c[0] for c in shape["columns"]]
    out = []
    for f in shape["files"]:
        for i in f:
            r = dict(zip(names, shape["rows"][i]))
            for c in (shape["read_as"] or []):
                r.setdefault(c[0], None)
            for name, ty in (shape["read_as"] or []):
                if ty == "float64" and isinstance(r[name], float):
                    import struct
                    r[name] = struct.unpack("f", struct.pack("f", r[name]))[0]  # float32 widened exactly
                if ty == "float64" and isinstance(r[name], int):
                    r[name] = float(r[name])
                if ty.startswith("decimal") and isinstance(r[name], int):
                    r[name] = Decimal(r[name])
            out.append(r)
    for r in out:  # float32 columns hold float32 values
        for name, ty in shape["columns"]:
            if ty == "float32" and isinstance(r.get(name), float) and not (shape["read_as"] and
                                                                            dict(shape["read_as"])[name] == "float64"):
                import struct
                r[name] = struct.unpack("f", struct.pack("f", r[name]))[0]
    return out
