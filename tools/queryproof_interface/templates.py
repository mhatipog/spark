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
         ('grouped_col', path, pred) -> SELECT x, count(*) FROM t WHERE pred GROUP BY x
         ('scalar', path, pred) -> SELECT (SELECT count(x) FROM t WHERE pred), (SELECT max(x) FROM t WHERE pred)
           (the last two from contract C2's templates, added in pair three)
Python evaluation follows SQL three-valued logic. It returns None (no expected answer) for
anything whose semantics differ between engines by design: NaN, signed zero in results,
array membership with NULL elements, collations.
"""
import datetime
import math
from decimal import Decimal, getcontext

getcontext().prec = 100  # DECIMAL(38) sums and normalize() must not round

from shapes import is_nan, parse_type, sql_type

NUMERIC = ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "decimal")
INT_BITS = {"int8": 8, "int16": 16, "int32": 32, "int64": 64, "uint8": 8, "uint16": 16, "uint32": 32, "uint64": 64}


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
        return f"CAST('{text}{'+00' if dialect in ('duckdb', 'polars') else ''}' AS {sql_type(t, dialect)})"
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
        return f"{'array_length' if d == 'polars' else 'cardinality'}({expr_sql(e[1], d)})"
    if k == "contains":
        fn = {"spark": "array_contains", "duckdb": "list_contains", "datafusion": "array_has",
              "trino": "contains", "polars": "array_contains"}[d]
        return f"{fn}({expr_sql(e[1], d)}, {expr_sql(e[2], d)})"
    if k == "lcase_eq":
        return f"({expr_sql(e[1], d)} COLLATE UTF8_LCASE = {expr_sql(e[2], d)})"
    if k == "cast":  # ("cast", expr, target type, try): phase two, O3
        fn = ("try_cast" if d in ("spark", "trino") else "TRY_CAST") if e[3] else "CAST"
        return f"{fn}({expr_sql(e[1], d)} AS {sql_type(parse_type(e[2]), d)})"
    if k == "truth":  # a boolean expression used as the whole predicate
        return expr_sql(e[1], d)
    raise ValueError(k)


def query_sql(q, d, leaf_types):
    if q[0] == "rows":
        return f"SELECT k FROM t WHERE {expr_sql(q[1], d)}"
    if q[0] == "grouped":
        return f"SELECT k, count(*) FROM t WHERE {expr_sql(q[1], d)} GROUP BY k"
    path, pred = q[1], q[2]
    c = col_sql(path, d)
    if q[0] == "grouped_col":
        return f"SELECT {c}, count(*) FROM t WHERE {expr_sql(pred, d)} GROUP BY {c}"
    if q[0] == "scalar":
        w = expr_sql(pred, d)
        return f"SELECT (SELECT count({c}) FROM t WHERE {w}), (SELECT max({c}) FROM t WHERE {w})"
    aggs = [f"coalesce(count(*), -1)", f"coalesce(count({c}), -1)", f"min({c})", f"max({c})"]
    if parse_type(leaf_types[path])[0] in NUMERIC and parse_type(leaf_types[path])[0] != "uint64":
        aggs.append(f"sum({c})")  # no uint64 SUM: its overflow behaviour is engine-specific
    return f"SELECT {', '.join(aggs)} FROM t" + (f" WHERE {expr_sql(pred, d)}" if pred else "")


def supported(q, d):
    """lcase_eq is Spark-only; Polars has no MAP type (a map reads as a list of structs), and
    pyarrow.dataset takes only what pa_expr() translates."""
    if d == "pyarrow":
        try:
            pa_query(q)
            return True
        except NoExpected:
            return False

    def walk(e):
        if isinstance(e, tuple):
            if e and e[0] == "lcase_eq" and d != "spark":
                return False
            if d == "polars" and e and e[0] == "col" and any(isinstance(p, tuple) for p in e[1]):
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
    # a DATE against a TIMESTAMP compares as midnight UTC (every engine runs with a UTC session)
    if isinstance(a, datetime.date) and not isinstance(a, datetime.datetime) and isinstance(b, datetime.datetime):
        a = datetime.datetime(a.year, a.month, a.day, tzinfo=datetime.timezone.utc)
    if isinstance(b, datetime.date) and not isinstance(b, datetime.datetime) and isinstance(a, datetime.datetime):
        b = datetime.datetime(b.year, b.month, b.day, tzinfo=datetime.timezone.utc)
    if isinstance(a, bool) != isinstance(b, bool):
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
    if k == "cast":
        return cast_value(ev(e[1], row), e[1], e[2], e[3])
    if k == "truth":
        return ev(e[1], row)
    raise NoExpected  # contains, lcase_eq: engine-specific by design


STRICT_INT = __import__("re").compile(r"-?[0-9]{1,18}")
STRICT_DATE = __import__("re").compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def cast_value(v, src, target, try_):
    """The standard SQL result of a cast, or NoExpected where engines legitimately differ
    (loose string formats, float-to-int rounding, decimal-to-int rounding, plain narrowing)."""
    if v is None:
        return None
    if isinstance(v, float) and is_nan(v):
        raise NoExpected
    t = parse_type(target)[0]
    if t == "float64":
        if isinstance(v, (int, Decimal)) and not isinstance(v, bool):
            return float(v)
        if isinstance(v, float):
            return v
        raise NoExpected
    if t == "bool":
        if isinstance(v, int) and not isinstance(v, bool):
            return v != 0
        raise NoExpected
    if t in INT_BITS:
        bits = INT_BITS[t]
        lo, hi = (0, 2 ** bits - 1) if t.startswith("uint") else (-2 ** (bits - 1), 2 ** (bits - 1) - 1)
        if isinstance(v, int) and not isinstance(v, bool):
            if lo <= v <= hi:
                return v
            if try_:
                return None
            raise NoExpected  # a plain narrowing cast: error or wrap, by engine
        if isinstance(v, str):
            if STRICT_INT.fullmatch(v):
                n = int(v)
                return n if lo <= n <= hi else (None if try_ else _no())
            if not any(ch.isdigit() for ch in v) and try_:
                return None
        raise NoExpected
    if t == "string":
        if isinstance(v, int) and not isinstance(v, bool):
            return str(v)
        if isinstance(v, str):
            return v
        raise NoExpected
    if t == "date":
        if isinstance(v, datetime.datetime):
            return v.astimezone(datetime.timezone.utc).date()
        if isinstance(v, datetime.date):
            return v
        if isinstance(v, str):
            if STRICT_DATE.fullmatch(v):
                try:
                    return datetime.date.fromisoformat(v)
                except ValueError:
                    pass
            elif not any(ch.isdigit() for ch in v) and try_:
                return None
        raise NoExpected
    if t == "ts":
        if isinstance(v, datetime.datetime):
            return v
        if isinstance(v, datetime.date):
            return datetime.datetime(v.year, v.month, v.day, tzinfo=datetime.timezone.utc)
        raise NoExpected
    raise NoExpected


def _no():
    raise NoExpected


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
        if q[0] == "grouped_col":
            from collections import Counter
            return sorted((norm([v, n]) for v, n in Counter(vals).items()), key=repr)
        if q[0] == "scalar":
            return [norm([len(nn), max(nn) if nn else None])]
        out = [len(keep), len(nn), min(nn) if nn else None, max(nn) if nn else None]
        if ty[0] in NUMERIC and ty[0] != "uint64":
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
        # integral decimals as ints (pair three): UINT_64 reads as DECIMAL(20,0) in Spark and Trino
        # but as an integer elsewhere; both sides of every comparison go through norm()
        if v == v.to_integral_value():
            return int(v)
        return str(v.normalize())
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
        if t[0] in INT_BITS:
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
    bits = INT_BITS.get(t[0])
    if bits and t[0].startswith("uint"):
        out = [v for v in out if 0 <= v < 2 ** bits]
    elif bits:
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
        none = ("cmp", "<", ("col", ("k",)), ("lit", -1000, "int32"))  # prunes every file
        every = ("cmp", ">", ("col", ("k",)), ("lit", -1000, "int32"))
        qs.append((f"{name}:scalar_none", ("scalar", path, none)))
        qs.append((f"{name}:scalar_all", ("scalar", path, every)))
        if ty not in ("float32", "float64"):
            qs.append((f"{name}:grouped_col_none", ("grouped_col", path, none)))
            qs.append((f"{name}:grouped_col_all", ("grouped_col", path, every)))
    for path, t in containers(cols).items():
        col, name = ("col", path), ".".join(path)
        qs.append((f"{name}:c_isnull", ("rows", ("isnull", col))))
        qs.append((f"{name}:c_notnull", ("rows", ("notnull", col))))
        if t[0] in ("list", "map"):
            for n in (0, 1, 2):
                qs.append((f"{name}:card_eq:{n}", ("rows", ("cmp", "=", ("card", col), ("lit", n, "int32")))))
        if t[0] == "list":
            qs.append((f"{name}:contains", ("rows", ("contains", col, ("lit", 1, "int32")))))
    if __import__("os").environ.get("QP_O3"):  # phase two, round B: cast and mixed-type predicates
        qs += cast_templates(lt, rows)
    # Pair four (C2 E4): predicates that mix a partition column with a data column, so that
    # partition pruning and file skipping must agree under three-valued logic.
    part = shape.get("partition", [])
    data = [p for p in lt if p != ("k",) and p[0] not in part] or [("k",)]
    for pname in part:
        pcol, pty = ("col", (pname,)), lt[(pname,)]
        P = lambda v: ("lit", v, pty)  # noqa: E731
        c, cty = ("col", data[0]), lt[data[0]]
        pvals = sorted({r[pname] for r in rows if r[pname] is not None}, key=repr)
        for i, v in enumerate(pvals):
            qs.append((f"{pname}:p_or_cnull:{i}", ("rows", ("or", ("cmp", "=", pcol, P(v)), ("isnull", c)))))
            qs.append((f"{pname}:p_and_cnotnull:{i}", ("rows", ("and", ("cmp", "=", pcol, P(v)), ("notnull", c)))))
            qs.append((f"{pname}:not_p_or_cnull:{i}", ("rows", ("or", ("not", ("cmp", "=", pcol, P(v))),
                                                                    ("isnull", c)))))
            qs.append((f"{pname}:in_with_null:{i}", ("rows", ("in", pcol, [P(v), P(None)]))))
            qs.append((f"{pname}:not_in:{i}", ("rows", ("not", ("in", pcol, [P(v)])))))
            qs.append((f"{pname}:agg_eq:{i}", ("agg", data[0], ("cmp", "=", pcol, P(v)))))
            for other in part:
                if other != pname:
                    qs.append((f"{pname}:p_and_other_null:{i}:{other}",
                               ("rows", ("and", ("cmp", "=", pcol, P(v)), ("isnull", ("col", (other,)))))))
        qs.append((f"{pname}:agg_pnull", ("agg", data[0], ("isnull", pcol))))
        if cty not in ("float32", "float64"):
            qs.append((f"{pname}:grouped_c_by_p", ("grouped_col", (pname,), ("notnull", c))))
    qs.append(("k:grouped_none", ("grouped", ("cmp", "<", ("col", ("k",)), ("lit", -1000, "int32")))))
    qs.append(("k:grouped_all", ("grouped", ("cmp", ">", ("col", ("k",)), ("lit", -1000, "int32")))))
    return qs, lt


def cast_templates(lt, rows, per_col=4):
    """O3 (research/queryproof/2026-11-03-approach-v2.md): predicates that make an engine unwrap or
    move a cast to prune, from the cast/mixed-type class of the bug study."""
    U = datetime.timezone.utc
    qs = []
    for path, ty in lt.items():
        if path == ("k",) or any(isinstance(p, tuple) for p in path):
            continue
        t = parse_type(ty)
        col, name = ("col", path), ".".join(path)
        vals = sorted({get_path(r, path) for r in rows} - {None}, key=repr)
        vals = [v for v in vals if not is_nan(v)][:per_col]
        L = lambda v, tt: ("lit", v, tt)  # noqa: E731
        Q = lambda tag, pred: qs.append((f"{name}:o3:{tag}", ("rows", pred)))  # noqa: E731
        if t[0] in INT_BITS:
            for i, v in enumerate(vals):
                Q(f"as_double_eq:{i}", ("cmp", "=", ("cast", col, "float64", False), L(float(v), "float64")))
                Q(f"as_string_eq:{i}", ("cmp", "=", ("cast", col, "string", False), L(str(v), "string")))
                if abs(v) < 2 ** 52:
                    Q(f"eq_half:{i}", ("cmp", "=", col, L(v + 0.5, "float64")))
                    Q(f"lt_half:{i}", ("cmp", "<", col, L(v + 0.5, "float64")))
                    Q(f"ge_half:{i}", ("cmp", ">=", col, L(v - 0.5, "float64")))
                    Q(f"in_mixed:{i}", ("in", col, [L(float(v), "float64"), L(v + 0.5, "float64")]))
                    qs.append((f"{name}:o3:agg_lt_half:{i}", ("agg", path, ("cmp", "<", col, L(v + 0.5, "float64")))))
            Q("as_bool", ("truth", ("cast", col, "bool", False)))
            Q("not_as_bool", ("not", ("cast", col, "bool", False)))
            if INT_BITS[t[0]] > 8:
                Q("try_narrow_null", ("isnull", ("cast", col, "int8", True)))
                Q("try_narrow_eq", ("cmp", "=", ("cast", col, "int8", True), L(1, "int8")))
        elif t[0] == "date":
            for i, v in enumerate(vals):
                mid = datetime.datetime(v.year, v.month, v.day, tzinfo=U)
                Q(f"eq_ts:{i}", ("cmp", "=", col, L(mid, "ts")))
                try:
                    Q(f"lt_ts_noon:{i}", ("cmp", "<", col, L(mid + datetime.timedelta(hours=12), "ts")))
                    Q(f"gt_ts_noon:{i}", ("cmp", ">", col, L(mid + datetime.timedelta(hours=12), "ts")))
                except OverflowError:
                    pass
                Q(f"as_ts_ge:{i}", ("cmp", ">=", ("cast", col, "ts", False), L(mid, "ts")))
        elif t[0] == "ts":
            for i, v in enumerate(vals):
                d0 = v.astimezone(U).date()
                Q(f"as_date_eq:{i}", ("cmp", "=", ("cast", col, "date", False), L(d0, "date")))
                Q(f"as_date_lt:{i}", ("cmp", "<", ("cast", col, "date", False), L(d0, "date")))
                Q(f"as_date_le:{i}", ("cmp", "<=", ("cast", col, "date", False), L(d0, "date")))
                Q(f"ge_date:{i}", ("cmp", ">=", col, L(d0, "date")))
                Q(f"lt_date:{i}", ("cmp", "<", col, L(d0, "date")))
                qs.append((f"{name}:o3:agg_as_date_eq:{i}",
                           ("agg", path, ("cmp", "=", ("cast", col, "date", False), L(d0, "date")))))
        elif t[0] == "decimal" and t[1] < 38:
            wider = f"decimal({t[1] + 1},{t[2] + 1})"
            half = Decimal(5).scaleb(-(t[2] + 1))
            for i, v in enumerate(vals):
                Q(f"eq_wider_scale:{i}", ("cmp", "=", col, L(v, wider)))
                Q(f"eq_half_ulp:{i}", ("cmp", "=", col, L(v + half, wider)))
                Q(f"lt_half_ulp:{i}", ("cmp", "<", col, L(v + half, wider)))
                Q(f"gt_half_ulp:{i}", ("cmp", ">", col, L(v - half, wider)))
        elif t[0] == "string":
            Q("try_int_null", ("isnull", ("cast", col, "int32", True)))
            for i, v in enumerate(vals):
                if STRICT_INT.fullmatch(v):
                    Q(f"try_int_eq:{i}", ("cmp", "=", ("cast", col, "int32", True), L(int(v), "int32")))
            Q("try_date_lt", ("cmp", "<", ("cast", col, "date", True), L(datetime.date(2000, 1, 1), "date")))
        elif t[0] in ("float32", "float64"):
            for i, v in enumerate(vals):
                if math.isfinite(v) and v == int(v) and abs(v) < 2 ** 31:
                    Q(f"eq_int:{i}", ("cmp", "=", col, L(int(v), "int32")))
                    Q(f"lt_int:{i}", ("cmp", "<", col, L(int(v), "int32")))
            Q("as_int_eq0", ("cmp", "=", ("cast", col, "int32", False), L(0, "int32")))  # rounding: on/off only
    return qs


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


# ---------------------------------------------------------------- pyarrow.dataset (phase two)
def pa_expr(e):
    """A templates.py predicate as a pyarrow compute expression with SQL WHERE semantics
    (a NULL result drops the row, as in SQL). Raises NoExpected for what does not translate."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from shapes import to_arrow
    k = e[0]
    if k == "col":
        if any(isinstance(p, tuple) for p in e[1]):
            raise NoExpected  # map lookups
        return pc.field(*e[1])
    if k == "lit":
        if e[1] is None:
            return pa.scalar(None, to_arrow(parse_type(e[2])))
        return pa.scalar(e[1], to_arrow(parse_type(e[2])))
    if k == "isnull":
        return pa_expr(e[1]).is_null()
    if k == "notnull":
        return pa_expr(e[1]).is_valid()
    if k == "not":
        return ~pa_expr(e[1])
    if k == "and":
        return pa_expr(e[1]) & pa_expr(e[2])
    if k == "or":
        return pa_expr(e[1]) | pa_expr(e[2])
    if k == "cmp":
        a, b = pa_expr(e[2]), pa_expr(e[3])
        return {"=": a == b, "<>": a != b, "<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[e[1]]
    if k == "nseq":
        a = pa_expr(e[1])
        if e[2][1] is None:
            return a.is_null()
        return pc.coalesce(a == pa_expr(e[2]), pa.scalar(False))
    if k == "between":
        return (pa_expr(e[1]) >= pa_expr(e[2])) & (pa_expr(e[1]) <= pa_expr(e[3]))
    if k == "in":
        vals = [x[1] for x in e[2]]
        if any(v is None for v in vals):
            raise NoExpected  # SQL IN with NULL has no direct isin() form
        out = None
        for x in e[2]:
            t = pa_expr(e[1]) == pa_expr(x)
            out = t if out is None else out | t
        return out
    if k == "coalesce":
        return pc.coalesce(pa_expr(e[1]), pa_expr(e[2])) == pa_expr(e[2])
    raise NoExpected


def pa_query(q):
    """(kind, filter expression or None, leaf path) for the queries pyarrow.dataset can answer."""
    if q[0] == "rows":
        return ("rows", pa_expr(q[1]), None)
    if q[0] == "agg":
        return ("agg", pa_expr(q[2]) if q[2] is not None else None, q[1])
    raise NoExpected
