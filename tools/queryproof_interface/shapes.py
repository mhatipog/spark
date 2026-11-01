"""Adversarial data shapes, one list per concept of research/queryproof/2026-11-01-storage-interface-contracts.md.

A shape is the rows of one table, its column types in a small neutral type language and how
the rows split into files (`files`: list of row-index lists; an empty list is a 0-row file).
Writers then split each file further into row groups, pages and stripes (writers.py).
`read_as` optionally declares a wider read schema (type widening, contract D2).

Type language: int8 int16 int32 int64 float32 float64 string date ts decimal(p,s)
               (ts: microsecond timestamps adjusted to UTC)
               struct<name:type,...> list<type> map<string,type>
"""
import datetime
import math
import os
import re
from decimal import Decimal


def parse_type(ty):
    """Return a tuple tree: ('int32',), ('decimal', p, s), ('struct', [(name, t)]), ('list', t), ('map', t)."""
    ty = ty.strip()
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", ty)
    if m:
        return ("decimal", int(m.group(1)), int(m.group(2)))
    for kind in ("struct", "list", "map"):
        if ty.startswith(kind + "<") and ty.endswith(">"):
            inner = _split_top(ty[len(kind) + 1:-1])
            if kind == "struct":
                return ("struct", [(f.split(":", 1)[0], parse_type(f.split(":", 1)[1])) for f in inner])
            if kind == "list":
                return ("list", parse_type(inner[0]))
            assert parse_type(inner[0]) == ("string",)
            return ("map", parse_type(inner[1]))
    assert ty in ("int8", "int16", "int32", "int64", "float32", "float64", "string", "date", "ts"), ty
    return (ty,)


def _split_top(s):
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
            continue
        depth += ch == "<"
        depth -= ch == ">"
        cur += ch
    return out + [cur]


def to_arrow(t):
    import pyarrow as pa
    k = t[0]
    if k == "decimal":
        return pa.decimal128(t[1], t[2])
    if k == "struct":
        return pa.struct([(n, to_arrow(c)) for n, c in t[1]])
    if k == "list":
        return pa.list_(to_arrow(t[1]))
    if k == "map":
        return pa.map_(pa.string(), to_arrow(t[1]))
    return {"int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(),
            "float32": pa.float32(), "float64": pa.float64(), "string": pa.string(), "date": pa.date32(),
            "ts": pa.timestamp("us", tz="UTC")}[k]


SMALL_INTS = {"int8": "TINYINT", "int16": "SMALLINT"}
SQL_ATOMIC = {
    "spark": {"int32": "INT", "int64": "BIGINT", "float32": "FLOAT", "float64": "DOUBLE", "string": "STRING", "date": "DATE"},
    "duckdb": {"int32": "INTEGER", "int64": "BIGINT", "float32": "REAL", "float64": "DOUBLE", "string": "VARCHAR", "date": "DATE"},
    "datafusion": {"int32": "INT", "int64": "BIGINT", "float32": "REAL", "float64": "DOUBLE", "string": "VARCHAR", "date": "DATE"},
    "trino": {"int32": "INTEGER", "int64": "BIGINT", "float32": "REAL", "float64": "DOUBLE", "string": "VARCHAR", "date": "DATE"},
}


def sql_type(t, dialect):
    k = t[0]
    if k == "decimal":
        return f"DECIMAL({t[1]},{t[2]})"
    if dialect == "spark" and k == "struct":
        return "STRUCT<" + ", ".join(f"{n}: {sql_type(c, dialect)}" for n, c in t[1]) + ">"
    if dialect == "spark" and k == "list":
        return f"ARRAY<{sql_type(t[1], dialect)}>"
    if dialect == "spark" and k == "map":
        return f"MAP<STRING, {sql_type(t[1], dialect)}>"
    if dialect == "trino" and k == "struct":
        return "ROW(" + ", ".join(f"{n} {sql_type(c, dialect)}" for n, c in t[1]) + ")"
    if dialect == "trino" and k == "list":
        return f"ARRAY({sql_type(t[1], dialect)})"
    if dialect == "trino" and k == "map":
        return f"MAP(VARCHAR, {sql_type(t[1], dialect)})"
    if k in SMALL_INTS:
        return SMALL_INTS[k]
    if k == "ts":
        return {"spark": "TIMESTAMP", "duckdb": "TIMESTAMPTZ", "datafusion": "TIMESTAMP",
                "trino": "TIMESTAMP(6)"}[dialect]
    assert k in SQL_ATOMIC[dialect], (dialect, t)
    return SQL_ATOMIC[dialect][k]


def spark_ddl(columns):
    return ", ".join(f"`{n}` {sql_type(parse_type(ty), 'spark')}" for n, ty in columns)


D = datetime.date
NAN, INF = float("nan"), float("inf")


def shape(sid, concept, columns, rows, files=None, read_as=None, note=""):
    files = files if files is not None else [list(range(len(rows)))]
    return {"id": sid, "concept": concept, "columns": columns, "rows": rows, "files": files,
            "read_as": read_as, "note": note}


ST = "struct<x:int32,y:string>"
SHAPES = [
    # ---- C1 NULL semantics in statistics
    shape("N1_struct_null_whole_file", "C1", [("k", "int32"), ("st", ST)],
          [(1, None), (2, None), (3, {"x": 1, "y": "a"})], files=[[0, 1], [2]],
          note="one file where the struct is NULL in every row: the child has 0 values"),
    shape("N2_struct_null_child_never_null", "C1", [("k", "int32"), ("st", ST)],
          [(1, {"x": 1, "y": "b"}), (2, None), (3, {"x": 2, "y": "c"}), (4, None)],
          note="the N4 shape"),
    shape("N3_child_null_vs_parent_null", "C1", [("k", "int32"), ("st", ST)],
          [(1, {"x": None, "y": "a"}), (2, None), (3, {"x": 3, "y": None}), (4, {"x": 4, "y": "d"})]),
    shape("N4_list_null_empty_nullelem", "C1", [("k", "int32"), ("arr", "list<int32>")],
          [(1, None), (2, []), (3, [None]), (4, [1, None, 2]), (5, [3])]),
    shape("N5_map_null_value_missing_key", "C1", [("k", "int32"), ("m", "map<string,int32>")],
          [(1, None), (2, []), (3, [("a", None)]), (4, [("a", 1), ("b", 2)]), (5, [("b", None)])]),
    shape("N6_two_levels", "C1", [("k", "int32"), ("o", "struct<i:struct<c:int32>,z:int32>")],
          [(1, None), (2, {"i": None, "z": 1}), (3, {"i": {"c": None}, "z": 2}), (4, {"i": {"c": 7}, "z": None})],
          files=[[0, 1], [2, 3]]),
    shape("N6b_two_levels_per_file", "C1", [("k", "int32"), ("o", "struct<i:struct<c:int32>,z:int32>")],
          [(1, None), (2, {"i": None, "z": 1}), (3, {"i": {"c": None}, "z": 2}), (4, {"i": {"c": 7}, "z": None})],
          files=[[0], [1], [2], [3]]),
    shape("N8_struct_of_list", "C1", [("k", "int32"), ("sl", "struct<l:list<int32>>")],
          [(1, None), (2, {"l": None}), (3, {"l": []}), (4, {"l": [5]})], files=[[0], [1, 2, 3]]),
    # ---- C2 empty and fully pruned input
    shape("E1_zero_row_file", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")], [], files=[[]]),
    shape("E1b_zero_row_plus_rows", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")],
          [(1, 10, "a"), (2, None, None)], files=[[], [0, 1]]),
    shape("E5_all_null_one_row", "C2", [("k", "int32"), ("v", "int64"), ("s", "string"), ("d", "decimal(9,2)")],
          [(1, None, None, None)]),
    shape("E5b_all_null_many", "C2", [("k", "int32"), ("v", "int64"), ("s", "string"), ("d", "decimal(9,2)")],
          [(i, None, None, None) for i in range(5)], files=[[0, 1], [2, 3, 4]]),
    shape("E6_one_file_one_row", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")], [(1, 5, "x")]),
    shape("E6b_mixed_null_groups", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")],
          [(1, None, None), (2, 3, "b"), (3, None, "c"), (4, -1, None)], files=[[0], [1, 2, 3]]),
    # ---- C3 min/max ordering
    shape("O1_utf8_vs_utf16", "C3", [("k", "int32"), ("s", "string")],
          [(1, "�"), (2, "\U0001f600"), (3, ""), (4, "z"), (5, "￿"), (6, "\U00010000")],
          files=[[0, 2], [1, 5], [3, 4]]),
    shape("O1b_utf8_one_per_file", "C3", [("k", "int32"), ("s", "string")],
          [(1, "�"), (2, "\U0001f600"), (3, ""), (4, "￿")], files=[[0], [1], [2], [3]]),
    shape("O2_long_prefix", "C3", [("k", "int32"), ("s", "string")],
          [(1, "p" * 80 + "a"), (2, "p" * 80 + "b"), (3, "p" * 80), (4, "\U0010ffff" * 20), (5, "p" * 81)],
          files=[[0, 1], [2, 3, 4]]),
    shape("O3_nan_signed_zero_f64", "C3", [("k", "int32"), ("f", "float64")],
          [(1, NAN), (2, -0.0), (3, 0.0), (4, 1.5), (5, -INF), (6, INF), (7, None)],
          files=[[0], [1], [2], [0, 3], [4, 5, 6]]),
    shape("O3b_nan_signed_zero_f32", "C3", [("k", "int32"), ("f", "float32")],
          [(1, NAN), (2, -0.0), (3, 0.0), (4, 1.5), (5, -INF)], files=[[0], [1], [2], [3, 4]]),
    shape("O3c_zero_only_files", "C3", [("k", "int32"), ("f", "float64")],
          [(1, -0.0), (2, -0.0), (3, 0.0), (4, 0.0)], files=[[0, 1], [2, 3]]),
    shape("O4_negative_decimals", "C3", [("k", "int32"), ("d", "decimal(5,2)"), ("dl", "decimal(18,3)"), ("dx", "decimal(38,0)")],
          [(1, Decimal("-1.00"), Decimal("-0.001"), Decimal("-" + "9" * 38)),
           (2, Decimal("0.00"), Decimal("0.000"), Decimal("0")),
           (3, Decimal("999.99"), Decimal("999999999999999.999"), Decimal("9" * 38)),
           (4, Decimal("-999.99"), Decimal("-999999999999999.999"), Decimal("-1"))],
          files=[[0, 1], [2, 3]]),
    shape("O5_old_dates", "C3", [("k", "int32"), ("d", "date")],
          [(1, D(1000, 1, 1)), (2, D(1582, 10, 4)), (3, D(1582, 10, 15)), (4, D(1969, 12, 31)), (5, D(1970, 1, 1)),
           (6, D(9999, 12, 31))], files=[[0, 1], [2, 3], [4, 5]]),
    shape("O6_case_variants", "C3", [("k", "int32"), ("s", "string")],
          [(1, "ABC"), (2, "abc"), (3, "Abd"), (4, "Å"), (5, "å"), (6, "Å")], files=[[0, 1], [2], [3, 4, 5]]),
    # ---- C4 dictionary and bloom-filter skipping
    shape("D1_dictionary_fallback", "C4", [("k", "int32"), ("s", "string"), ("v", "int64")],
          [(i, "val%03d" % i, i * 7) for i in range(40)], files=[list(range(40))],
          note="writers use a tiny dictionary page limit so the chunk falls back to plain"),
    shape("D2_widen_int_to_long", "C4", [("k", "int32"), ("v", "int32")],
          [(1, 1), (2, -1), (3, 2147483647), (4, -2147483648), (5, None)], files=[[0, 1], [2, 3, 4]],
          read_as=[("k", "int32"), ("v", "int64")]),
    shape("D2b_widen_float_to_double", "C4", [("k", "int32"), ("f", "float32")],
          [(1, 0.1), (2, 1.5), (3, -0.0), (4, None)], files=[[0, 1], [2, 3]],
          read_as=[("k", "int32"), ("f", "float64")]),
    shape("D2c_widen_decimal", "C4", [("k", "int32"), ("d", "decimal(5,2)")],
          [(1, Decimal("1.10")), (2, Decimal("-3.25")), (3, None)], files=[[0, 1], [2]],
          read_as=[("k", "int32"), ("d", "decimal(10,2)")]),
    shape("D2d_widen_int_to_double", "C4", [("k", "int32"), ("v", "int32")],
          [(1, 1), (2, 16777217), (3, None)], files=[[0], [1, 2]], read_as=[("k", "int32"), ("v", "float64")]),
    shape("D2e_widen_date_to_ts_absent", "C4", [("k", "int32"), ("v", "int64")],
          [(1, 1), (2, None)], files=[[0], [1]], read_as=[("k", "int32"), ("v", "int64"), ("extra", "int32")],
          note="a read schema column absent from the files (schema evolution)"),
    shape("D3_bloom_signed_zero_nan", "C4", [("k", "int32"), ("f", "float64")],
          [(1, -0.0), (2, NAN), (3, 2.0)], files=[[0], [1], [2]]),
    shape("D4_bloom_case", "C4", [("k", "int32"), ("s", "string")],
          [(1, "Key"), (2, "OTHER"), (3, "å")], files=[[0], [1], [2]]),
    shape("D5_low_cardinality_many_rows", "C4", [("k", "int32"), ("s", "string"), ("v", "int32")],
          [(i, ["a", "b", None][i % 3], i % 2) for i in range(30)], files=[list(range(15)), list(range(15, 30))]),
]


def check_shapes():
    ids = [s["id"] for s in SHAPES]
    assert len(ids) == len(set(ids)), "duplicate shape id"
    for s in SHAPES:
        for c in s["columns"] + (s["read_as"] or []):
            parse_type(c[1])
        for f in s["files"]:
            assert all(0 <= i < len(s["rows"]) for i in f), s["id"]


def is_nan(v):
    return isinstance(v, float) and math.isnan(v)


# QP_IFACE_GEN=random:SEED replaces the contract shapes with the matched random baseline,
# QP_IFACE_GEN=spec2 with the second contract shape set.
GEN = os.environ.get("QP_IFACE_GEN", "spec")
if GEN.startswith("random:"):
    import random_gen
    SHAPES = random_gen.make_shapes(int(GEN.split(":", 1)[1]))
elif GEN == "spec2":  # the second contract shape set (pair two)
    import spec2_shapes
    SHAPES = spec2_shapes.SHAPES2
check_shapes()
