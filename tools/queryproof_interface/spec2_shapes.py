"""Second spec-driven shape set (pair two): contract items of C1-C4 that the first set
(shapes.SHAPES) did not cover or covered too coarsely. Each shape names the item it comes from.
Written from research/queryproof/2026-11-01-storage-interface-contracts.md only.
Enabled with QP_IFACE_GEN=spec2 (see shapes.py).
"""
import datetime
from decimal import Decimal

from shapes import shape

UTC = datetime.timezone.utc


def ts(*a):
    return datetime.datetime(*a, tzinfo=UTC)


NEST2 = "struct<i:struct<c:int32>,z:int32>"
LS = "list<struct<x:int32>>"
SL = "struct<l:list<int32>,s:string>"
SHAPES2 = [
    # ---- C1 NULL at every nesting level, levels combined within one file (N3, N6)
    shape("N9_levels_mixed_per_file", "C1", [("k", "int32"), ("o", NEST2)],
          [(1, {"i": {"c": 5}, "z": 1}), (2, {"i": None, "z": 2}), (3, {"i": {"c": None}, "z": 3}), (4, None),
           (5, {"i": {"c": 6}, "z": None})],
          files=[[0, 1, 2, 3, 4], [0, 1, 2, 4], [0, 1], [1, 4], [2, 3]],
          note="every combination of present/NULL outer and inner struct in one file"),
    shape("N9b_inner_null_outer_never_null", "C1", [("k", "int32"), ("o", NEST2), ("v", "int32")],
          [(1, {"i": {"c": 10}, "z": 1}, 1), (2, {"i": None, "z": 2}, 2), (3, {"i": {"c": 30}, "z": 3}, 3),
           (4, {"i": {"c": 40}, "z": 4}, 4)], files=[[0, 1, 2, 3], [1, 2]]),
    shape("N10_list_of_struct", "C1", [("k", "int32"), ("ls", LS)],
          [(1, None), (2, []), (3, [None]), (4, [{"x": None}]), (5, [{"x": 1}, None, {"x": 2}])],
          files=[[0, 1, 2, 3, 4], [2, 3]]),
    shape("N11_struct_of_list_and_string", "C1", [("k", "int32"), ("sl", SL)],
          [(1, None), (2, {"l": None, "s": None}), (3, {"l": [], "s": ""}), (4, {"l": [None], "s": "a"}),
           (5, {"l": [1, 2], "s": None})], files=[[0, 1, 2, 3, 4], [1, 2], [3, 4]]),
    shape("N13_child_all_null_parent_never_null", "C1", [("k", "int32"), ("st", "struct<x:int32,y:string>")],
          [(1, {"x": None, "y": "a"}), (2, {"x": None, "y": "b"}), (3, {"x": None, "y": None})],
          files=[[0, 1, 2], [0], [2]]),
    # ---- C1/C2 statistics absent (N7, E7): the pa_stats_k_only writer covers every shape; these add
    # all-NULL and mixed chunks so that a missing null_count matters
    shape("E7_all_null_and_mixed", "C2", [("k", "int32"), ("v", "int64"), ("s", "string"), ("d", "decimal(9,2)")],
          [(1, None, None, None), (2, None, None, None), (3, 7, "x", Decimal("1.50")), (4, None, "y", None)],
          files=[[0, 1], [2, 3], [0, 3]]),
    # ---- C2 empty or pruned input (E1, E3, E5) with nested and new types
    shape("E8_only_empty_files_nested", "C2", [("k", "int32"), ("o", NEST2), ("t", "ts")], [], files=[[], []]),
    shape("E9_nested_all_null_file", "C2", [("k", "int32"), ("o", NEST2), ("t", "ts")],
          [(1, None, None), (2, None, None), (3, {"i": {"c": 1}, "z": 1}, ts(2020, 1, 1))], files=[[0, 1], [2]]),
    shape("E10_min_max_partly_missing", "C2", [("k", "int32"), ("v", "int16"), ("t", "ts")],
          [(1, None, None), (2, 5, ts(2021, 6, 1)), (3, None, None), (4, -7, ts(1999, 12, 31, 23, 59, 59, 999999))],
          files=[[0], [1], [2], [3]]),
    # ---- C3 ordering of new types (O5 timestamps; O4 decimals; integer boundaries)
    shape("O7_timestamps", "C3", [("k", "int32"), ("t", "ts")],
          [(1, ts(1, 1, 1)), (2, ts(1582, 10, 10, 12)), (3, ts(1582, 10, 15)), (4, ts(1899, 12, 31, 23, 59, 59, 999999)),
           (5, ts(1969, 12, 31, 23, 59, 59, 999999)), (6, ts(1970, 1, 1)), (7, ts(1970, 1, 1, 0, 0, 0, 1)),
           (8, ts(9999, 12, 31, 23, 59, 59, 999999)), (9, None)],
          files=[[0, 1], [2, 3], [4, 5, 6], [7, 8], [1], [5]]),
    shape("O8_long_strings", "C3", [("k", "int32"), ("s", "string")],
          [(1, "a" * 1100), (2, "a" * 1100 + "b"), (3, "a" * 64), (4, "a" * 65), (5, "a" * 4), (6, "a" * 5),
           (7, "b"), (8, "\xff" * 70)], files=[[0, 1], [2, 3], [4, 5], [6, 7], [0], [3]]),
    shape("O9_wide_decimals", "C3", [("k", "int32"), ("d", "decimal(38,10)"), ("e", "decimal(18,0)")],
          [(1, Decimal("-" + "9" * 28 + "." + "9" * 10), Decimal("-" + "9" * 18)),
           (2, Decimal("0E-10"), Decimal("0")), (3, Decimal("1E-10"), Decimal("1")),
           (4, Decimal("9" * 28 + "." + "9" * 10), Decimal("9" * 18)), (5, Decimal("-1E-10"), Decimal("-1"))],
          files=[[0, 1], [2, 3], [4], [1, 4]]),
    shape("O10_small_int_bounds", "C3", [("k", "int32"), ("b", "int8"), ("h", "int16")],
          [(1, -128, -32768), (2, 127, 32767), (3, 0, 0), (4, -1, -1), (5, None, None)],
          files=[[0, 1], [2, 3], [4], [0], [1]]),
    # ---- C4 bloom and dictionary under widened reads (D2) for the new types
    shape("D6_widen_int8_to_int32", "C4", [("k", "int32"), ("b", "int8")],
          [(1, -128), (2, 127), (3, -1), (4, None)], files=[[0, 1], [2, 3]], read_as=[("k", "int32"), ("b", "int32")]),
    shape("D6b_widen_int16_to_int64", "C4", [("k", "int32"), ("h", "int16")],
          [(1, -32768), (2, 32767), (3, -1), (4, None)], files=[[0, 1], [2, 3]], read_as=[("k", "int32"), ("h", "int64")]),
    shape("D6c_widen_int8_to_decimal", "C4", [("k", "int32"), ("b", "int8")],
          [(1, -128), (2, 5), (3, None)], files=[[0], [1, 2]], read_as=[("k", "int32"), ("b", "decimal(10,0)")]),
    shape("D7_dictionary_many_values", "C4", [("k", "int32"), ("s", "string"), ("t", "ts")],
          [(i, "v%02d" % (i % 25), ts(2020, 1, 1 + i % 25)) for i in range(60)], files=[list(range(60))]),
    shape("D8_bloom_decimal_and_ts", "C4", [("k", "int32"), ("d", "decimal(9,2)"), ("t", "ts")],
          [(1, Decimal("1.00"), ts(2020, 1, 1)), (2, Decimal("-1.00"), ts(2020, 1, 1, 0, 0, 0, 1)),
           (3, Decimal("123.45"), None)], files=[[0], [1], [2]]),
]
