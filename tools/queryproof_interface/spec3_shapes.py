"""Third spec-driven shape set (pair three): contract items of C1-C4 that pairs one and two left out.
Each shape names the item it comes from. Written from research/queryproof/2026-11-01-storage-interface-contracts.md
only, before any pair-three run. Enabled with QP_IFACE_GEN=spec3 (see shapes.py).

New in the harness for this set (shared with the random baseline where it is a type):
  uint8..uint64 (Parquet UINT_N; Parquet-only, not written by Spark), a 0-row row group before the
  rows of a file, a shape with no data file, and the C2 GROUP BY and scalar-subquery templates.
"""
from decimal import Decimal

from shapes import shape

U8, U16, U32, U64 = 2 ** 8, 2 ** 16, 2 ** 32, 2 ** 64
NEST2 = "struct<i:struct<c:int32>,z:int32>"
SHAPES3 = [
    # ---- C3 UINT_* order: TYPE_DEFINED_ORDER compares unsigned. Values on both sides of the signed
    # maximum, in files whose min/max straddle it, plus one value per file.
    shape("O11_uint8_signed_boundary", "C3", [("k", "int32"), ("u", "uint8")],
          [(1, 0), (2, 1), (3, 127), (4, 128), (5, 200), (6, U8 - 1), (7, None)],
          files=[[0, 1], [2, 3], [4, 5], [0, 5, 6], [3], [5]]),
    shape("O11b_uint16_signed_boundary", "C3", [("k", "int32"), ("u", "uint16")],
          [(1, 0), (2, 2 ** 15 - 1), (3, 2 ** 15), (4, U16 - 1), (5, None)],
          files=[[0, 1], [2, 3], [0, 3], [1, 2, 4], [2]]),
    shape("O11c_uint32_signed_boundary", "C3", [("k", "int32"), ("u", "uint32")],
          [(1, 0), (2, 2 ** 31 - 1), (3, 2 ** 31), (4, U32 - 1), (5, None), (6, 1)],
          files=[[0, 1], [2, 3], [0, 3], [1, 2, 4], [2], [5, 3]]),
    shape("O11d_uint64_signed_boundary", "C3", [("k", "int32"), ("u", "uint64")],
          [(1, 0), (2, 2 ** 63 - 1), (3, 2 ** 63), (4, U64 - 1), (5, None), (6, 1)],
          files=[[0, 1], [2, 3], [0, 3], [1, 2, 4], [2], [5, 3]]),
    shape("O11e_uint_all_widths_one_per_file", "C3",
          [("k", "int32"), ("a", "uint8"), ("b", "uint16"), ("c", "uint32"), ("d", "uint64")],
          [(1, 255, 65535, U32 - 1, U64 - 1), (2, 0, 0, 0, 0), (3, 128, 32768, 2 ** 31, 2 ** 63),
           (4, None, None, None, None)], files=[[0], [1], [2], [3]]),
    # ---- C4 D2 for unsigned: UINT_32 is INT32 in the file and BIGINT in the Spark/Trino schema, so a
    # bloom filter or dictionary must be probed with the file's physical value
    shape("D9_uint32_bloom_widened_read", "C4", [("k", "int32"), ("u", "uint32")],
          [(1, U32 - 1), (2, 2 ** 31), (3, 5), (4, None)], files=[[0], [1], [2, 3]]),
    shape("D9b_uint8_bloom_dictionary", "C4", [("k", "int32"), ("u", "uint8"), ("s", "string")],
          [(i, (i * 37) % 256, "v%d" % (i % 3)) for i in range(40)], files=[list(range(40))]),
    # ---- C2 E5 for unsigned and nested: an all-NULL unsigned column has counts but no min/max
    shape("E5c_uint_all_null", "C2", [("k", "int32"), ("u", "uint32"), ("w", "uint64")],
          [(1, None, None), (2, None, None), (3, 7, U64 - 1)], files=[[0, 1], [2]]),
    # ---- C2 E3: a directory with no data file at all
    shape("E3_no_data_file", "C2", [("k", "int32"), ("v", "int64"), ("s", "string"), ("o", NEST2)], [],
          files=[]),
    # ---- C2 E2: a 0-row row group next to a non-empty one (pyarrow Parquet writers)
    shape("E2_zero_row_group_first", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")],
          [(1, 5, "a"), (2, None, "b"), (3, -1, None)], files=[[0, 1], [2]], empty_row_group_first=True),
    shape("E2b_zero_row_group_all_null", "C2", [("k", "int32"), ("v", "int64"), ("d", "decimal(9,2)")],
          [(1, None, None), (2, None, None)], files=[[0, 1]], empty_row_group_first=True),
    shape("E2c_zero_row_group_nested", "C2", [("k", "int32"), ("o", NEST2), ("l", "list<int32>")],
          [(1, {"i": None, "z": 1}, [1]), (2, None, None), (3, {"i": {"c": 3}, "z": None}, [])],
          files=[[0, 1, 2]], empty_row_group_first=True),
    # ---- C2 the GROUP BY / scalar-subquery templates over empty and all-NULL input (the new templates
    # apply to every shape; these shapes make their empty cases matter)
    shape("E4b_every_file_empty_or_all_null", "C2", [("k", "int32"), ("v", "int64"), ("s", "string")],
          [(1, None, None)], files=[[], [0], []]),
    shape("E6c_many_row_groups_one_value", "C2", [("k", "int32"), ("v", "int64")],
          [(i, 42 if i == 3 else None) for i in range(8)], files=[list(range(8))]),
    # ---- C1 for maps: statistics range over entries, not rows (map values NULL vs missing keys)
    shape("N12_map_values_over_entries", "C1", [("k", "int32"), ("m", "map<string,int32>")],
          [(1, [("a", None), ("b", 1)]), (2, [("b", 2)]), (3, None), (4, [("a", 5)]), (5, [])],
          files=[[0, 1], [2, 3, 4], [1], [3]]),
    # ---- C3 O4: decimals of maximal precision next to their negatives, in INT64-sized and FLBA forms
    shape("O4b_decimal18_signed_order", "C3", [("k", "int32"), ("d", "decimal(18,2)")],
          [(1, Decimal("-9999999999999999.99")), (2, Decimal("-0.01")), (3, Decimal("0.00")),
           (4, Decimal("9999999999999999.99")), (5, None)], files=[[0, 1], [2, 3], [0, 3], [4]]),
]
