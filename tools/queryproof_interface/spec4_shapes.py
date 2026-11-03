"""Fourth spec-driven shape set (pair four): partitioned layouts, from the C2 addendum (E4, P1-P10)
of research/queryproof/2026-11-01-storage-interface-contracts.md, written before any pair-four run.
Enabled with QP_IFACE_GEN=spec4 (see shapes.py).

Partition columns go last; the pyarrow writers name directories with pyarrow's hive partitioning
(or `dir_names`), Spark's writers with partitionBy. A 0-row file goes to the all-NULL directory
unless `dir_names` names one.
"""
import datetime
from decimal import Decimal

from shapes import shape

D = datetime.date
UTC = datetime.timezone.utc
NEST2 = "struct<i:struct<c:int32>,z:int32>"
SHAPES4 = [
    # ---- P1: INT partition, string order differs from numeric order, NULL partition, 0-row file
    shape("P1_int_partition_order_null", "C2", [("k", "int32"), ("v", "int64"), ("p", "int32")],
          [(1, 5, -1), (2, None, 0), (3, 7, 9), (4, 8, 10), (5, None, None), (6, 1, 10)],
          files=[[0, 1], [2, 3], [4], [5], []], partition=["p"]),
    # ---- P2: STRING values that need escaping; the empty string on its own (P2b)
    shape("P2_string_partition_escaping", "C2", [("k", "int32"), ("v", "int32"), ("s", "string")],
          [(1, 1, "a/b"), (2, 2, "a=b"), (3, None, "a b"), (4, 4, "50%"), (5, 5, "%2F"), (6, 6, "é"),
           (7, 7, "null"), (8, None, None), (9, 9, "Z")],
          files=[[0, 1, 2], [3, 4], [5, 6, 7], [8]], partition=["s"]),
    shape("P2b_string_partition_empty", "C2", [("k", "int32"), ("v", "int32"), ("s", "string")],
          [(1, 1, ""), (2, 2, "x"), (3, 3, None)], files=[[0], [1], [2]], partition=["s"]),
    # ---- P3: non-canonical directory names (pyarrow writers only; Spark writes canonical ones)
    shape("P3_int_noncanonical_dirs", "C2", [("k", "int32"), ("v", "int32"), ("p", "int32")],
          [(1, 1, 1), (2, 2, 1), (3, 3, 0), (4, None, 2)], files=[[0], [1], [2], [3]], partition=["p"],
          dir_names={0: "p=01", 1: "p=1", 2: "p=-0", 3: "p=0002"}),
    shape("P3b_decimal_noncanonical_dirs", "C2", [("k", "int32"), ("v", "int32"), ("d", "decimal(5,2)")],
          [(1, 1, Decimal("1.50")), (2, 2, Decimal("1.50")), (3, 3, Decimal("-0.50")), (4, 4, None)],
          files=[[0], [1], [2], [3]], partition=["d"], dir_names={0: "d=1.5", 1: "d=1.50", 2: "d=-.5"}),
    # ---- P4: DATE partitions in the gap, before 1970, at the limits
    shape("P4_date_partition_gap_limits", "C2", [("k", "int32"), ("v", "int32"), ("dt", "date")],
          [(1, 1, D(1582, 10, 4)), (2, 2, D(1582, 10, 10)), (3, 3, D(1582, 10, 15)), (4, 4, D(1969, 12, 31)),
           (5, 5, D(1, 1, 1)), (6, 6, D(9999, 12, 31)), (7, None, None)],
          files=[[0, 1], [2], [3, 4], [5, 6]], partition=["dt"]),
    # ---- P5: two levels with NULL at each level
    shape("P5_two_levels_nulls", "C2", [("k", "int32"), ("v", "int32"), ("p", "int32"), ("q", "string")],
          [(1, 1, 1, "a"), (2, None, 1, None), (3, 3, None, "a"), (4, 4, None, None), (5, 5, 2, "b"),
           (6, None, 2, "a")], files=[[0, 1], [2, 3], [4, 5]], partition=["p", "q"]),
    # ---- P6: BIGINT and DECIMAL at their limits and around zero
    shape("P6_bigint_decimal_limits", "C2", [("k", "int32"), ("v", "int32"), ("b", "int64"), ("m", "decimal(9,2)")],
          [(1, 1, 2 ** 63 - 1, Decimal("9999999.99")), (2, 2, -2 ** 63, Decimal("-9999999.99")),
           (3, 3, 0, Decimal("-0.50")), (4, None, -1, Decimal("0.00")), (5, 5, None, None)],
          files=[[0, 1], [2, 3], [4]], partition=["b", "m"]),
    # ---- P7: TIMESTAMP partitions with microseconds, before and after 1970
    shape("P7_ts_partition", "C2", [("k", "int32"), ("v", "int32"), ("t", "ts")],
          [(1, 1, datetime.datetime(1970, 1, 1, tzinfo=UTC)),
           (2, 2, datetime.datetime(2020, 1, 2, 3, 4, 5, 6, tzinfo=UTC)),
           (3, 3, datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)), (4, None, None)],
          files=[[0, 1], [2, 3]], partition=["t"]),
    # ---- P8: a partition directory that holds only an empty file
    shape("P8_partition_dir_only_empty_file", "C2", [("k", "int32"), ("v", "int32"), ("p", "int32")],
          [(1, 1, 1), (2, None, 1), (3, 3, 2)], files=[[0, 1], [2], [], []], partition=["p"],
          dir_names={2: "p=7", 3: "p=__HIVE_DEFAULT_PARTITION__"}),
    # ---- P9: nested data with NULL parents under a partition column
    shape("P9_nested_under_partition", "C1", [("k", "int32"), ("o", NEST2), ("p", "int32")],
          [(1, {"i": None, "z": 1}, 1), (2, None, 1), (3, {"i": {"c": 3}, "z": None}, 2), (4, None, None)],
          files=[[0, 1], [2, 3]], partition=["p"]),
    # ---- P10: a partition column that is NULL in every row
    shape("P10_partition_all_null", "C2", [("k", "int32"), ("v", "int64"), ("p", "int32")],
          [(1, 1, None), (2, None, None), (3, 3, None)], files=[[0, 1], [2]], partition=["p"]),
]
