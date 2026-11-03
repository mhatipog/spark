"""Fifth shape set (phase two, round C, oracle O4): transplanted triggers of recent wrong-result bugs
from other engines (catalogue: research/queryproof/2026-11-03-approach-v2.md). Each shape names the
bug it transplants; the writers and settings that complete the trigger are in extra_writers.py
(ROUND_C, LAYOUT_WRITERS). Enabled with QP_IFACE_GEN=spec5.
"""
import datetime
from decimal import Decimal

from shapes import shape

UTC = datetime.timezone.utc
T = lambda *a: datetime.datetime(*a, tzinfo=UTC)  # noqa: E731
SHAPES5 = [
    # duckdb#27455 (backslash read as a path separator), datafusion#26492 ('=' unescaped),
    # polars#30044 (NULL vs the literal "__HIVE_DEFAULT_PARTITION__"): partition strings that writers
    # escape differently
    shape("T1_partition_special_strings", "O4", [("k", "int32"), ("v", "int32"), ("s", "string")],
          [(1, 1, "a\\b"), (2, 2, "a%b"), (3, 3, "#x"), (4, 4, "q?r"), (5, 5, "a+b"), (6, 6, "t~"),
           (7, 7, " lead"), (8, 8, "trail "), (9, 9, "."), (10, 10, "a:b"), (11, None, None), (12, 12, "x=y")],
          files=[[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]], partition=["s"]),
    # duckdb#27863 (INT96 statistics trusted): timestamps across row groups, written as INT96 with
    # legacy statistics by pa_int96_legacy_stats
    shape("T2_int96_timestamps", "O4", [("k", "int32"), ("t", "ts")],
          [(1, T(1999, 12, 31, 23, 59, 59)), (2, T(2020, 6, 1, 12, 0)), (3, T(1970, 1, 1)), (4, None),
           (5, T(2038, 1, 19, 3, 14, 8)), (6, T(1900, 1, 1))], files=[[0, 1, 2], [3, 4, 5]]),
    # iceberg-rust#3264 (page-index pruning of INT32/INT64-encoded decimals): decimals spread over
    # pages, negatives included (pa_decint_page_index)
    shape("T3_decimal_int_encoded_pages", "O4", [("k", "int32"), ("d", "decimal(9,2)"), ("e", "decimal(18,4)")],
          [(i, Decimal(v).scaleb(-2), Decimal(v * 997).scaleb(-4)) for i, v in
           enumerate([-99999, -100, -1, 0, 1, 99, 100, 12345, 99999])] + [(9, None, None)], files=[list(range(10))]),
    # iceberg#18173 (TIMESTAMP_MILLIS statistics pruning drops rows): millisecond-exact values, so
    # pa_ts_millis stores them exactly; the C3 neighbours add literals 1 microsecond either side
    shape("T4_ts_millis_exact", "O4", [("k", "int32"), ("t", "ts")],
          [(1, T(2020, 1, 1, 0, 0, 0, 1000)), (2, T(2020, 1, 1, 0, 0, 0, 999000)), (3, T(1969, 12, 31, 23, 59, 59, 999000)),
           (4, T(1970, 1, 1)), (5, None), (6, T(2020, 1, 1, 0, 0, 1))], files=[[0, 1], [2, 3], [4, 5]]),
    # arrow#51626 (dictionary strings: NULLs under a NULL struct not counted, unused dictionary
    # values in min/max): structs with string children, NULL structs, pa_dict_input_bloom
    shape("T5_dict_strings_under_null_struct", "O4", [("k", "int32"), ("o", "struct<x:int32,y:string>"), ("s", "string")],
          [(1, {"x": 1, "y": "m"}, "m"), (2, None, None), (3, {"x": None, "y": None}, "n"), (4, None, "o"),
           (5, {"x": 5, "y": "p"}, None)], files=[[0, 1, 2], [3, 4]]),
    # iceberg-rust#3482 / trinodb#30544 (filters on type-promoted columns): INT32 files at the int32
    # limits read under a BIGINT schema, filtered and aggregated
    shape("T6_int_files_read_as_bigint", "O4", [("k", "int32"), ("v", "int32")],
          [(1, 2 ** 31 - 1), (2, -2 ** 31), (3, 0), (4, None)], files=[[0, 1], [2, 3]],
          read_as=[("k", "int32"), ("v", "int64")]),
]
