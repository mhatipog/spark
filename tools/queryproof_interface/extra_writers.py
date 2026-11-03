"""Phase-two writers (oracle O2): the same shapes written by Polars, DuckDB, DataFusion (arrow-rs),
fastparquet and chDB (ClickHouse), each with settings that push row groups, pages, page indexes
and bloom filters onto a few rows. Directory layout is the pyarrow writers' (partition_groups), so
only the file writer differs. Enabled by QP_EXTRA_WRITERS (a comma list, or "all"); a writer whose
library is not installed is skipped, and a shape a writer cannot represent is recorded as
QUERYPROOF_IFACE_WRITE_FAILED, not hidden.
"""
import os
import sys

from shapes import SHAPES, parse_type
from writers import arrow_table, entry, partition_groups


def has(shape, kinds):
    def walk(t):
        return t[0] in kinds or (t[0] == "struct" and any(walk(c) for _, c in t[1])) or \
            (t[0] in ("list", "map") and walk(t[1]))
    return any(walk(parse_type(ty)) for _, ty in shape["columns"])


def polars_writer(opts):
    def w(table, path):
        import polars as pl
        pl.from_arrow(table, rechunk=False).write_parquet(path, **opts)
    return w


def duckdb_writer(opts):
    def w(table, path):
        import duckdb
        con = duckdb.connect()
        con.register("src", table)
        con.execute(f"COPY src TO '{path.replace(chr(92), '/')}' (FORMAT parquet{opts})")
        con.close()
    return w


def datafusion_writer(opts):
    def w(table, path):
        import datafusion
        from datafusion import ParquetWriterOptions
        ctx = datafusion.SessionContext(datafusion.SessionConfig({"datafusion.execution.target_partitions": "1"}))
        ctx.from_arrow(table).write_parquet(path, ParquetWriterOptions(**opts))
    return w


def fastparquet_writer(opts):
    def w(table, path):
        import fastparquet
        import pandas as pd
        import pyarrow as pa
        nullable = {pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(),
                    pa.int64(): pd.Int64Dtype(), pa.uint8(): pd.UInt8Dtype(), pa.uint16(): pd.UInt16Dtype(),
                    pa.uint32(): pd.UInt32Dtype(), pa.float32(): pd.Float32Dtype(), pa.float64(): pd.Float64Dtype(),
                    pa.bool_(): pd.BooleanDtype()}
        fastparquet.write(path, table.to_pandas(types_mapper=nullable.get), **opts)
    return w


def chdb_writer(settings):
    def w(table, path):
        import chdb
        src = table  # noqa: F841 - read by the Python() table function below
        res = chdb.query(f"SELECT * FROM Python(src) SETTINGS {settings}", "Parquet")
        open(path, "wb").write(res.bytes())
    return w


# name -> (library, writer, shape filter: kinds the writer cannot represent)
WRITERS = {
    "polars_rg2_full_stats": ("polars", polars_writer(dict(row_group_size=2, statistics="full", data_page_size=64)),
                              {"map"}),
    "polars_default": ("polars", polars_writer(dict()), {"map"}),
    "duckdb_rg2_v2": ("duckdb", duckdb_writer(", ROW_GROUP_SIZE 2, PARQUET_VERSION v2"), set()),
    "duckdb_default": ("duckdb", duckdb_writer(""), set()),
    "df_rg2_pages1_bloom": ("datafusion", datafusion_writer(dict(
        max_row_group_size=2, data_page_row_count_limit=1, write_batch_size=1, statistics_enabled="page",
        bloom_filter_on_write=True, writer_version="2.0", dictionary_page_size_limit=16)), set()),
    "df_default": ("datafusion", datafusion_writer(dict()), set()),
    "fastparquet_rg2": ("fastparquet", fastparquet_writer(dict(row_group_offsets=2, stats=True, write_index=False)),
                        {"map", "struct", "list", "decimal", "uint64", "date", "float32", "float64"}),  # pandas: NaN == NULL
    "chdb_rg2_index_bloom": ("chdb", chdb_writer(
        "output_format_parquet_row_group_size=2, output_format_parquet_data_page_size=64, "
        "output_format_parquet_write_page_index=1, output_format_parquet_write_bloom_filter=1"), set()),
    "chdb_default": ("chdb", chdb_writer("output_format_parquet_row_group_size=1000000"), set()),
}


# Round C pyarrow settings, each from a recent bug (research/queryproof/2026-11-03-approach-v2.md, O4).
def pyarrow_writer(opts, dict_input=False, int96_bogus_stats=False):
    def w(table, path):
        import pyarrow.parquet as pq
        if dict_input:
            table = dictionary_input(table)
        pq.write_table(table, path, **opts)
        if int96_bogus_stats:
            bogus_int96_stats(path)
    return w


def dictionary_input(table):
    """String columns (top level and in structs) as dictionary arrays whose dictionary also holds
    values that no row uses (arrow#51626: unused dictionary values reached min/max)."""
    import pyarrow as pa

    def conv(arr):
        t = arr.type
        if pa.types.is_string(t):
            extra = pa.array(["", "zzzz-unused", "AAAA-unused"])
            vals = pa.concat_arrays([arr, extra]).dictionary_encode()
            return vals.slice(0, len(arr))
        if pa.types.is_struct(t):
            kids = [conv(arr.field(i)) for i in range(t.num_fields)]
            return pa.StructArray.from_arrays(kids, fields=[pa.field(t.field(i).name, kids[i].type) for i in
                                                            range(t.num_fields)], mask=arr.is_null())
        return arr
    cols = [conv(c.combine_chunks()) for c in table.columns]
    return pa.table(cols, names=table.column_names)


def bogus_int96_stats(path):
    """Statistics on INT96 columns as parquet-mr 1.8.1 wrote them (duckdb#27863): INT96 has no
    defined order, so a reader must ignore them. Here they claim a range holding no value."""
    import struct as st
    from audit_selftest import footer, with_footer
    buf = open(path, "rb").read()
    fm, _ = footer(buf)
    import parquet_audit as A
    changed = False
    for rg in fm.row_groups:
        for cc in rg.columns:
            if cc.meta_data.type == A.INT96:
                b = st.pack("<qi", 0, 2440588)  # 1970-01-01T00:00:00 as INT96 (nanos, Julian day)
                cc.meta_data.statistics = A.PT.Statistics(min=b, max=b, min_value=b, max_value=b, null_count=None)
                changed = True
    if changed:
        open(path, "wb").write(with_footer(buf, fm))


def ms_exact(shape):
    """Every ts value of the shape is a whole millisecond (so TIMESTAMP_MILLIS stores it exactly)."""
    import datetime
    for r in shape["rows"]:
        for v in r:
            if isinstance(v, datetime.datetime) and v.microsecond % 1000:
                return False
    return True


ROUND_C = {
    "pa_decint_page_index": (pyarrow_writer(dict(store_decimal_as_integer=True, write_page_index=True,
                                                 max_rows_per_page=1, row_group_size=4)), None),
    "pa_ts_millis": (pyarrow_writer(dict(coerce_timestamps="ms", allow_truncated_timestamps=False,
                                         write_page_index=True, max_rows_per_page=1)), ms_exact),
    "pa_int96_legacy_stats": (pyarrow_writer(dict(use_deprecated_int96_timestamps=True, row_group_size=2),
                                             int96_bogus_stats=True), None),
    "pa_delta_bss": (pyarrow_writer(dict(use_dictionary=False, use_byte_stream_split=True, data_page_version="2.0",
                                         write_page_index=True, max_rows_per_page=2)), None),
    "pa_cdc": (pyarrow_writer(dict(use_content_defined_chunking={"min_chunk_size": 32, "max_chunk_size": 128},
                                   write_page_index=True)), None),
    "pa_dict_input_bloom": (pyarrow_writer(dict(row_group_size=2, write_page_index=True, max_rows_per_page=1,
                                                bloom_filter_options={}), dict_input=True), None),
}


def write_round_c(base):
    out = []
    for wname, (fn, ok) in ROUND_C.items():
        for s in SHAPES:
            if ok is not None and not ok(s):
                continue
            d = os.path.join(base, s["id"], wname)
            try:
                os.makedirs(d, exist_ok=True)
                for fi, idx_all in enumerate(s["files"]):
                    for sub, idx in partition_groups(s, fi, idx_all):
                        os.makedirs(os.path.join(d, sub), exist_ok=True)
                        fn(arrow_table(s, idx), os.path.join(d, sub, f"part-{fi:03d}.parquet"))
                out.append(entry(s, wname, "parquet", "pyarrow", d))
            except Exception as exc:  # noqa: BLE001 - recorded, not hidden
                print("QUERYPROOF_IFACE_WRITE_FAILED", s["id"], wname, str(exc).split("\n")[0][:200], file=sys.stderr)
    return out


# Layout writers (round C, from the bug study: datafusion#26492/#26749, polars#30044/#30046,
# duckdb#26724): for a partitioned shape the engine itself writes the hive directories, from
# all rows at once (table: every row, partition columns included).
def polars_layout(table, d, part):
    import polars as pl
    pl.from_arrow(table).write_parquet(d, partition_by=part, mkdir=True)


def duckdb_layout(table, d, part):
    import duckdb
    con = duckdb.connect()
    con.register("src", table)
    con.execute(f"COPY src TO '{d.replace(chr(92), '/')}' (FORMAT parquet, PARTITION_BY ({', '.join(part)}), "
                f"OVERWRITE_OR_IGNORE)")
    con.close()


def datafusion_layout(table, d, part):
    import datafusion
    from datafusion import DataFrameWriteOptions
    ctx = datafusion.SessionContext()
    ctx.from_arrow(table).write_parquet(d, write_options=DataFrameWriteOptions(partition_by=part))


LAYOUT_WRITERS = {"polars_hive": ("polars", polars_layout), "duckdb_hive": ("duckdb", duckdb_layout),
                  "df_hive": ("datafusion", datafusion_layout)}


def write_layouts(base, names):
    import pyarrow as pa
    from shapes import to_arrow
    out = []
    for wname, (lib, fn) in LAYOUT_WRITERS.items():
        if (names != ["all"] and wname not in names) or not available(lib):
            continue
        for s in SHAPES:
            if not s.get("partition"):
                continue
            d = os.path.join(base, s["id"], wname)
            try:
                rows = [s["rows"][i] for f in s["files"] for i in f]
                cols = s["columns"]
                tbl = pa.table({n: [r[j] for r in rows] for j, (n, _) in enumerate(cols)},
                               schema=pa.schema([(n, to_arrow(parse_type(ty))) for n, ty in cols]))
                os.makedirs(d, exist_ok=True)
                fn(tbl, d, s["partition"])
                out.append(entry(s, wname, "parquet", lib, d))
            except Exception as exc:  # noqa: BLE001 - recorded, not hidden
                print("QUERYPROOF_IFACE_WRITE_FAILED", s["id"], wname, str(exc).split("\n")[0][:200], file=sys.stderr)
    return out


def available(lib):
    try:
        __import__(lib)
        return True
    except ImportError:
        return False


def write_extra(base, names):
    out = []
    for wname, (lib, fn, cannot) in WRITERS.items():
        if (names != ["all"] and wname not in names) or not available(lib):
            continue
        for s in SHAPES:
            if has(s, cannot):
                continue
            d = os.path.join(base, s["id"], wname)
            try:
                os.makedirs(d, exist_ok=True)
                for fi, idx_all in enumerate(s["files"]):
                    for sub, idx in partition_groups(s, fi, idx_all):
                        os.makedirs(os.path.join(d, sub), exist_ok=True)
                        fn(arrow_table(s, idx), os.path.join(d, sub, f"part-{fi:03d}.parquet"))
                out.append(entry(s, wname, "parquet", lib, d))
            except Exception as exc:  # noqa: BLE001 - recorded, not hidden
                print("QUERYPROOF_IFACE_WRITE_FAILED", s["id"], wname, str(exc).split("\n")[0][:200], file=sys.stderr)
    if os.environ.get("QP_LAYOUT_WRITERS"):  # round C
        out += write_layouts(base, names)
    if os.environ.get("QP_ROUND_C"):
        out += write_round_c(base)
    return out
