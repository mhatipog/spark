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
    return out
