"""Write every shape as Parquet and ORC files under several writer settings.

pyarrow (arrow-cpp) writers run here; Spark's parquet-mr and orc-java writers run from
engines.py inside the Spark process (spark_write). Each file set is recorded in a manifest
with the footer statistics that were actually written, so triage never relies on the
contract document's claims about a writer.
"""
import json
import os

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq

from shapes import SHAPES, parse_type, to_arrow

# Writer settings: each forces skipping structures onto a few rows.
PA_PARQUET = {
    "pa_rg1_dict_v1_bloom": dict(row_group_size=1, use_dictionary=True, data_page_version="1.0",
                                 write_page_index=True, max_rows_per_page=1, bloom=True),
    "pa_rg2_plain_v2": dict(row_group_size=2, use_dictionary=False, data_page_version="2.0",
                            write_page_index=True, max_rows_per_page=1),
    "pa_pages1_dictfallback_bloom": dict(row_group_size=1 << 20, use_dictionary=True, dictionary_pagesize_limit=16,
                                         write_page_index=True, max_rows_per_page=1, bloom=True),
    "pa_decint_nostats_index": dict(row_group_size=3, store_decimal_as_integer=True, write_page_index=False,
                                    data_page_version="2.0"),
    # Statistics only for `k`: the other columns have no null_count, min or max (C1 N7, C2 E7).
    "pa_stats_k_only": dict(row_group_size=2, write_statistics=["k"], write_page_index=False),
}
PA_ORC = {
    "pa_orc_stride1_bloom": dict(row_index_stride=1, bloom=True),
    "pa_orc_default": dict(),
}
# Spark writer settings, applied as DataFrameWriter options (Hadoop configuration keys).
SPARK_WRITERS = {
    "mr_parquet_small_bloom": ("parquet", {
        "parquet.block.size": "64", "parquet.block.size.row.check.min": "1", "parquet.block.size.row.check.max": "2",
        "parquet.page.row.count.limit": "1", "parquet.page.size.row.check.min": "1",
        "parquet.page.size.row.check.max": "1", "parquet.bloom.filter.enabled": "true"}),
    "mr_parquet_v2_plain": ("parquet", {
        "parquet.block.size": "64", "parquet.block.size.row.check.min": "1", "parquet.block.size.row.check.max": "2",
        "parquet.page.row.count.limit": "1", "parquet.writer.version": "PARQUET_2_0",
        "parquet.enable.dictionary": "false"}),
    # Truncated string statistics and column index (C3 O2): bounds, not values.
    "mr_parquet_trunc4": ("parquet", {
        "parquet.block.size": "64", "parquet.block.size.row.check.min": "1", "parquet.block.size.row.check.max": "2",
        "parquet.page.row.count.limit": "1", "parquet.statistics.truncate.length": "4",
        "parquet.columnindex.truncate.length": "4"}),
    "orcj_stride_bloom": ("orc", {"orc.row.index.stride": "1000", "orc.bloom.filter.columns": "*",
                                  "orc.stripe.size": "1024"}),
}


def arrow_value(v, t):
    """Shape values to pyarrow input (maps are lists of pairs already)."""
    return v


def arrow_table(shape, idx):
    cols = shape["columns"]
    schema = pa.schema([(n, to_arrow(parse_type(ty))) for n, ty in cols])
    data = {n: [shape["rows"][i][j] for i in idx] for j, (n, _) in enumerate(cols)}
    return pa.table(data, schema=schema)


def bloom_columns(shape):
    return [n for n, ty in shape["columns"] if parse_type(ty)[0] not in ("struct", "list", "map")]


def write_pyarrow(base):
    """Write all pyarrow file sets; return manifest entries."""
    out = []
    for s in SHAPES:
        for wname, opts in PA_PARQUET.items():
            opts = dict(opts)
            bloom = opts.pop("bloom", False)
            if bloom:
                opts["bloom_filter_options"] = {c: {"ndv": 64, "fpp": 0.01} for c in bloom_columns(s)}
            d = os.path.join(base, s["id"], wname)
            os.makedirs(d, exist_ok=True)
            for fi, idx in enumerate(s["files"]):
                pq.write_table(arrow_table(s, idx), os.path.join(d, f"part-{fi:03d}.parquet"), **opts)
            out.append(entry(s, wname, "parquet", "pyarrow", d))
        for wname, opts in PA_ORC.items():
            opts = dict(opts)
            if opts.pop("bloom", False):
                opts["bloom_filter_columns"] = [i for i, (n, ty) in enumerate(s["columns"])
                                                if n in bloom_columns(s)]
            d = os.path.join(base, s["id"], wname)
            os.makedirs(d, exist_ok=True)
            for fi, idx in enumerate(s["files"]):
                orc.write_table(arrow_table(s, idx), os.path.join(d, f"part-{fi:03d}.orc"), **opts)
            out.append(entry(s, wname, "orc", "pyarrow", d))
    return out


def entry(shape, wname, fmt, writer, d):
    return {"shape": shape["id"], "concept": shape["concept"], "writer_set": wname, "format": fmt,
            "writer": writer, "dir": d, "footer": footer_summary(d, fmt)}


def footer_summary(d, fmt):
    """Per file: row groups (or stripes), rows, and per leaf column null_count/min/max presence."""
    files = sorted(f for f in os.listdir(d) if f.endswith("." + fmt))
    out = []
    for f in files:
        p = os.path.join(d, f)
        try:
            if fmt == "parquet":
                md = pq.ParquetFile(p).metadata
                groups = []
                for g in range(md.num_row_groups):
                    rg = md.row_group(g)
                    cols = {}
                    for c in range(rg.num_columns):
                        cc = rg.column(c)
                        st = cc.statistics
                        cols[cc.path_in_schema] = {
                            "nulls": None if st is None or not st.has_null_count else st.null_count,
                            "minmax": bool(st is not None and st.has_min_max),
                            "dict": cc.has_dictionary_page, "bloom": cc.bloom_filter_offset is not None,
                            "enc": list(cc.encodings)}
                    groups.append({"rows": rg.num_rows, "cols": cols})
                out.append({"file": f, "rows": md.num_rows, "row_groups": groups})
            else:
                of = orc.ORCFile(p)
                out.append({"file": f, "rows": of.nrows, "stripes": of.nstripes})
        except Exception as exc:  # noqa: BLE001 - recorded, not hidden
            out.append({"file": f, "error": str(exc)[:200]})
    return out


def main(base, manifest):
    entries = write_pyarrow(base)
    with open(manifest, "w", newline="\n") as fh:
        json.dump(entries, fh, indent=1, default=str)
    print("QUERYPROOF_IFACE_WRITE", len(entries), "file sets")


if __name__ == "__main__":
    import sys
    main(*sys.argv[1:3])
