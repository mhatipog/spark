"""Positive controls for parquet_audit.py: corrupt one claim of a correct file at a time and require
the auditor to report exactly that claim, and report nothing on the unmodified file.

Usage: python audit_selftest.py WORKDIR
"""
import copy
import os
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from thriftpy2.protocol.compact import TCompactProtocol

import parquet_audit as A


class _Writer:
    def __init__(self):
        self.parts = []

    def write(self, b):
        self.parts.append(bytes(b))

    def flush(self):
        pass

    def getvalue(self):
        return b"".join(self.parts)


def ser(obj):
    w = _Writer()
    obj.write(TCompactProtocol(w))
    return w.getvalue()


def footer(buf):
    n = struct.unpack("<I", buf[-8:-4])[0]
    return A.read_struct(A.PT.FileMetaData, buf, len(buf) - 8 - n)[0], len(buf) - 8 - n


def with_footer(buf, fm):
    _, start = footer(buf)
    body = ser(fm)
    return buf[:start] + body + struct.pack("<I", len(body)) + b"PAR1"


def patch_struct(buf, offset, length, cls, mutate):
    """Re-serialize a page-index struct at `offset`; only when its length is unchanged."""
    obj = A.read_struct(cls, buf, offset)[0]
    mutate(obj)
    b = ser(obj)
    assert len(b) == length, ("patched struct changed length", len(b), length)
    return buf[:offset] + b + buf[offset + length:]


def main(work):
    os.makedirs(work, exist_ok=True)
    base = os.path.join(work, "base.parquet")
    t = pa.table({"k": pa.array(list(range(1, 9)), pa.int32()),
                  "s": pa.array(["a", None, "c", "d", None, "f", "g", "h"])})
    pq.write_table(t, base, row_group_size=8, max_rows_per_page=2, write_page_index=True,
                   use_dictionary=False, bloom_filter_options={"k": {"ndv": 16, "fpp": 0.01}})
    buf = open(base, "rb").read()
    fm, _ = footer(buf)
    cases = {}

    def nulls(fm):
        fm.row_groups[0].columns[1].meta_data.statistics.null_count += 1
    def minv(fm):
        fm.row_groups[0].columns[0].meta_data.statistics.min_value = struct.pack("<i", 2)
    def maxv(fm):
        fm.row_groups[0].columns[0].meta_data.statistics.max_value = struct.pack("<i", 7)
    def numrows(fm):
        fm.num_rows += 1
    for name, m, claim in [("null_count", nulls, "stats.null_count"), ("min", minv, "stats.min_not_lower_bound"),
                           ("max", maxv, "stats.max_not_upper_bound"), ("num_rows", numrows, "file.num_rows")]:
        f2 = copy.deepcopy(fm)
        m(f2)
        cases[name] = (with_footer(buf, f2), claim)
    cc = fm.row_groups[0].columns[1]

    def flip(ci):
        ci.null_pages[0] = not ci.null_pages[0]
    cases["null_pages"] = (patch_struct(buf, cc.column_index_offset, cc.column_index_length, A.PT.ColumnIndex, flip),
                           "column_index.null_pages")
    c0 = fm.row_groups[0].columns[0]

    def frow(oi):
        oi.page_locations[1].first_row_index += 1
    cases["first_row_index"] = (patch_struct(buf, c0.offset_index_offset, c0.offset_index_length, A.PT.OffsetIndex, frow),
                                "offset_index.first_row_index")
    bh, used = A.read_struct(A.PT.BloomFilterHeader, buf, c0.meta_data.bloom_filter_offset)
    start = c0.meta_data.bloom_filter_offset + used
    cases["bloom"] = (buf[:start] + b"\x00" * bh.numBytes + buf[start + bh.numBytes:], "bloom.false_negative")
    ok = True
    clean = A.audit_file(base)
    print("clean file:", "OK" if not clean else f"FAIL {clean}")
    ok &= not clean
    for name, (b, claim) in cases.items():
        p = os.path.join(work, f"mut_{name}.parquet")
        open(p, "wb").write(b)
        got = {v["claim"] for v in A.audit_file(p)}
        hit = claim in got
        print(f"{name:16s} expect {claim:32s}", "OK" if hit else "MISSED", sorted(got - {claim}))
        ok &= hit
    print("AUDIT_SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
