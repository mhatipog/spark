"""Metadata soundness auditor (phase two, oracle O1; research/queryproof/2026-11-03-approach-v2.md).

Every claim a Parquet file makes about its data is checked against the data. The footer, the
page headers, the column and offset indexes and the bloom filters are decoded here from the
Thrift structures of parquet-format master (parquet.thrift). The values come from decoding
the rows (pyarrow, row group by row group) and rebuilding the leaf slots with their definition
and repetition levels from the file's own schema, so nothing relies on what a writer library
says about its output.

A violation is a writer bug whether or not a reader trips over it. Each one names a file,
row group, leaf path, page (if any), claim and detail.

Usage: python parquet_audit.py FILE_OR_DIR... (prints one JSON line per violation)
"""
import datetime
import decimal
import json
import math
import os
import struct
import sys
from decimal import Decimal

import thriftpy2
from thriftpy2.protocol.compact import TCompactProtocol

PT = thriftpy2.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet.thrift"),
                    module_name="parquet_thrift")
BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FLBA = range(8)
REQUIRED, OPTIONAL, REPEATED = range(3)
DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 = range(4)
DICT_ENCODINGS = {2, 8}  # PLAIN_DICTIONARY, RLE_DICTIONARY
SALT = [0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31]


class _Reader:
    """A read-only transport that remembers how far it has read."""

    def __init__(self, buf, pos):
        self.buf, self.pos = buf, pos

    def read(self, n):
        out = self.buf[self.pos:self.pos + n]
        if len(out) < n:
            raise EOFError("struct runs past the end of the file")
        self.pos += n
        return out


def read_struct(cls, buf, pos=0):
    """Deserialize one compact-protocol struct at `pos`; return (struct, bytes consumed)."""
    trans = _Reader(buf, pos)
    obj = cls()
    obj.read(TCompactProtocol(trans))
    return obj, trans.pos - pos


# ---------------------------------------------------------------- schema
class Node:
    def __init__(self, el):
        self.el, self.name, self.children = el, el.name, []
        self.rep = el.repetition_type if el.repetition_type is not None else REQUIRED
        self.lt, self.ct = el.logicalType, el.converted_type

    def is_list(self):
        return (self.lt is not None and self.lt.LIST is not None) or self.ct == 3  # LIST

    def is_map(self):
        return (self.lt is not None and self.lt.MAP is not None) or self.ct in (1, 2)  # MAP, MAP_KEY_VALUE


def build_tree(schema):
    it = iter(schema)

    def take():
        el = next(it)
        n = Node(el)
        for _ in range(el.num_children or 0):
            n.children.append(take())
        return n
    return take()


def leaves(root):
    """leaf path -> (node, max_def, max_rep)."""
    out = {}

    def walk(n, path, d, r):
        for c in n.children:
            dd = d + (c.rep != REQUIRED)
            rr = r + (c.rep == REPEATED)
            if c.children:
                walk(c, path + (c.name,), dd, rr)
            else:
                out[path + (c.name,)] = (c, dd, rr)
    walk(root, (), 0, 0)
    return out


def slots(root, rows):
    """leaf path -> list of (def, rep, value or None) over `rows` (pyarrow to_pylist dicts)."""
    out = {p: [] for p in leaves(root)}

    def null_all(n, path, d, r):
        if n.children:
            for c in n.children:
                null_all(c, path + (c.name,), d, r)
        else:
            out[path].append((d, r, None))

    def visit(n, path, v, d, r, rlev):
        """`n` is a node whose own repetition is handled here; `v` is its value."""
        if n.rep == OPTIONAL:
            if v is None:
                return null_all(n, path, d, r)
            d += 1
        elif n.rep == REPEATED:
            seq = list(v) if v is not None else []
            if not seq:
                return null_all(n, path, d, r)
            for i, e in enumerate(seq):
                inner(n, path, e, d + 1, r if i == 0 else rlev + 1, rlev + 1)
            return
        elif v is None:  # a REQUIRED node holding NULL: the writer broke its own schema
            return null_all(n, path, d, r)
        inner(n, path, v, d, r, rlev)

    def inner(n, path, v, d, r, rlev):
        if not n.children:
            out[path].append((d, r, v))
            return
        kids = n.children
        if n.is_list() and len(kids) == 1 and kids[0].rep == REPEATED:
            rep = kids[0]
            if rep.children and len(rep.children) == 1 and not (rep.name in ("array",) or rep.name.endswith("_tuple")):
                # three-level list: repeated group with one element child
                return visit_repeated_wrapper(rep, path + (rep.name,), v, d, r, rlev)
            return visit(rep, path + (rep.name,), v, d, r, rlev)  # two-level list
        if n.is_map() and len(kids) == 1 and kids[0].rep == REPEATED:
            kv = kids[0]
            pairs = [(k, x) for k, x in v] if v is not None else []
            return visit_kv(kv, path + (kv.name,), pairs, d, r, rlev)
        if n.rep == REPEATED and len(kids) > 1 and isinstance(v, tuple) and len(v) == len(kids):
            v = dict(zip([c.name for c in kids], v))
        for c in kids:
            cv = v.get(c.name) if isinstance(v, dict) else (v if len(kids) == 1 else None)
            visit(c, path + (c.name,), cv, d, r, rlev)

    def visit_repeated_wrapper(rep, path, seq, d, r, rlev):
        seq = list(seq) if seq is not None else []
        if not seq:
            return null_all(rep, path, d, r)
        el = rep.children[0]
        for i, e in enumerate(seq):
            visit(el, path + (el.name,), e, d + 1, r if i == 0 else rlev + 1, rlev + 1)

    def visit_kv(kv, path, pairs, d, r, rlev):
        if not pairs:
            return null_all(kv, path, d, r)
        names = [c.name for c in kv.children]
        for i, pair in enumerate(pairs):
            vals = dict(zip(names, pair))
            for c in kv.children:
                visit(c, path + (c.name,), vals.get(c.name), d + 1, r if i == 0 else rlev + 1, rlev + 1)

    for row in rows:
        for c in root.children:
            visit(c, (c.name,), row.get(c.name), 0, 0, 0)
    return out


# ---------------------------------------------------------------- values and order
def leaf_kind(n):
    """(order, decode) for a leaf: order is 'signed', 'unsigned', 'bytes', 'decimal', 'float',
    'bool' or None (no defined order: INT96, INTERVAL, FLOAT16, unknown)."""
    t, lt, ct = n.el.type, n.lt, n.ct
    if t == INT96 or ct == 21:  # INTERVAL
        return None
    if lt is not None and (lt.DECIMAL is not None) or ct == 5:
        return "decimal"
    if t in (INT32, INT64):
        if (lt is not None and lt.INTEGER is not None and not lt.INTEGER.isSigned) or ct in (11, 12, 13, 14):
            return "unsigned"
        return "signed"
    if t in (FLOAT, DOUBLE):
        return "float"
    if t == BOOLEAN:
        return "bool"
    if t == FLBA and lt is not None and getattr(lt, "FLOAT16", None) is not None:
        return None
    return "bytes"


def time_unit(n):
    lt = n.lt
    for kind in ("TIMESTAMP", "TIME"):
        x = getattr(lt, kind, None) if lt is not None else None
        if x is not None:
            u = x.unit
            return {"MILLIS": 10 ** 3, "MICROS": 10 ** 6, "NANOS": 10 ** 9}[
                next(k for k in ("MILLIS", "MICROS", "NANOS") if getattr(u, k) is not None)]
    if n.ct in (9, 7):  # TIMESTAMP_MILLIS, TIME_MILLIS
        return 10 ** 3
    if n.ct in (10, 8):  # TIMESTAMP_MICROS, TIME_MICROS
        return 10 ** 6
    return None


def scale_of(n):
    if n.lt is not None and n.lt.DECIMAL is not None:
        return n.lt.DECIMAL.scale
    return n.el.scale or 0


EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def key(n, v):
    """The decoded value as the physical comparison key of the leaf."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, Decimal):  # exact: the default context (28 digits) would round DECIMAL(38) values
        return int(v.scaleb(scale_of(n), context=decimal.Context(prec=80)))
    if hasattr(v, "value") and type(v).__name__ == "Timestamp":  # pandas Timestamp: nanoseconds
        return v.value * (time_unit(n) or 10 ** 9) // 10 ** 9
    if isinstance(v, datetime.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=datetime.timezone.utc)
        delta = v - EPOCH
        unit = time_unit(n) or 10 ** 6
        return (delta.days * 86400 + delta.seconds) * unit + delta.microseconds * unit // 10 ** 6
    if isinstance(v, datetime.date):
        return (v - datetime.date(1970, 1, 1)).days
    if isinstance(v, datetime.time):
        unit = time_unit(n) or 10 ** 6
        return ((v.hour * 60 + v.minute) * 60 + v.second) * unit + v.microsecond * unit // 10 ** 6
    if isinstance(v, datetime.timedelta):
        return None
    if isinstance(v, str):
        return v.encode("utf-8")
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if hasattr(v, "bytes") and type(v).__name__ == "UUID":
        return v.bytes
    return v


def decode_stat(n, b):
    """A statistics value (plain encoding of the physical type) as a comparison key."""
    if b is None:
        return None
    t, order = n.el.type, leaf_kind(n)
    if order == "decimal":
        if t == INT32:
            return struct.unpack("<i", b[:4])[0]
        if t == INT64:
            return struct.unpack("<q", b[:8])[0]
        return int.from_bytes(b, "big", signed=True) if b else 0
    if t == BOOLEAN:
        return b[0] & 1 if b else None
    if t == INT32:
        return struct.unpack("<I" if order == "unsigned" else "<i", b[:4])[0]
    if t == INT64:
        return struct.unpack("<Q" if order == "unsigned" else "<q", b[:8])[0]
    if t == FLOAT:
        return struct.unpack("<f", b[:4])[0]
    if t == DOUBLE:
        return struct.unpack("<d", b[:8])[0]
    return bytes(b)


def fmt(v):
    if isinstance(v, bytes):
        return v[:24].hex() + ("…" if len(v) > 24 else "")
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return repr(v)


# ---------------------------------------------------------------- bloom filter
def xxh64(b):
    import xxhash
    return xxhash.xxh64_intdigest(b, seed=0)


def plain_bytes(n, k):
    """Plain encoding of a physical value (for the bloom filter hash), or None to skip."""
    t, order = n.el.type, leaf_kind(n)
    if k is None or order is None:
        return None
    if order == "float" and math.isnan(k):
        return None
    if t == INT32:
        return struct.pack("<I" if order == "unsigned" else "<i", k)
    if t == INT64:
        return struct.pack("<Q" if order == "unsigned" else "<q", k)
    if t == FLOAT:
        return struct.pack("<f", k)
    if t == DOUBLE:
        return struct.pack("<d", k)
    if t == FLBA:
        if order == "decimal":
            return k.to_bytes(n.el.type_length, "big", signed=True)
        return k
    if t == BYTE_ARRAY and order != "decimal":
        return k
    return None


def bloom_contains(bitset, h):
    nblocks = len(bitset) // 32
    blk = ((h >> 32) * nblocks) >> 32
    key32 = h & 0xFFFFFFFF
    for i in range(8):
        bit = ((key32 * SALT[i]) & 0xFFFFFFFF) >> 27
        word = struct.unpack_from("<I", bitset, blk * 32 + 4 * i)[0]
        if not (word >> bit) & 1:
            return False
    return True


# ---------------------------------------------------------------- pages
CODECS = {0: None, 1: "snappy", 2: "gzip", 4: "brotli", 6: "zstd", 7: "lz4_raw"}


def walk_pages(buf, md):
    """The page headers of a column chunk, in file order: (offset, header)."""
    start = md.dictionary_page_offset if md.dictionary_page_offset else md.data_page_offset
    if md.dictionary_page_offset and md.dictionary_page_offset > md.data_page_offset:
        start = md.data_page_offset
    end, pos, out = start + md.total_compressed_size, start, []
    while pos < end:
        h, used = read_struct(PT.PageHeader, buf, pos)
        out.append((pos, h))
        pos += used + h.compressed_page_size
    return out


# ---------------------------------------------------------------- the audit
def check_bounds(viol, where, n, order, keys, mn, mx, exact_min=None, exact_max=None, claim="stats"):
    vals = [k for k in keys if k is not None]
    if order == "float":
        if mn is not None and isinstance(mn, float) and math.isnan(mn):
            viol(where, claim + ".min_is_nan", "min is NaN")
        if mx is not None and isinstance(mx, float) and math.isnan(mx):
            viol(where, claim + ".max_is_nan", "max is NaN")
        vals = [v for v in vals if not math.isnan(v)]
    if not vals:
        return
    lo, hi = min(vals), max(vals)
    if mn is not None and not (isinstance(mn, float) and math.isnan(mn)) and lo < mn:
        viol(where, claim + ".min_not_lower_bound", f"min {fmt(mn)} > value {fmt(lo)}")
    if mx is not None and not (isinstance(mx, float) and math.isnan(mx)) and hi > mx:
        viol(where, claim + ".max_not_upper_bound", f"max {fmt(mx)} < value {fmt(hi)}")
    if exact_min and mn is not None and mn != lo and not (order == "float" and mn == lo):
        viol(where, claim + ".min_not_exact", f"is_min_value_exact but min {fmt(mn)} != {fmt(lo)}")
    if exact_max and mx is not None and mx != hi and not (order == "float" and mx == hi):
        viol(where, claim + ".max_not_exact", f"is_max_value_exact but max {fmt(mx)} != {fmt(hi)}")


def audit_file(path):
    import pyarrow.parquet as pq
    buf = open(path, "rb").read()
    out = []
    if buf[-4:] != b"PAR1":
        return [{"file": path, "claim": "file.magic", "detail": "no PAR1 trailer"}]
    flen = struct.unpack("<I", buf[-8:-4])[0]
    fm, _ = read_struct(PT.FileMetaData, buf, len(buf) - 8 - flen)
    root = build_tree(fm.schema)
    lv = leaves(root)
    order_of = {p: leaf_kind(n) for p, (n, _, _) in lv.items()}
    has_orders = bool(fm.column_orders)
    pf = pq.ParquetFile(path)
    total = 0

    def viol(where, claim, detail):
        out.append({"file": path, **where, "claim": claim, "detail": detail})

    for gi, rg in enumerate(fm.row_groups):
        total += rg.num_rows
        rows = pf.read_row_group(gi).to_pylist()
        if len(rows) != rg.num_rows:
            viol({"rg": gi}, "rowgroup.num_rows", f"footer {rg.num_rows}, decoded {len(rows)}")
        sl = slots(root, rows)
        for ci, cc in enumerate(rg.columns):
            md = cc.meta_data
            p = tuple(md.path_in_schema)
            if p not in lv:
                viol({"rg": gi, "col": ".".join(p)}, "schema.path", "column path not in schema")
                continue
            n, maxd, maxr = lv[p]
            order = order_of[p] if has_orders else None
            s = sl[p]
            keys = [key(n, v) if d == maxd else None for d, r, v in s]
            where = {"rg": gi, "col": ".".join(p)}
            nulls = sum(1 for d, r, v in s if d < maxd)
            if md.num_values != len(s):
                viol(where, "chunk.num_values", f"footer {md.num_values}, slots {len(s)}")
            st = md.statistics
            if st is not None:
                if st.null_count is not None and st.null_count != nulls:
                    viol(where, "stats.null_count", f"claimed {st.null_count}, actual {nulls}")
                if order is not None:
                    mn, mx = decode_stat(n, st.min_value), decode_stat(n, st.max_value)
                    check_bounds(viol, where, n, order, keys, mn, mx, st.is_min_value_exact,
                                 st.is_max_value_exact)
                    if all(k is None for k in keys) and (mn is not None or mx is not None):
                        viol(where, "stats.minmax_on_all_null", "min/max present for an all-NULL chunk")
                if st.nan_count is not None and order == "float":
                    nan = sum(1 for k in keys if k is not None and math.isnan(k))
                    if st.nan_count != nan:
                        viol(where, "stats.nan_count", f"claimed {st.nan_count}, actual {nan}")
                if st.distinct_count is not None:
                    dc = len({k for k in keys if k is not None})
                    if st.distinct_count != dc:
                        viol(where, "stats.distinct_count", f"claimed {st.distinct_count}, actual {dc}")
            # size statistics (level histograms, unencoded bytes)
            ss = getattr(md, "size_statistics", None)
            if ss is not None:
                if ss.definition_level_histogram:
                    h = [0] * (maxd + 1)
                    for d, r, v in s:
                        h[d] += 1
                    if list(ss.definition_level_histogram) != h:
                        viol(where, "size_stats.def_histogram", f"claimed {list(ss.definition_level_histogram)}, actual {h}")
                if ss.repetition_level_histogram:
                    h = [0] * (maxr + 1)
                    for d, r, v in s:
                        h[r] += 1
                    if list(ss.repetition_level_histogram) != h:
                        viol(where, "size_stats.rep_histogram", f"claimed {list(ss.repetition_level_histogram)}, actual {h}")
                if ss.unencoded_byte_array_data_bytes is not None and n.el.type == BYTE_ARRAY:
                    nb = sum(len(k) for k in keys if isinstance(k, bytes))
                    if ss.unencoded_byte_array_data_bytes != nb and order == "bytes":
                        viol(where, "size_stats.unencoded_bytes", f"claimed {ss.unencoded_byte_array_data_bytes}, actual {nb}")
            # pages: split the slots by each data page's value count
            try:
                pages = walk_pages(buf, md) if md.num_values else []
            except Exception as exc:  # noqa: BLE001
                viol(where, "pages.unreadable", str(exc)[:120])
                pages = []
            data = [(off, h) for off, h in pages if h.type in (DATA_PAGE, DATA_PAGE_V2)]
            seen = {}
            pos, page_slots = 0, []
            for off, h in data:
                dh = h.data_page_header if h.type == DATA_PAGE else h.data_page_header_v2
                enc = dh.encoding
                seen[enc] = seen.get(enc, 0) + 1  # by encoding: writers record v2 pages as DATA_PAGE
                nv = dh.num_values
                if nv == 0 and md.num_values:
                    viol({**where, "page": len(page_slots)}, "page.zero_values", "a data page with no values in a non-empty chunk")
                ps = s[pos:pos + nv]
                page_slots.append((off, h, ps))
                pos += nv
                pw = {**where, "page": len(page_slots) - 1}
                if h.type == DATA_PAGE_V2:
                    pn = sum(1 for d, r, v in ps if d < maxd)
                    pr = sum(1 for d, r, v in ps if r == 0)
                    if dh.num_nulls != pn:
                        viol(pw, "page_v2.num_nulls", f"claimed {dh.num_nulls}, actual {pn}")
                    if dh.num_rows != pr:
                        viol(pw, "page_v2.num_rows", f"claimed {dh.num_rows}, actual {pr}")
                if ps and ps[0][1] != 0 and (fm.row_groups and (cc.offset_index_offset or h.type == DATA_PAGE_V2)):
                    viol(pw, "page.starts_mid_row", "page does not begin at a row boundary")
                pst = dh.statistics
                if pst is not None and order is not None:
                    pk = [key(n, v) if d == maxd else None for d, r, v in ps]
                    if pst.null_count is not None:
                        pn = sum(1 for d, r, v in ps if d < maxd)
                        if pst.null_count != pn:
                            viol(pw, "page_stats.null_count", f"claimed {pst.null_count}, actual {pn}")
                    check_bounds(viol, pw, n, order, pk, decode_stat(n, pst.min_value), decode_stat(n, pst.max_value),
                                 pst.is_min_value_exact, pst.is_max_value_exact, claim="page_stats")
            if data and pos != len(s):
                viol(where, "pages.value_total", f"pages hold {pos} values, chunk has {len(s)}")
            if md.encoding_stats:
                claimed = {}
                for es in md.encoding_stats:
                    if es.page_type in (DATA_PAGE, DATA_PAGE_V2):
                        claimed[es.encoding] = claimed.get(es.encoding, 0) + es.count
                if data and claimed != seen:
                    viol(where, "encoding_stats", f"claimed {sorted(claimed.items())}, pages {sorted(seen.items())}")
            if data and md.encodings is not None:
                missing = set(seen) - set(md.encodings)
                if missing:
                    viol(where, "encodings_list", f"data page encodings {sorted(missing)} not in the chunk's encodings")
            # page index
            if cc.offset_index_offset is not None and cc.offset_index_length:
                oi, _ = read_struct(PT.OffsetIndex, buf, cc.offset_index_offset)
                locs = oi.page_locations
                if [l.offset for l in locs] != [off for off, _, _ in page_slots]:
                    viol(where, "offset_index.locations", f"{len(locs)} locations vs {len(page_slots)} data pages")
                starts, row = [], 0
                for off, h, ps in page_slots:
                    starts.append(row)
                    row += sum(1 for d, r, v in ps if r == 0)
                if [l.first_row_index for l in locs] != starts[:len(locs)] or len(locs) != len(starts):
                    viol(where, "offset_index.first_row_index", f"claimed {[l.first_row_index for l in locs][:12]}, actual {starts[:12]}")
                ub = getattr(oi, "unencoded_byte_array_data_bytes", None)
                if ub and n.el.type == BYTE_ARRAY and order == "bytes" and len(ub) == len(page_slots):
                    act = [sum(len(key(n, v)) for d, r, v in ps if d == maxd and v is not None) for _, _, ps in page_slots]
                    if list(ub) != act:
                        viol(where, "offset_index.unencoded_bytes", f"claimed {list(ub)[:12]}, actual {act[:12]}")
            if cc.column_index_offset is not None and cc.column_index_length and order is not None:
                ci, _ = read_struct(PT.ColumnIndex, buf, cc.column_index_offset)
                if len(ci.null_pages) != len(page_slots):
                    viol(where, "column_index.page_count", f"{len(ci.null_pages)} entries vs {len(page_slots)} data pages")
                else:
                    mins, maxs = [], []
                    for i, (off, h, ps) in enumerate(page_slots):
                        pw = {**where, "page": i}
                        pk = [key(n, v) if d == maxd else None for d, r, v in ps]
                        allnull = all(k is None for k in pk)
                        if bool(ci.null_pages[i]) != allnull:
                            viol(pw, "column_index.null_pages", f"null_pages={ci.null_pages[i]} but page has {'no' if allnull else 'some'} non-null values")
                        if ci.null_counts is not None and i < len(ci.null_counts):
                            pn = sum(1 for d, r, v in ps if d < maxd)
                            if ci.null_counts[i] != pn:
                                viol(pw, "column_index.null_counts", f"claimed {ci.null_counts[i]}, actual {pn}")
                        if not ci.null_pages[i]:
                            mn, mx = decode_stat(n, ci.min_values[i]), decode_stat(n, ci.max_values[i])
                            check_bounds(viol, pw, n, order, pk, mn, mx, claim="column_index")
                            mins.append(mn)
                            maxs.append(mx)
                        if ci.definition_level_histograms:
                            w = maxd + 1
                            h = [0] * w
                            for d, r, v in ps:
                                h[d] += 1
                            got = list(ci.definition_level_histograms[i * w:(i + 1) * w])
                            if got != h:
                                viol(pw, "column_index.def_histogram", f"claimed {got}, actual {h}")
                    bo = ci.boundary_order
                    clean = lambda xs: [x for x in xs if not (isinstance(x, float) and math.isnan(x))]  # noqa: E731
                    if bo == 1 and (clean(mins) != sorted(clean(mins)) or clean(maxs) != sorted(clean(maxs))):
                        viol(where, "column_index.boundary_order", "ASCENDING but page bounds are not ascending")
                    if bo == 2 and (clean(mins) != sorted(clean(mins), reverse=True) or clean(maxs) != sorted(clean(maxs), reverse=True)):
                        viol(where, "column_index.boundary_order", "DESCENDING but page bounds are not descending")
            # bloom filter
            if md.bloom_filter_offset is not None and md.bloom_filter_offset > 0 and order is not None:
                try:
                    bh, used = read_struct(PT.BloomFilterHeader, buf, md.bloom_filter_offset)
                    start = md.bloom_filter_offset + used
                    bits = buf[start:start + bh.numBytes]
                    miss = [k for k in {k for k in keys if k is not None}
                            if plain_bytes(n, k) is not None and not bloom_contains(bits, xxh64(plain_bytes(n, k)))]
                    if miss:
                        viol(where, "bloom.false_negative", f"{len(miss)} value(s) absent, e.g. {fmt(sorted(miss, key=repr)[0])}")
                except Exception as exc:  # noqa: BLE001
                    viol(where, "bloom.unreadable", str(exc)[:120])
    if total != fm.num_rows:
        out.append({"file": path, "claim": "file.num_rows", "detail": f"footer {fm.num_rows}, row groups {total}"})
    return out


def audit_paths(paths):
    out = []
    for p in paths:
        files = [p] if os.path.isfile(p) else sorted(
            os.path.join(r, f) for r, _, fs in os.walk(p) for f in fs if f.endswith(".parquet"))
        for f in files:
            try:
                out += audit_file(f)
            except Exception as exc:  # noqa: BLE001 - an unreadable file is itself a finding
                out.append({"file": f, "claim": "audit.error", "detail": f"{type(exc).__name__}: {str(exc)[:160]}"})
    return out


def audit_shard(base, shard, count):
    """Audit the file sets of this shard's shapes (shapes.SHAPES under QP_IFACE_GEN), every writer."""
    from shapes import SHAPES
    out = []
    for i, s in enumerate(SHAPES):
        if i % count != shard or not os.path.isdir(os.path.join(base, s["id"])):
            continue
        for wname in sorted(os.listdir(os.path.join(base, s["id"]))):
            for v in audit_paths([os.path.join(base, s["id"], wname)]):
                out.append({"shape": s["id"], "writer_set": wname, **v,
                            "file": os.path.relpath(v["file"], base).replace(os.sep, "/")})
    return out


if __name__ == "__main__":
    if sys.argv[1] == "--shard":  # --shard BASE SHARD COUNT OUT.json
        import time
        t0 = time.time()
        res = audit_shard(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
        json.dump({"violations": res, "cpu_seconds": round(time.time() - t0, 1)}, open(sys.argv[5], "w"), indent=1,
                  default=str)
        print("QUERYPROOF_AUDIT", len(res), "violations", round(time.time() - t0, 1), "s")
    else:
        for v in audit_paths(sys.argv[1:]):
            print(json.dumps(v, default=str))
