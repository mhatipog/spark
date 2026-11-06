"""Phase two, oracle O6: spec-legal metadata variants (round D).

Every variant rewrites the metadata of a correct file into another form the Parquet format
allows, and leaves the data pages alone, so every reader must return the same rows for it as for
the original. The forms come from what real writers do differently (C2: chDB omits null_counts;
parquet-java truncates binary bounds; old writers fill only the deprecated min/max; writers
differ in boundary_order and created_by):

  nonullcounts  ColumnIndex.null_counts omitted (optional)
  nonullcount   Statistics.null_count omitted (optional)
  inexact       min/max widened by one step in the chunk statistics and the column index, with
                is_min_value_exact / is_max_value_exact false and boundary_order UNORDERED
  legacy        only the deprecated min/max (signed order: kept for signed, float and integer
                decimal leaves, dropped for the others)
  nostats       no chunk Statistics (the column index stays)
  noindex       no column index and no offset index
  ordered       boundary_order ASCENDING or DESCENDING where the pages really are
  unordered     boundary_order UNORDERED (always allowed; readers lose the binary search)
  oldmr         created_by of parquet-mr 1.8.1

A created_by of an old parquet-cpp is not used: readers rightly apply that writer's known bugs
(Arrow reads its V2 data pages as the old writer laid them out, PARQUET_CPP_10353), so the variant
would describe a file that writer never wrote. Readers' version workarounds are by design; treat
oldmr findings with that in mind.

Validity is checked, not assumed: a variant whose audit (parquet_audit.audit_file) reports a
claim that the original file does not is dropped and logged (QUERYPROOF_O6_INVALID).
Enabled with QP_O6=1; QP_O6_BASES picks the writer sets to derive from.
"""
import copy
import math
import os
import shutil
import struct
import sys

import parquet_audit as A
from audit_selftest import footer, ser

BASES = os.environ.get("QP_O6_BASES", "pa_rg2_plain_v2,pa_pages1_dictfallback_bloom").split(",")
OLD_MR = "parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)"


def encode_stat(n, v, like):
    """Inverse of parquet_audit.decode_stat; `like` is the original bytes (for fixed lengths)."""
    t, order = n.el.type, A.leaf_kind(n)
    if order == "decimal":
        if t == A.INT32:
            return struct.pack("<i", v)
        if t == A.INT64:
            return struct.pack("<q", v)
        return v.to_bytes(len(like), "big", signed=True)
    if t == A.INT32:
        return struct.pack("<I" if order == "unsigned" else "<i", v)
    if t == A.INT64:
        return struct.pack("<Q" if order == "unsigned" else "<q", v)
    if t == A.FLOAT:
        return struct.pack("<f", v)
    if t == A.DOUBLE:
        return struct.pack("<d", v)
    return bytes(v)


def int_range(n):
    """The logical range of an integer leaf (so a widened bound stays a value of the type)."""
    lt, t, order = n.lt, n.el.type, A.leaf_kind(n)
    bits = 32 if t == A.INT32 else 64
    if lt is not None and lt.INTEGER is not None:
        bits = lt.INTEGER.bitWidth
    elif n.ct in (15, 11):  # INT_8, UINT_8
        bits = 8
    elif n.ct in (16, 12):  # INT_16, UINT_16
        bits = 16
    if order == "unsigned":
        return 0, 2 ** bits - 1
    return -2 ** (bits - 1), 2 ** (bits - 1) - 1


def next_f32(v, down):
    """The neighbouring float32 of `v` towards -inf (down) or +inf."""
    if v == 0:
        return -1.401298464324817e-45 if down else 1.401298464324817e-45
    i = struct.unpack("<i", struct.pack("<f", v))[0] + (-1 if (v > 0) == down else 1)
    return struct.unpack("<f", struct.pack("<i", i))[0]


def widen(n, b, down):
    """One step outward from the bound `b` (bytes), or None when there is no legal step."""
    if b is None:
        return None
    order, t = A.leaf_kind(n), n.el.type
    v = A.decode_stat(n, b)
    if order in ("signed", "unsigned"):
        lo, hi = int_range(n)
        w = v - 1 if down else v + 1
        return encode_stat(n, w, b) if lo <= w <= hi else None
    if order == "decimal":
        prec = n.lt.DECIMAL.precision if n.lt is not None and n.lt.DECIMAL is not None else n.el.precision
        w = v - 1 if down else v + 1
        if abs(w) >= 10 ** prec:
            return None
        try:
            return encode_stat(n, w, b)
        except OverflowError:
            return None
    if order == "float":
        if not math.isfinite(v):
            return None
        w = next_f32(v, down) if t == A.FLOAT else math.nextafter(v, -math.inf if down else math.inf)
        return encode_stat(n, w, b) if math.isfinite(w) else None
    if order == "bytes" and t == A.BYTE_ARRAY:
        if down:
            return bytes(b[:-1]) if len(b) else None
        return bytes(b) + b"\x00"
    return None


def _stats_inexact(n, st):
    if st is None:
        return
    for new, old, flag, down in (("min_value", "min_value", "is_min_value_exact", True),
                                 ("max_value", "max_value", "is_max_value_exact", False)):
        w = widen(n, getattr(st, old), down)
        if w is not None:
            setattr(st, new, w)
            setattr(st, flag, False)


def _ci_inexact(n, ci):
    for i, null in enumerate(ci.null_pages):
        if null:
            continue
        lo, hi = widen(n, ci.min_values[i], True), widen(n, ci.max_values[i], False)
        if lo is not None:
            ci.min_values[i] = lo
        if hi is not None:
            ci.max_values[i] = hi
    ci.boundary_order = 0  # UNORDERED: widening can break an ordering claim


def _ci_ordered(n, ci):
    keys = [(A.decode_stat(n, ci.min_values[i]), A.decode_stat(n, ci.max_values[i]))
            for i, null in enumerate(ci.null_pages) if not null]
    if len(keys) < 2 or A.leaf_kind(n) in (None, "bool") or any(isinstance(k, float) and math.isnan(k)
                                                               for kk in keys for k in kk):
        return
    mins, maxs = [k[0] for k in keys], [k[1] for k in keys]
    if mins == sorted(mins) and maxs == sorted(maxs):
        ci.boundary_order = 1  # ASCENDING
    elif mins == sorted(mins, reverse=True) and maxs == sorted(maxs, reverse=True):
        ci.boundary_order = 2  # DESCENDING


def _legacy(n, st):
    if st is None:
        return
    keep = A.leaf_kind(n) in ("signed", "float") or (A.leaf_kind(n) == "decimal" and n.el.type in (A.INT32, A.INT64))
    st.min, st.max = (st.min_value, st.max_value) if keep else (None, None)
    st.min_value = st.max_value = st.is_min_value_exact = st.is_max_value_exact = None


MUTATIONS = {
    "nonullcounts": dict(ci=lambda n, ci: setattr(ci, "null_counts", None)),
    "nonullcount": dict(stats=lambda n, st: st is not None and setattr(st, "null_count", None)),
    "inexact": dict(stats=_stats_inexact, ci=_ci_inexact),
    "legacy": dict(stats=_legacy),
    "nostats": dict(chunk=lambda n, cc: setattr(cc.meta_data, "statistics", None)),
    "noindex": dict(chunk=lambda n, cc: [setattr(cc, f, None) for f in (
        "column_index_offset", "column_index_length", "offset_index_offset", "offset_index_length")]),
    "ordered": dict(ci=_ci_ordered),
    "unordered": dict(ci=lambda n, ci: setattr(ci, "boundary_order", 0)),
    "oldmr": dict(file=lambda fm: setattr(fm, "created_by", OLD_MR)),
}


def mutate(buf, name):
    """The variant `name` of the file bytes `buf`; None when it changes nothing."""
    m = MUTATIONS[name]
    fm, start = footer(buf)
    fm = copy.deepcopy(fm)
    tree = A.leaves(A.build_tree(fm.schema))
    body, before = bytearray(buf[:start]), ser(fm)
    for rg in fm.row_groups:
        for cc in rg.columns:
            n = tree[tuple(cc.meta_data.path_in_schema)][0]
            if "chunk" in m:
                m["chunk"](n, cc)
            if "stats" in m:
                m["stats"](n, cc.meta_data.statistics)
            if "ci" in m and cc.column_index_offset is not None:
                ci = A.read_struct(A.PT.ColumnIndex, buf, cc.column_index_offset)[0]
                old = ser(ci)
                m["ci"](n, ci)
                new = ser(ci)
                if new != old:  # appended after the data; the old index bytes stay, unreferenced
                    cc.column_index_offset, cc.column_index_length = len(body), len(new)
                    body += new
    if "file" in m:
        m["file"](fm)
    after = ser(fm)
    if after == before:
        return None
    return bytes(body) + after + struct.pack("<I", len(after)) + b"PAR1"


def claims(path):
    try:
        return {(v["claim"], v.get("col")) for v in A.audit_file(path)}
    except Exception as exc:  # noqa: BLE001 - the auditor (pyarrow) cannot read the file
        return {("unreadable", f"{type(exc).__name__}: {str(exc)[:120]}")}


def write_variants(base_dir, manifest):
    """Derive every variant of every base writer set in `manifest`; return the new entries."""
    out = []
    for e in list(manifest):
        if e["writer_set"] not in BASES or e["format"] != "parquet":
            continue
        for name in MUTATIONS:
            d = os.path.join(os.path.dirname(e["dir"]), f"m_{name}__{e['writer_set']}")
            shutil.rmtree(d, ignore_errors=True)
            changed, bad = False, None
            for root, _, files in os.walk(e["dir"]):
                for f in files:
                    src = os.path.join(root, f)
                    dst = os.path.join(d, os.path.relpath(src, e["dir"]))
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    if not f.endswith(".parquet"):
                        shutil.copyfile(src, dst)
                        continue
                    buf = open(src, "rb").read()
                    try:
                        new = mutate(buf, name)
                    except Exception as exc:  # noqa: BLE001 - recorded, not hidden
                        bad = f"mutation failed: {type(exc).__name__}: {str(exc)[:160]}"
                        break
                    open(dst, "wb").write(new if new is not None else buf)
                    if new is not None:
                        changed = True
                        extra = claims(dst) - claims(src)
                        if extra:
                            bad = f"audit: {sorted(extra)[:3]}"
                            break
                if bad:
                    break
            if bad or not changed:
                if bad:
                    print("QUERYPROOF_O6_INVALID", e["shape"], name, e["writer_set"], bad, file=sys.stderr)
                shutil.rmtree(d, ignore_errors=True)
                continue
            out.append({**e, "writer_set": f"m_{name}__{e['writer_set']}", "dir": d,
                        "writer": e["writer"] + "+o6"})
    return out
