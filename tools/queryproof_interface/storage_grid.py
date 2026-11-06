"""Phase two, oracle O5: literals on the storage grid (round D).

The bugs of phases one and two sit where a literal meets what is stored: a finer literal against a
coarser stored unit (Z1, datafusion#22142, polars#22435), a literal next to a statistics bound
that is not a data value (truncated or widened bounds), a literal in a calendar gap (G). The
templates so far take their literals from the data values. O5 adds two things:

  ms_variants      a copy of every shape with TIMESTAMP values, floored to whole milliseconds and
                   suffixed `_ms`, so that the millisecond writers (pa_ts_millis, polars_ts_millis)
                   cover every timestamp shape and not only the ones that happen to be exact
  stats_literals   the bounds the writers actually stored (chunk statistics, deprecated min/max for
                   signed leaves, column-index page bounds), as values of the read type, for
                   templates.build_queries to compare against with their one-step neighbours

Enabled with QP_O5=1 (shapes.py, extra_writers.py, run_interface.py).
"""
import copy
import datetime
import glob
import os
import re
import struct
from decimal import Decimal

ENABLED = bool(os.environ.get("QP_O5"))


def _floor_ms(v):
    if isinstance(v, datetime.datetime):
        return v.replace(microsecond=v.microsecond // 1000 * 1000)
    if isinstance(v, dict):
        return {k: _floor_ms(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return type(v)(_floor_ms(x) for x in v)
    return v


def _has_ts(ty):
    return re.search(r"(^|[<:,])ts($|[>,])", ty) is not None


def ms_variants(shapes):
    out = []
    for s in shapes:
        cols = s["read_as"] or s["columns"]
        if not any(_has_ts(ty) for _, ty in cols):
            continue
        rows = [_floor_ms(r) for r in s["rows"]]
        if rows == s["rows"]:  # already on the millisecond grid
            continue
        v = copy.deepcopy(s)
        v["id"], v["rows"], v["note"] = s["id"] + "_ms", rows, (s.get("note", "") + " [O5: floored to ms]").strip()
        out.append(v)
    return out


def _logical(n, raw, ty):
    """A decoded statistics value (parquet_audit.decode_stat) as a value of the template type."""
    import parquet_audit as A
    from shapes import parse_type
    t = parse_type(ty)
    if raw is None or isinstance(raw, float) and raw != raw:
        return None
    k = t[0]
    if k == "decimal":
        return Decimal(raw).scaleb(-t[2]) if isinstance(raw, int) else None
    if k == "date":
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=raw) if isinstance(raw, int) else None
    if k == "ts":
        unit = A.time_unit(n)
        if not isinstance(raw, int) or unit is None:
            return None
        micros = raw * 10 ** 6 // unit
        try:
            return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=micros)
        except OverflowError:
            return None
    if k == "string":
        try:
            return raw.decode("utf-8") if isinstance(raw, bytes) else None
        except UnicodeDecodeError:  # a truncated bound may cut a character
            return None
    if k in ("float32", "float64"):
        return float(raw) if isinstance(raw, (int, float)) else None
    if k.startswith("int") or k.startswith("uint"):
        return raw if isinstance(raw, int) else None
    return None


def stats_literals(shape, sets, leaf_types):
    """{template leaf path: [values]} from every Parquet file of the shape's file sets."""
    import parquet_audit as A
    from audit_selftest import footer
    out = {}
    for fs in sets:
        if fs.get("format") != "parquet":
            continue
        for p in glob.glob(os.path.join(fs["dir"], "**", "*.parquet"), recursive=True):
            try:
                buf = open(p, "rb").read()
                fm, _ = footer(buf)
                tree = A.leaves(A.build_tree(fm.schema))
            except Exception:  # noqa: BLE001 - a file the auditor cannot parse adds no literals
                continue
            for rg in fm.row_groups:
                for cc in rg.columns:
                    path = tuple(cc.meta_data.path_in_schema)
                    if path not in leaf_types or path not in tree:
                        continue
                    n, ty = tree[path][0], leaf_types[path]
                    raws = []
                    st = cc.meta_data.statistics
                    if st is not None:
                        raws += [st.min_value, st.max_value]
                        if A.leaf_kind(n) in ("signed", "float"):
                            raws += [st.min, st.max]
                    if cc.column_index_offset is not None:
                        try:
                            ci = A.read_struct(A.PT.ColumnIndex, buf, cc.column_index_offset)[0]
                            raws += [b for i, b in enumerate(ci.min_values + ci.max_values)
                                     if not ci.null_pages[i % len(ci.null_pages)]]
                        except Exception:  # noqa: BLE001
                            pass
                    for b in raws:
                        try:
                            v = _logical(n, A.decode_stat(n, b), ty)
                        except (struct.error, ValueError, OverflowError):
                            v = None
                        if v is not None:
                            out.setdefault(path, set()).add(v)
    return {k: sorted(v, key=repr) for k, v in out.items()}
