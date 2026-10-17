"""Conservative runtime extraction of observable ORDER BY obligations from Spark's
analyzed logical plan. Requires no SQL regex and no manually supplied key positions.

This is a *partial* extractor. It returns UNKNOWN unless the entire required
lexicographic key is exposed as a supported integral column in the result.
It deliberately makes no assertion about optimized/physical-plan correctness.
"""
from __future__ import annotations

ORDERED_WRAPPERS = {"GlobalLimit", "LocalLimit", "Offset", "SubqueryAlias", "WithCTE"}
NUMERIC_TYPES = {"integer", "long", "short", "byte"}


def _simple(obj):
    return str(obj.getClass().getSimpleName())


def _seq(seq):
    for index in range(int(seq.size())):
        yield seq.apply(index)


def _id(attr):
    return int(attr.exprId().id())


def _plain_attribute(expr):
    """Only resolve direct AttributeReferences, optionally through a simple Alias."""
    kind = _simple(expr)
    if kind == "AttributeReference":
        return _id(expr)
    if kind == "Alias" and _simple(expr.child()) == "AttributeReference":
        return _id(expr.child())
    return None


def _unknown(reason, path, **extras):
    return {"status": "UNKNOWN", "reason": reason, "plan_path": path, "keys": [],
            **extras}


def extract(df) -> dict:
    """Extract an observable result-wide ordering obligation, or explicit UNKNOWN.

    Global ordering is considered only for top-level Sort beneath transparent
    order-preserving operators. Project is supported only when it exposes all
    ordering keys via direct attribute pass-through. No implicit analysis of
    joins, unions, aggregates, filtering, or inner CTE sorts.
    """
    path, slices = [], []
    try:
        analyzed = df._jdf.queryExecution().analyzed()
        final_output = list(_seq(analyzed.output()))
        mapping = {_id(a): i for i, a in enumerate(final_output)}
        plan = analyzed
        for depth in range(24):
            kind = _simple(plan)
            path.append(kind)

            if kind == "Sort":
                if not bool(getattr(plan, "global")()):
                    return _unknown("PARTITION_LOCAL_SORT", path, slice_nodes=slices)
                keys = []
                for so in _seq(plan.order()):
                    attr_id = _plain_attribute(so.child())
                    if attr_id is None:
                        return _unknown("COMPUTED_OR_HIDDEN_SORT_EXPRESSION", path,
                                        slice_nodes=slices, sort_expression=so.sql())
                    if attr_id not in mapping:
                        return _unknown("SORT_KEY_NOT_EXPOSED_IN_RESULT", path,
                                        slice_nodes=slices, sort_expression=so.sql())
                    kind_type = str(so.child().dataType().typeName())
                    if kind_type not in NUMERIC_TYPES:
                        return _unknown("UNSUPPORTED_SORT_KEY_TYPE", path,
                                        slice_nodes=slices, type_name=kind_type)
                    direction = str(so.direction().sql())
                    null_ordering = str(so.nullOrdering().sql())
                    if direction not in ("ASC", "DESC") or null_ordering not in (
                            "NULLS FIRST", "NULLS LAST"):
                        return _unknown("UNKNOWN_ORDER_COMPARATOR", path, slice_nodes=slices,
                                        direction=direction, null_ordering=null_ordering)
                    keys.append({
                        "index": mapping[attr_id],
                        "direction": direction,
                        "nulls": null_ordering.split()[-1],
                    })
                if not keys:
                    return _unknown("EMPTY_SORT_KEYS", path, slice_nodes=slices)
                return {"status": "EXTRACTED", "keys": keys, "plan_path": path,
                        "slice_nodes": slices, "scope": "OBSERVABLE_GLOBAL_RESULT_ORDER"}

            if kind == "Project":
                next_mapping = {}
                # Build a mapping for the child plan. Only direct attribute
                # pass-through survives; losing any key later yields UNKNOWN.
                for expression in _seq(plan.projectList()):
                    projected_id = _id(expression.toAttribute())
                    if projected_id in mapping:
                        origin_id = _plain_attribute(expression)
                        if origin_id is not None:
                            next_mapping[origin_id] = mapping[projected_id]
                mapping = next_mapping
                plan = plan.child()
                continue

            if kind == "WithCTE":
                plan = plan.plan()
                continue

            if kind in ORDERED_WRAPPERS:
                if kind in ("GlobalLimit", "LocalLimit"):
                    slices.append({"operator": kind, "expression": str(plan.limitExpr().sql())})
                elif kind == "Offset":
                    slices.append({"operator": kind, "expression": str(plan.offsetExpr().sql())})
                plan = plan.child()
                continue

            return _unknown("UNSUPPORTED_ROOT_OPERATOR_" + kind, path, slice_nodes=slices)
        return _unknown("MAX_DEPTH", path, slice_nodes=slices)
    except Exception as exc:
        return _unknown("INSPECTION_ERROR", path, slice_nodes=slices,
                        error=type(exc).__name__ + ": " + str(exc))
