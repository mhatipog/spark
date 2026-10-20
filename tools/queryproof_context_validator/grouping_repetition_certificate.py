"""Local row-bag certificate for duplicate GROUP BY expression elimination.

The distinct grouping expressions remain exactly the same expressions (up to
permutation). Removing repeated deterministic, total keys leaves the
equivalence relation on rows unchanged, so each aggregate sees the same input
bag. This includes NULL keys, duplicate rows and an empty child bag.

The proof is deliberately about *one* Catalyst Aggregate transition with
unchanged outputs, input subtree and optional identical Sort frames. It does
not certify physical ordering, runtime error behavior or whole optimizer runs.
"""
import json

from equality_orbits import Unsupported, require
from join_prefilter_certificate import LOGICAL, EXPR, tree_from_preorder
from generalized_projection_liveness_certificate import (
    _aggregate_references, _expression, _literal_value,
    _projection_outputs, _projection_stack,
)
from column_pruning_liveness_certificate import EXACT_ARITIES
from collapse_project_certificate import _attribute, _identity

GROUP_METADATA = {
    "class", "num-children", "child",
    "groupingExpressions", "aggregateExpressions",
}


def _group_key(expression, scope):
    require(isinstance(expression, list) and bool(expression),
            "EMPTY_GROUPING_EXPRESSION")
    for node in expression:
        require(isinstance(node, dict), "INVALID_GROUPING_EXPRESSION")
        if node.get("class") == EXPR + "AttributeReference":
            required = {"class", "num-children", "name", "dataType", "exprId"}
            optional = {"qualifier", "metadata", "nullable"}
            qualifier = node.get("qualifier", [])
            require(required <= set(node) <= required | optional and
                    (qualifier == [] or
                     (isinstance(qualifier, str) and
                      len(qualifier) <= 512)) and
                    ("nullable" not in node or
                     type(node["nullable"]) is bool) and
                    ("metadata" not in node or
                     (isinstance(node["metadata"], dict) and
                      len(json.dumps(node["metadata"], sort_keys=True)) <= 4096)),
                    "UNTRUSTED_GROUPING_ATTRIBUTE_METADATA")
    _expression(expression, scope)
    # Exact syntax is a stronger, simpler condition than Catalyst's
    # semanticEquals: no normalizations or name-based bindings are assumed.
    return json.dumps(expression, sort_keys=True, separators=(",", ":"))


def _checked_aggregate_references(node):
    """Extend the previous aggregate grammar only with extrema of direct atoms."""
    try:
        return _aggregate_references(node)
    except Unsupported:
        pass
    # Nothing beyond the narrow Max/Min fallback justifies widening the
    # preexisting aggregate validator or altering its frozen implementation.
    outputs = node.get("aggregateExpressions")
    require(isinstance(outputs, list) and any(
        isinstance(entry, list) and any(
            part.get("class") in (EXPR + "aggregate.Max",
                                  EXPR + "aggregate.Min")
            for part in entry if isinstance(part, dict))
        for entry in outputs), "NO_EXTREMUM_REQUIRING_EXTENSION")
    require(node.get("class") == LOGICAL + "Aggregate" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == GROUP_METADATA, "INVALID_EXTREMA_AGGREGATE_METADATA")
    groups = node["groupingExpressions"]
    require(isinstance(groups, list) and bool(outputs),
            "BAD_EXTREMA_AGGREGATE_ITEMS")
    arities = dict(EXACT_ARITIES, Upper=1, Lower=1, Remainder=2,
                   **{"aggregate.Max": 1, "aggregate.Min": 1})

    def scan(nodes):
        require(isinstance(nodes, list) and bool(nodes),
                "EMPTY_EXTREMA_AGGREGATE_EXPRESSION")

        def read(pos):
            require(pos < len(nodes), "TRUNCATED_EXTREMA_EXPRESSION")
            expr = nodes[pos]
            require(isinstance(expr, dict) and
                    isinstance(expr.get("class"), str) and
                    expr["class"].startswith(EXPR),
                    "UNKNOWN_EXTREMA_EXPRESSION_NAMESPACE")
            op = expr["class"][len(EXPR):]
            require(op in arities and
                    expr.get("num-children") == arities[op],
                    "UNKNOWN_EXTREMA_EXPRESSION")
            if op == "AttributeReference":
                key, dtype, _ = _attribute(expr)
                return pos + 1, {(key, dtype)}
            if op == "Literal":
                _literal_value(expr)
                return pos + 1, set()
            if op in ("aggregate.Max", "aggregate.Min"):
                require(pos + 1 < len(nodes) and
                        nodes[pos + 1].get("class") in
                        (EXPR + "AttributeReference", EXPR + "Literal"),
                        "EXTREMUM_NONATOMIC_INPUT")
            if op == "aggregate.AggregateExpression":
                require(not any(k in expr for k in
                                ("filter", "filterExpression", "filterOption")),
                        "FILTERED_AGGREGATE_NOT_ADMITTED")
            if op == "Alias":
                _identity(expr)
                require(expr.get("child") == 0 and
                        isinstance(expr.get("name"), str),
                        "INVALID_EXTREMA_ALIAS")
            cursor, refs = pos + 1, set()
            for _ in range(arities[op]):
                cursor, subrefs = read(cursor)
                refs.update(subrefs)
            return cursor, refs

        end, refs = read(0)
        require(end == len(nodes), "TRAILING_EXTREMA_EXPRESSION")
        return refs

    refs = set()
    for expression in [*groups, *outputs]:
        refs.update(scan(expression))
    return refs


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    require(old != new, "NO_TRANSITION")
    sort_frames = 0
    while old[0].get("class") == LOGICAL + "Sort":
        frame, children = old
        other, other_children = new
        require(frame == other and frame.get("num-children") == 1 and
                frame.get("child") == 0 and
                len(children) == len(other_children) == 1,
                "SORT_CONTEXT_CHANGED")
        old, new = children[0], other_children[0]
        sort_frames += 1

    before, old_children = old
    after, new_children = new
    require(before.get("class") == after.get("class") ==
            LOGICAL + "Aggregate" and
            len(old_children) == len(new_children) == 1 and
            old_children[0] == new_children[0],
            "AGGREGATE_INPUT_CHANGED")
    require(set(before) == set(after) == GROUP_METADATA and
            before.get("num-children") == after.get("num-children") == 1 and
            before.get("child") == after.get("child") == 0,
            "AGGREGATE_METADATA_CHANGED")
    require(all(before[k] == after[k] for k in GROUP_METADATA
                if k != "groupingExpressions"),
            "AGGREGATE_OUTPUT_OR_METADATA_CHANGED")
    old_groups = before["groupingExpressions"]
    new_groups = after["groupingExpressions"]
    require(isinstance(old_groups, list) and isinstance(new_groups, list) and
            len(old_groups) > len(new_groups) >= 1,
            "NOT_STRICT_NONEMPTY_GROUP_DEDUPLICATION")

    # Both plans have identical resolved outputs and identical relational
    # inputs; only repeated grouping keys may disappear.
    references = (_checked_aggregate_references(before) |
                  _checked_aggregate_references(after))
    stack, base = _projection_stack(old_children[0])
    scope = _projection_outputs(stack, base)
    for key, dtype in references:
        require(key in scope and scope[key][0] == dtype,
                "AGGREGATE_REFERENCE_OUT_OF_SCOPE")

    before_keys = [_group_key(g, scope) for g in old_groups]
    after_keys = [_group_key(g, scope) for g in new_groups]
    require(len(set(after_keys)) == len(after_keys),
            "AFTER_CONTAINS_DUPLICATE_GROUPING_KEYS")
    require(set(before_keys) == set(after_keys),
            "GROUPING_KEY_SET_CHANGED")

    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "duplicate_grouping_key_idempotence",
        "grouping_keys_before": len(before_keys),
        "grouping_keys_after": len(after_keys),
        "removed_duplicates": len(before_keys) - len(after_keys),
        "sort_frames": sort_frames,
        "live_attributes": len(references),
        "semantic_law": "partition(R, [g,g,...]) == partition(R, [g,...])",
    }


def certify_grouping_repetition_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
