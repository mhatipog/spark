"""Conservative observational liveness certificate across Project stacks.

If two stacks of pure, total, one-to-one Projects have the same unchanged base
bag and deliver syntactically identical normalized values for every resolved
attribute read by an unchanged Aggregate, the aggregate output bags coincide.
Optional unchanged Sort ancestors preserve the bag conclusion (not order).

This is a local, explicitly restricted Catalyst logical-semantics theorem.
Unrecognized operators, schemas, metadata, state, effects, and scopes abstain.
"""
import json

from equality_orbits import Unsupported, require
from join_prefilter_certificate import EXPR, LOGICAL, tree_from_preorder
from collapse_project_certificate import _attribute, _identity, _literal
from column_pruning_liveness_certificate import (
    EXACT_ARITIES, _aggregate_free_variables, _input_descriptors,
)

INTEGERS = {"integer", "long", "short", "byte"}
MAX_STACK_DEPTH = 8


def _literal_value(node):
    if node.get("dataType") == "void":
        require(node.get("value") is None and
                set(node) == {"class", "num-children", "dataType", "value"},
                "MALFORMED_NULL_LITERAL")
        return ("literal", "void", "null"), "void"
    atom = _literal(node)
    return atom, atom[1]


def _expression(items, scope):
    """Symbolically evaluate one accepted total projection expression."""
    require(isinstance(items, list) and items, "EMPTY_PROJECT_EXPRESSION")

    def read(offset):
        require(offset < len(items), "TRUNCATED_PROJECT_EXPRESSION")
        node = items[offset]
        require(isinstance(node, dict), "BAD_PROJECT_EXPRESSION")
        klass = node.get("class")
        require(isinstance(klass, str) and klass.startswith(EXPR),
                "UNTRUSTED_PROJECT_EXPRESSION")
        opname = klass[len(EXPR):]
        if opname == "AttributeReference":
            key, dtype, name = _attribute(node)
            require(key in scope, "PROJECT_UNBOUND_ATTRIBUTE")
            entry_type, entry_name, value = scope[key]
            require((dtype, name) == (entry_type, entry_name),
                    "PROJECT_ATTRIBUTE_SCOPE_MISMATCH")
            return value, dtype, offset + 1
        if opname == "Literal":
            value, dtype = _literal_value(node)
            return value, dtype, offset + 1
        # Integral remainder with a nonzero *literal* divisor is deterministic,
        # per-row and total for NULL as well as all integral inputs.
        require(opname == "Remainder" and
                node.get("num-children") == 2 and
                node.get("left") == 0 and node.get("right") == 1 and
                set(node) == {"class", "num-children", "left", "right"},
                "UNSAFE_OR_UNKNOWN_PROJECT_EXPRESSION")
        left, lefttype, nextpos = read(offset + 1)
        rightpos = nextpos
        require(rightpos < len(items) and
                items[rightpos].get("class") == EXPR + "Literal",
                "REMAINDER_NON_LITERAL_DIVISOR")
        right, righttype, finalpos = read(rightpos)
        require(lefttype in INTEGERS and righttype == lefttype,
                "REMAINDER_TYPE_MISMATCH")
        divisor = items[rightpos].get("value")
        require(type(divisor) is int and divisor != 0,
                "REMAINDER_DIVISOR_ZERO_OR_NONINT")
        return ("remainder", lefttype, left, right), lefttype, finalpos

    result, dtype, pos = read(0)
    require(pos == len(items), "TRAILING_PROJECT_EXPRESSION")
    return result, dtype


def _project_step(node, scope):
    require(node.get("class") == LOGICAL + "Project" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == {"class", "num-children", "child", "projectList"},
            "BAD_PROJECT_METADATA")
    items = node["projectList"]
    require(isinstance(items, list), "INVALID_PROJECT_LIST")
    outputs = {}
    for item in items:
        require(isinstance(item, list) and bool(item),
                "BAD_PROJECT_ENTRY")
        first = item[0]
        if first.get("class") == EXPR + "AttributeReference":
            require(len(item) == 1, "COMPUTED_DIRECT_ATTRIBUTE")
            output_id, dtype, name = _attribute(first)
            require(output_id in scope, "DIRECT_ATTRIBUTE_UNBOUND")
            entry_type, entry_name, value = scope[output_id]
            require((dtype, name) == (entry_type, entry_name),
                    "DIRECT_ATTRIBUTE_SCOPE_MISMATCH")
        else:
            require(first.get("class") == EXPR + "Alias" and
                    first.get("num-children") == 1 and
                    first.get("child") == 0 and
                    isinstance(first.get("name"), str) and
                    set(first) == {"class", "num-children", "child",
                                   "name", "exprId"},
                    "BAD_PROJECT_ALIAS")
            output_id = _identity(first)
            require(output_id not in scope, "ALIAS_SHADOWS_INPUT_EXPR_ID")
            name = first["name"]
            value, dtype = _expression(item[1:], scope)
        require(output_id not in outputs, "DUPLICATE_PROJECT_OUTPUT_ID")
        outputs[output_id] = (dtype, name, value)
    return outputs


def _projection_stack(tree):
    stack = []
    current = tree
    while current[0].get("class") == LOGICAL + "Project":
        node, children = current
        require(len(children) == 1 and
                len(stack) < MAX_STACK_DEPTH, "INVALID_PROJECT_STACK")
        stack.append(node)
        current = children[0]
    return stack, current


def _projection_outputs(stack, base):
    descriptors = _input_descriptors(base)
    scope = {key: (dtype, name, ("attribute", dtype, key))
             for key, (dtype, name) in descriptors.items()}
    for node in reversed(stack):
        scope = _project_step(node, scope)
    return scope


def _aggregate_references(node):
    """Reuse original aggregate validator; add total upper/lower and remainder.

    The extension only broadens the *unchanged* aggregate context grammar,
    never the accepted arbitrary computed projection expressions.
    """
    try:
        return _aggregate_free_variables(node)
    except Unsupported as original:
        arities = dict(EXACT_ARITIES, Upper=1, Lower=1, Remainder=2)
        require(node.get("class") == LOGICAL + "Aggregate" and
                node.get("num-children") == 1 and node.get("child") == 0 and
                set(node) == {"class", "num-children", "child",
                              "groupingExpressions", "aggregateExpressions"},
                "UNTRUSTED_AGGREGATE_METADATA")
        groups = node["groupingExpressions"]
        outputs = node["aggregateExpressions"]
        require(isinstance(groups, list) and isinstance(outputs, list)
                and bool(outputs), "BAD_AGGREGATE_ITEMS")

        def scan(nodes):
            require(isinstance(nodes, list) and nodes,
                    "EMPTY_AGGREGATE_EXPRESSION")
            def read(pos):
                require(pos < len(nodes), "TRUNCATED_AGGREGATE_EXPRESSION")
                expr = nodes[pos]
                require(isinstance(expr, dict) and
                        isinstance(expr.get("class"), str) and
                        expr["class"].startswith(EXPR),
                        "BAD_AGGREGATE_EXPRESSION")
                op = expr["class"][len(EXPR):]
                require(op in arities and
                        expr.get("num-children") == arities[op],
                        "UNKNOWN_AGGREGATE_EXPRESSION")
                if op == "AttributeReference":
                    key, dtype, _ = _attribute(expr)
                    return pos + 1, {(key, dtype)}
                if op == "Literal":
                    _literal_value(expr)
                    return pos + 1, set()
                if op == "aggregate.AggregateExpression":
                    require(not any(k in expr for k in
                                   ("filter", "filterExpression",
                                    "filterOption")),
                            "FILTERED_AGGREGATE_NOT_ADMITTED")
                if op == "Alias":
                    _identity(expr)
                    require(expr.get("child") == 0 and
                            isinstance(expr.get("name"), str),
                            "INVALID_AGGREGATE_ALIAS")
                cursor, refs = pos + 1, set()
                for _ in range(arities[op]):
                    cursor, found = read(cursor)
                    refs.update(found)
                return cursor, refs
            end, refs = read(0)
            require(end == len(nodes), "TRAILING_AGGREGATE_EXPRESSION")
            return refs

        refs = set()
        for item in [*groups, *outputs]:
            refs.update(scan(item))
        return refs


def _certify(event):
    before = tree_from_preorder(event["old"])
    after = tree_from_preorder(event["new"])
    require(before != after, "NO_TRANSITION")
    sort_frames = 0
    while before[0].get("class") == LOGICAL + "Sort":
        node, children = before
        counterpart, new_children = after
        require(node == counterpart and
                node.get("num-children") == 1 and node.get("child") == 0
                and len(children) == len(new_children) == 1,
                "SORT_CONTEXT_CHANGED")
        before, after = children[0], new_children[0]
        sort_frames += 1
    aggregate, old_children = before
    new_aggregate, new_children = after
    require(aggregate == new_aggregate and
            aggregate.get("class") == LOGICAL + "Aggregate" and
            len(old_children) == len(new_children) == 1,
            "AGGREGATE_CONTEXT_CHANGED")
    refs = _aggregate_references(aggregate)
    original_stack, original_base = _projection_stack(old_children[0])
    optimized_stack, optimized_base = _projection_stack(new_children[0])
    require(original_base == optimized_base, "PROJECT_BASE_CHANGED")
    require(original_stack or optimized_stack, "NO_PROJECT_FRAMES")
    original = _projection_outputs(original_stack, original_base)
    optimized = _projection_outputs(optimized_stack, optimized_base)
    require(original != optimized or len(original_stack) != len(optimized_stack),
            "NO_OBSERVABLE_STRUCTURE_CHANGE")
    for key, dtype in refs:
        require(key in original and key in optimized,
                "LIVE_ATTRIBUTE_MISSING")
        origtype, origname, origvalue = original[key]
        newtype, newname, newvalue = optimized[key]
        require(origtype == newtype == dtype and
                origname == newname and origvalue == newvalue,
                "LIVE_ATTRIBUTE_SEMANTICS_CHANGED")
    return {"status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "compositional_project_observational_liveness",
            "live_attributes": len(refs),
            "old_project_depth": len(original_stack),
            "new_project_depth": len(optimized_stack),
            "old_output_width": len(original),
            "new_output_width": len(optimized),
            "sort_frames": sort_frames,
            "semantic_law":
                "C(F(R)) == C(G(R)) as bags when F|live == G|live"}


def certify_generalized_projection_liveness_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
