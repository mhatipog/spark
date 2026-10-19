"""Sound identity-Project removal under pure Aggregate and Sort frames.

For each *defined* row sequence of T, Project(T.output attributes)(T)
forwards precisely the same rows, types, identifiers and order. Applying the
same checked Aggregate/Sort contexts above both plans preserves output
row bags. Sort is certified only as a bag-preserving permutation.
The verified Aggregate grammar only allows grouped attributes,
attribute aliases and integral SUM; it rejects volatile/UDF expressions.

This is a separate, narrower theorem from general projection composition.
An untouched Add or Remainder in T is NEVER inlined or re-evaluated.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import tree_from_preorder, LOGICAL, EXPR
from collapse_project_certificate import (
    _attribute, _identity, _literal, _input_scope,
)

INTEGRAL = {"integer", "long", "short", "byte"}


def _source_name(head, dtype):
    name = head.get("name")
    require(isinstance(name, str) and bool(name), "MISSING_RESOLVED_OUTPUT_NAME")
    return _identity(head), dtype, name


def _retained_project_output(tree):
    """Read only stable output identities/types, never evaluate child expressions."""
    node, children = tree
    require(node.get("class") == LOGICAL + "Project" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == {"class", "num-children", "projectList", "child"} and
            len(children) == 1, "RETAINED_CHILD_NOT_PROJECT")
    scope = _input_scope(children[0])
    outputs = []
    for item in node.get("projectList", []):
        require(isinstance(item, list) and item, "BAD_RETAINED_OUTPUT")
        if len(item) == 1:
            key, dtype, name = _attribute(item[0])
            require(key in scope and scope[key] == dtype,
                    "RETAINED_DIRECT_INPUT_UNSCOPED")
            require(isinstance(name, str), "UNNAMED_DIRECT_OUTPUT")
            out = (key, dtype, name)
        else:
            head = item[0]
            require(head.get("class") == EXPR + "Alias" and
                    head.get("num-children") == 1 and
                    head.get("child") == 0, "RETAINED_OUTPUT_NOT_ALIAS")
            if len(item) == 2:
                target = item[1]
                if target.get("class") == EXPR + "AttributeReference":
                    key, dtype, _ = _attribute(target)
                    require(key in scope and scope[key] == dtype,
                            "RETAINED_ALIAS_UNSCOPED")
                else:
                    dtype = _literal(target)[1]
            else:
                require(len(item) == 4 and
                        item[1].get("class") in (EXPR + "Add", EXPR + "Remainder") and
                        item[1].get("num-children") == 2,
                        "UNKNOWN_RETAINED_EXPRESSION")
                key, dtype, _ = _attribute(item[2])
                require(key in scope and scope[key] == dtype and
                        dtype in {"integer", "long"}, "RETAINED_ARITHMETIC_LEFT_NOT_INTEGRAL_SCOPED")
                ltype = _literal(item[3])[1]
                require(ltype == dtype, "RETAINED_ARITHMETIC_OPERAND_TYPE_MISMATCH")
                # Spark same-type integer Add/Remainder retain their integral type.
                # The Add subtree itself remains byte-for-byte unchanged.
            out = _source_name(head, dtype)
        outputs.append(out)
    require(outputs, "EMPTY_RETAINED_PROJECTION")
    require(len({row[0] for row in outputs}) == len(outputs),
            "DUPLICATE_RETAINED_EXPR_ID")
    return outputs


def _aggregate_scope(node, source):
    require(node.get("class") == LOGICAL + "Aggregate" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == {"class", "num-children", "child",
                          "groupingExpressions", "aggregateExpressions"},
            "UNTRUSTED_AGGREGATE_FRAME_METADATA")
    groups = node["groupingExpressions"]
    results = node["aggregateExpressions"]
    require(isinstance(groups, list) and isinstance(results, list) and
            bool(results), "INVALID_AGGREGATE_EXPRESSIONS")
    for group in groups:
        require(isinstance(group, list) and len(group) == 1,
                "COMPLEX_GROUPING_EXPRESSION")
        key, dtype, _ = _attribute(group[0])
        require(source.get(key) == dtype, "GROUP_ATTRIBUTE_OUT_OF_SCOPE")
    output = {}
    for entry in results:
        require(isinstance(entry, list) and
                len(entry) in (1, 2, 4), "COMPLEX_AGGREGATE_EXPRESSION")
        if len(entry) == 1:
            key, dtype, _ = _attribute(entry[0])
            require(source.get(key) == dtype, "AGGREGATE_DIRECT_OUT_OF_SCOPE")
            out_id = key
        else:
            alias = entry[0]
            require(alias.get("class") == EXPR + "Alias" and
                    alias.get("num-children") == 1 and
                    alias.get("child") == 0 and
                    isinstance(alias.get("name"), str), "AGGREGATE_NOT_ALIAS")
            out_id = _identity(alias)
            if len(entry) == 2:
                key, dtype, _ = _attribute(entry[1])
                require(source.get(key) == dtype,
                        "AGGREGATE_ALIAS_SOURCE_OUT_OF_SCOPE")
            else:
                ae, fun, attr = entry[1:]
                require(ae.get("class") == EXPR + "aggregate.AggregateExpression" and
                        ae.get("num-children") == 1 and
                        fun.get("class") == EXPR + "aggregate.Sum" and
                        fun.get("num-children") == 1,
                        "UNTRUSTED_AGGREGATE_FUNCTION")
                key, source_type, _ = _attribute(attr)
                require(source.get(key) == source_type and
                        source_type in INTEGRAL,
                        "SUM_SOURCE_NOT_SCOPED_INTEGRAL")
                dtype = "long"  # Catalyst Sum of nullable 32-bit integers is Long.
        require(out_id not in output, "DUPLICATE_AGGREGATE_OUTPUT_ID")
        output[out_id] = dtype
    return output


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    frames = []
    while old[0].get("class") in (LOGICAL + "Sort", LOGICAL + "Aggregate"):
        node, children = old
        counterpart, new_children = new
        require(node == counterpart and node.get("num-children") == 1 and
                node.get("child") == 0 and
                len(children) == len(new_children) == 1,
                "CONTEXT_FRAME_CHANGED")
        frames.append((node.get("class").split(".")[-1], node))
        old, new = children[0], new_children[0]
    require(any(kind == "Aggregate" for kind, _ in frames),
            "NO_AGGREGATE_FRAMES")
    identity, identity_children = old
    require(identity.get("class") == LOGICAL + "Project" and
            identity.get("num-children") == 1 and
            identity.get("child") == 0 and
            set(identity) == {"class", "num-children", "projectList", "child"}
            and len(identity_children) == 1,
            "NOT_REMOVED_IDENTITY_PROJECT")
    require(identity_children[0] == new, "RETAINED_CHILD_CHANGED")
    retained_output = _retained_project_output(new)
    forwarded = identity["projectList"]
    require(isinstance(forwarded, list) and
            len(forwarded) == len(retained_output), "IDENTITY_ARITY_CHANGED")
    for expression, (key, dtype, name) in zip(forwarded, retained_output):
        require(isinstance(expression, list) and len(expression) == 1,
                "OUTER_PROJECT_NOT_DIRECT_FORWARD")
        observed = _attribute(expression[0])
        require(observed == (key, dtype, name),
                "NOT_IDENTITY_IN_OUTPUT_ORDER")
    scope = {key: dtype for key, dtype, _ in retained_output}
    for kind, frame in reversed(frames):
        if kind == "Aggregate":
            scope = _aggregate_scope(frame, scope)
        else:
            require(kind == "Sort", "UNEXPECTED_CONTEXT_FRAME")
            # An unchanged Sort is a row-bag permutation, not an ordered
            # equivalence certificate. Subsequent operators see equal bags.
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "identity_project_removed_under_pure_integer_aggregates",
        "aggregate_frames": sum(kind == "Aggregate" for kind, _ in frames),
        "sort_frames": sum(kind == "Sort" for kind, _ in frames),
        "forwarded_columns": len(retained_output),
        "semantic_law": "C(identity(T)) == C(T) as row bags for pure contexts",
        "untouched_child_expressions": True}


def certify_aggregate_framed_identity_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
