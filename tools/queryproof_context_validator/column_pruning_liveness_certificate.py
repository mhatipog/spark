"""Exact column-liveness certificate for a narrow ColumnPruning rewrite.

  Aggregate(R) -> Aggregate(Project(S, R))

The aggregate and optional Sort ancestors are byte-identical. The added
Project merely forwards a subset S of the original child attributes, in any
order, including an empty list. Every attribute referenced in the unchanged
Aggregate grouping or result expressions must be in S.

Under *defined* bag-relational semantics, projection preserves each input
row's multiplicity, including when S is empty. Therefore a pure Aggregate
whose free variables are contained in S produces the same result row bag.
Sort provides only bag equivalence. All other contexts abstain.

This fragment's aggregates have explicitly whitelisted pure expression
classes. We claim neither physical order, runtime error behavior, optimizer
cost, nor complete Catalyst metadata equality.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import LOGICAL, EXPR, tree_from_preorder
from collapse_project_certificate import _attribute, _identity, _literal

EXACT_ARITIES = {
    "Alias": 1,
    "AttributeReference": 0,
    "Literal": 0,
    "aggregate.AggregateExpression": 1,
    "aggregate.Sum": 1,
    "aggregate.Count": 1,
    "aggregate.Average": 1,
    "Add": 2,
    "Multiply": 2,
    "Cast": 1,
    "Length": 1,
}
LEAVES = {"org.apache.spark.sql.execution.LogicalRDD",
          LOGICAL + "LocalRelation"}


def _input_descriptors(tree):
    """Only leaves and unchanged Filters: computed inputs abstain."""
    node, children = tree
    klass = node.get("class")
    if klass == LOGICAL + "Filter":
        require(node.get("num-children") == 1 and node.get("child") == 0
                and len(children) == 1, "INVALID_UNCHANGED_FILTER")
        return _input_descriptors(children[0])
    require(klass in LEAVES and node.get("num-children") == 0
            and not children, "UNSUPPORTED_INPUT_SCHEMA")
    entries = node.get("output")
    require(isinstance(entries, list) and entries,
            "MISSING_INPUT_OUTPUT")
    descriptors = {}
    for entry in entries:
        require(isinstance(entry, list) and len(entry) == 1,
                "BAD_LEAF_ATTRIBUTE")
        key, dtype, name = _attribute(entry[0])
        require(isinstance(name, str) and key not in descriptors,
                "UNNAMED_OR_DUPLICATE_INPUT")
        descriptors[key] = (dtype, name)
    return descriptors


def _referenced_attributes(preorder):
    """Parse every expression-tree child, fail closed on unknown operators."""
    require(isinstance(preorder, list) and preorder, "EMPTY_EXPRESSION")

    def read(pos):
        require(pos < len(preorder), "TRUNCATED_EXPRESSION")
        node = preorder[pos]
        require(isinstance(node, dict), "BAD_EXPRESSION_NODE")
        klass = node.get("class")
        require(isinstance(klass, str) and klass.startswith(EXPR),
                "UNKNOWN_EXPRESSION_NAMESPACE")
        name = klass[len(EXPR):]
        require(name in EXACT_ARITIES and
                node.get("num-children") == EXACT_ARITIES[name],
                "UNKNOWN_EXPRESSION_CLASS_OR_ARITY")
        if name == "AttributeReference":
            key, dtype, _ = _attribute(node)
            return pos + 1, {(key, dtype)}
        if name == "Literal":
            if node.get("dataType") == "void":
                require("value" in node and node["value"] is None,
                        "MALFORMED_NULL_LITERAL")
            else:
                _literal(node)
            return pos + 1, set()
        # A filtered AggregateExpression or nondeterministic extension is
        # not in the admitted grammar; arity guards exclude extra children.
        if name == "aggregate.AggregateExpression":
            require(not any(k in node for k in
                            ("filter", "filterExpression", "filterOption")),
                    "FILTERED_AGGREGATE_NOT_ADMITTED")
        refs = set()
        cursor = pos + 1
        for _ in range(EXACT_ARITIES[name]):
            cursor, child_refs = read(cursor)
            refs.update(child_refs)
        if name == "Alias":
            require(node.get("child") == 0
                    and isinstance(node.get("name"), str), "BAD_ALIAS")
            _identity(node)
        return cursor, refs

    end, refs = read(0)
    require(end == len(preorder), "TRAILING_EXPRESSION_NODES")
    return refs


def _aggregate_free_variables(node):
    require(node.get("class") == LOGICAL + "Aggregate"
            and node.get("num-children") == 1
            and node.get("child") == 0
            and set(node) == {"class", "num-children", "child",
                              "groupingExpressions", "aggregateExpressions"},
            "UNTRUSTED_AGGREGATE_METADATA")
    groups = node.get("groupingExpressions")
    outputs = node.get("aggregateExpressions")
    require(isinstance(groups, list) and isinstance(outputs, list)
            and bool(outputs), "BAD_AGGREGATE_ITEMS")
    refs = set()
    for expr in [*groups, *outputs]:
        refs.update(_referenced_attributes(expr))
    return refs


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    sort_frames = 0
    while old[0].get("class") == LOGICAL + "Sort":
        node, children = old
        counterpart, new_children = new
        require(node == counterpart and node.get("num-children") == 1
                and node.get("child") == 0 and
                len(children) == len(new_children) == 1,
                "SORT_FRAME_CHANGED")
        old, new = children[0], new_children[0]
        sort_frames += 1
    aggregate, children = old
    new_aggregate, new_children = new
    require(aggregate == new_aggregate
            and aggregate.get("class") == LOGICAL + "Aggregate"
            and len(children) == len(new_children) == 1,
            "AGGREGATE_CHANGED")
    old_child = children[0]
    projection, projection_children = new_children[0]
    require(projection.get("class") == LOGICAL + "Project"
            and projection.get("num-children") == 1
            and projection.get("child") == 0
            and set(projection) == {"class", "num-children", "child",
                                    "projectList"}
            and len(projection_children) == 1
            and old_child == projection_children[0],
            "NOT_EXACT_INSERTED_PRUNING_PROJECT")

    descriptors = _input_descriptors(old_child)
    project_entries = projection["projectList"]
    require(isinstance(project_entries, list),
            "BAD_PRUNING_PROJECT_LIST")
    kept = {}
    for item in project_entries:
        require(isinstance(item, list) and len(item) == 1,
                "COMPUTED_PRUNING_PROJECT")
        key, dtype, name = _attribute(item[0])
        require(key in descriptors and descriptors[key] == (dtype, name)
                and key not in kept, "PRUNED_ATTRIBUTE_MISMATCH")
        kept[key] = dtype
    require(len(kept) < len(descriptors), "NOT_ACTUAL_PRUNING")
    refs = _aggregate_free_variables(aggregate)
    for key, dtype in refs:
        require(kept.get(key) == dtype, "AGGREGATE_READS_DROPPED_COLUMN")
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "column_liveness_under_pure_aggregate",
        "kept_columns": len(kept),
        "dropped_columns": len(descriptors) - len(kept),
        "aggregate_referenced_columns": len({key for key, _ in refs}),
        "sort_frames": sort_frames,
        "semantic_law": "Agg(R) == Agg(Project(live_columns, R)) as row bags",
        "empty_projection_supported": len(kept) == 0,
    }


def certify_column_pruning_liveness_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
