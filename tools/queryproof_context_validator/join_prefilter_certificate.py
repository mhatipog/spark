"""Sound local certificate for redundant filters introduced beneath INNER JOIN.

Only entire-root, resolved, binary INNER JOIN plan transitions with unchanged join
metadata and children differing solely by new unary Filter wrappers are in scope.
For any pair of input rows, if C(join pair) is TRUE then every inserted side
guard must be TRUE. Exhaustive equality/NULL orbits establish this implication
for the no-literals nullable-integral grammar, independently checked by SQLite.

This certificate is exploratory on the 2026-10-18 holdout, NOT part of its
preregistered primary metric. Test on a newly frozen suite before claiming
out-of-sample coverage.
"""
import json

from equality_orbits import (
    Unsupported, attributes, classify_trees, parse_expression, require,
    sqlite_validate, validate_plan_tree,
)

LOGICAL = "org.apache.spark.sql.catalyst.plans.logical."
EXPR = "org.apache.spark.sql.catalyst.expressions."
INNER = {"object": "org.apache.spark.sql.catalyst.plans.Inner$"}


def tree_from_preorder(serialized):
    validate_plan_tree(serialized)

    def read(offset):
        node = serialized[offset]
        position = offset + 1
        children = []
        for _ in range(node["num-children"]):
            child, position = read(position)
            children.append(child)
        return (node, tuple(children)), position

    root, end = read(0)
    require(end == len(serialized), "BAD_PLAN_STREAM")
    return root


def project_output_expr_ids(subtree):
    node, children = subtree
    require(node.get("class") == LOGICAL + "Project" and len(children) == 1,
            "GUARDED_CHILD_NOT_DIRECT_PROJECT")
    outputs = node.get("projectList")
    require(isinstance(outputs, list) and bool(outputs), "MISSING_PROJECT_LIST")
    ids = set()
    for item in outputs:
        require(isinstance(item, list) and bool(item), "UNREADABLE_PROJECT_ENTRY")
        top = item[0]
        require(isinstance(top, dict)
                and top.get("class") in (EXPR + "Alias", EXPR + "AttributeReference"),
                "UNSUPPORTED_PROJECT_OUTPUT")
        identifier = top.get("exprId")
        require(isinstance(identifier, dict) and "id" in identifier,
                "PROJECT_OUTPUT_MISSING_ID")
        ids.add(json.dumps(identifier, sort_keys=True))
    return ids


def guard_attribute_expr_ids(expr):
    return {
        json.dumps(json.loads(key)["id"], sort_keys=True)
        for key in attributes(expr)
    }


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    old_root, old_children = old
    new_root, new_children = new
    require(old_root.get("class") == LOGICAL + "Join"
            and new_root.get("class") == LOGICAL + "Join",
            "NOT_ROOT_JOIN")
    require(len(old_children) == len(new_children) == 2,
            "NOT_BINARY_JOIN")
    require(old_root == new_root, "JOIN_METADATA_CHANGED")
    require(old_root.get("joinType") == INNER, "NOT_EXACT_INNER_JOIN")
    join_condition = parse_expression(old_root.get("condition"))
    inserted_guards = []
    guarded_sides = []
    for side, (before, after) in enumerate(zip(old_children, new_children)):
        if before == after:
            continue
        node, children = after
        require(node.get("class") == LOGICAL + "Filter"
                and node.get("num-children") == 1 and len(children) == 1,
                "SIDE_CHANGE_NOT_INSERTED_FILTER")
        require(set(node) == {"class", "num-children", "condition"},
                "EXTRA_FILTER_METADATA")
        require(children[0] == before, "CHILD_NOT_IDENTICAL")
        guard = parse_expression(node["condition"])
        available = project_output_expr_ids(before)
        require(guard_attribute_expr_ids(guard) <= available,
                "GUARD_NOT_SCOPED_TO_CHILD_OUTPUT")
        inserted_guards.append(guard)
        guarded_sides.append(side)
    require(inserted_guards, "NO_NEW_FILTERS")
    combined = join_condition
    for guard in inserted_guards:
        combined = ("And", combined, guard)
    proof = classify_trees(join_condition, combined, "TRUE_ONLY",
                           allow_remap=True)
    require(proof["status"] == "EQUIVALENT_IN_FRAGMENT",
            "JOIN_CONDITION_DOES_NOT_IMPLY_GUARDS")
    independent = sqlite_validate(join_condition, combined, proof)
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "root_inner_join_implied_side_filter_insertion",
        "guarded_sides": guarded_sides,
        "input_attributes": proof["input_attributes"],
        "orbits_checked": proof["orbits_checked"],
        "sqlite_verified": independent["verified"],
        "semantic_law": "C=TRUE implies every inserted guard=TRUE",
    }


def certify_inner_join_prefilter_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as error:
        return {"status": "UNKNOWN", "reason": str(error)}
