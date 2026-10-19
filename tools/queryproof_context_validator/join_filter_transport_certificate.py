"""Exact full-predicate transport across a binary INNER JOIN.

Old: Filter(P, Join_Inner(C, L, R))
New: Join_Inner(C', Filter(G_L, L), Filter(G_R, R))

The exact NULL/equality fragment establishes per-row-pair equivalence:
  [C == TRUE and P == TRUE] iff
  [C' == TRUE and each inserted side G == TRUE].
The SQL bag multiplicity of a pair is m_L(l)*m_R(r) times this Boolean.
Either G may be absent. All metadata, input subtrees, output IDs and scopes
must be identical, otherwise UNKNOWN. No Catalyst rule name is trusted.
"""
from equality_orbits import (
    Unsupported, attributes, classify_trees, parse_expression, require,
    sqlite_validate,
)
from join_prefilter_certificate import tree_from_preorder, LOGICAL, INNER
from project_pushdown_certificate import project_substitution

FILTER_KEYS = {"class", "num-children", "condition", "child"}


def _parse_filter(node):
    require(node.get("class") == LOGICAL + "Filter" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == FILTER_KEYS, "NOT_EXACT_FILTER")
    return parse_expression(node["condition"])


def _certify(event):
    old, new = tree_from_preorder(event["old"]), tree_from_preorder(event["new"])
    old_filter, old_filter_children = old
    new_join, new_children = new
    original_where = _parse_filter(old_filter)
    require(len(old_filter_children) == 1, "NOT_FILTER_OVER_JOIN")
    old_join, old_children = old_filter_children[0]
    require(old_join.get("class") == new_join.get("class") ==
            LOGICAL + "Join", "NOT_EXACT_JOIN_UNDER_FILTER")
    require(old_join.get("num-children") == new_join.get("num-children") == 2
            and old_join.get("left") == new_join.get("left") == 0
            and old_join.get("right") == new_join.get("right") == 1
            and len(old_children) == len(new_children) == 2,
            "NOT_BINARY_INNER_FRAME")
    require(old_join.get("joinType") == new_join.get("joinType") == INNER,
            "NOT_INNER_JOIN")
    require({k: v for k, v in old_join.items() if k != "condition"} ==
            {k: v for k, v in new_join.items() if k != "condition"},
            "JOIN_METADATA_CHANGED")
    condition_before = parse_expression(old_join.get("condition"))
    condition_after = parse_expression(new_join.get("condition"))
    scopes = []
    for child in old_children:
        node, grand = child
        require(node.get("class") == LOGICAL + "Project" and len(grand) == 1,
                "INPUT_NOT_DIRECT_PROJECT")
        scopes.append(set(project_substitution(node)))
    require(scopes[0].isdisjoint(scopes[1]), "OVERLAPPING_JOIN_SCOPES")
    total = scopes[0] | scopes[1]
    require(attributes(original_where) <= total and
            attributes(condition_before) <= total and
            attributes(condition_after) <= total,
            "PREDICATES_OUTSIDE_JOIN_OUTPUT")
    new_guards = []
    for side, (before, after) in enumerate(zip(old_children, new_children)):
        if before == after:
            continue
        wrapper, kids = after
        require(len(kids) == 1 and kids[0] == before,
                "JOIN_CHILD_CHANGED")
        guard = _parse_filter(wrapper)
        require(attributes(guard) <= scopes[side],
                "PUSHED_GUARD_WRONG_SIDE")
        new_guards.append(guard)
    original = ("And", condition_before, original_where)
    candidate = condition_after
    for guard in new_guards:
        candidate = ("And", candidate, guard)
    proof = classify_trees(original, candidate, "TRUE_ONLY",
                           allow_remap=True)
    require(proof["status"] == "EQUIVALENT_IN_FRAGMENT",
            "JOIN_FILTER_TRANSPORT_NOT_EQUIVALENT")
    check = sqlite_validate(original, candidate, proof)
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "exact_inner_join_selection_filter_transport",
        "side_guards": len(new_guards),
        "input_attributes": proof["input_attributes"],
        "orbits_checked": proof["orbits_checked"],
        "sqlite_verified": check["verified"],
        "semantic_law": "C∧P and C'∧all(G) select identical joined row pairs"}


def certify_inner_join_filter_transport(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as error:
        return {"status": "UNKNOWN", "reason": str(error)}
