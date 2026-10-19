"""Exact local certificate for Filter/Project commutation.

Scope: resolved Catalyst plans, pure identity/attribute-alias projections over
nullable integral columns, predicates in the existing equality/NULL grammar.
For each input row r, Filter_P(Project_E(r)) equals
Project_E(Filter_(P composed with E)(r)), with row multiplicities retained.
An unchanged INNER JOIN may frame one such rewrite independently on each side.
Outside this fragment the checker abstains (UNKNOWN), never guesses.
"""
import json

from equality_orbits import (
    Unsupported, attributes, classify_trees, parse_expression, require,
    sqlite_validate,
)
from join_prefilter_certificate import tree_from_preorder

LOGICAL = "org.apache.spark.sql.catalyst.plans.logical."
EXPR = "org.apache.spark.sql.catalyst.expressions."
INNER = {"object": "org.apache.spark.sql.catalyst.plans.Inner$"}
INTEGER = {"integer", "long", "short", "byte"}


def attr_key(node):
    require(isinstance(node, dict) and
            node.get("class") == EXPR + "AttributeReference" and
            node.get("num-children") == 0, "NOT_DIRECT_ATTRIBUTE")
    dtype = node.get("dataType")
    require(dtype in INTEGER, "PROJECT_ATTRIBUTE_NOT_INTEGRAL")
    eid = node.get("exprId")
    require(isinstance(eid, dict) and
            type(eid.get("id")) is int and
            isinstance(eid.get("jvmId"), str),
            "INVALID_PROJECT_ATTRIBUTE_ID")
    return json.dumps({"id": eid, "type": dtype}, sort_keys=True)


def project_substitution(node):
    require(node.get("class") == LOGICAL + "Project" and
            node.get("num-children") == 1 and node.get("child") == 0,
            "NOT_VALID_PROJECT")
    items = node.get("projectList")
    require(isinstance(items, list) and bool(items), "MISSING_PROJECT_ITEMS")
    mapping = {}
    for item in items:
        require(isinstance(item, list) and len(item) in (1, 2),
                "PROJECT_EXPRESSION_NOT_PASSTHROUGH")
        head = item[0]
        if len(item) == 1:
            source = attr_key(head)
            output = source
        else:
            require(isinstance(head, dict) and
                    head.get("class") == EXPR + "Alias" and
                    head.get("num-children") == 1 and
                    head.get("child", 0) == 0,
                    "PROJECT_NOT_DIRECT_ALIAS")
            source = attr_key(item[1])
            eid = head.get("exprId")
            require(isinstance(eid, dict) and
                    type(eid.get("id")) is int and
                    isinstance(eid.get("jvmId"), str),
                    "INVALID_ALIAS_OUTPUT_ID")
            output = json.dumps({"id": eid,
                                 "type": json.loads(source)["type"]},
                                sort_keys=True)
        require(output not in mapping, "DUPLICATE_PROJECT_OUTPUT_ID")
        mapping[output] = source
    return mapping


def substitute(tree, mapping):
    if tree[0] == "attr":
        require(tree[1] in mapping, "PREDICATE_OUTSIDE_PROJECT_OUTPUT")
        return ("attr", mapping[tree[1]])
    return (tree[0], *(substitute(child, mapping)
                       for child in tree[1:]))


def filter_node(node):
    require(node.get("class") == LOGICAL + "Filter" and
            node.get("num-children") == 1 and
            node.get("child") == 0 and
            set(node) == {"class", "num-children", "condition", "child"},
            "NOT_VALID_UNARY_FILTER")
    return parse_expression(node["condition"])


def certify_branch(before, after):
    """Precisely Filter(Project(R)) -> Project(Filter(R))."""
    filt, old_children = before
    project, project_children = after
    require(len(old_children) == len(project_children) == 1,
            "NOT_UNARY_SWAP")
    old_project, old_grandchildren = old_children[0]
    require(old_project == project and len(old_grandchildren) == 1,
            "PROJECT_CHANGED_DURING_SWAP")
    new_filter, new_children = project_children[0]
    require(len(new_children) == 1 and
            new_children[0] == old_grandchildren[0],
            "PROJECT_INPUT_CHANGED")
    old_pred = filter_node(filt)
    new_pred = filter_node(new_filter)
    mapping = project_substitution(project)
    translated = substitute(old_pred, mapping)
    require(attributes(new_pred) <= set(mapping.values()),
            "PUSHED_PREDICATE_OUTSIDE_PROJECT_INPUT")
    proof = classify_trees(translated, new_pred, "TRUE_ONLY",
                           allow_remap=True)
    require(proof["status"] == "EQUIVALENT_IN_FRAGMENT",
            "PUSHED_FILTER_NOT_EQUIVALENT")
    check = sqlite_validate(translated, new_pred, proof)
    return {"input_attributes": proof["input_attributes"],
            "orbits_checked": proof["orbits_checked"],
            "sqlite_verified": check["verified"],
            "projected_columns": len(mapping),
            "aliases": sum(k != v for k, v in mapping.items())}


def _certify(event):
    old, new = tree_from_preorder(event["old"]), tree_from_preorder(event["new"])
    a, achildren = old
    b, bchildren = new
    branches = []
    if (a.get("class") == LOGICAL + "Filter" and
            b.get("class") == LOGICAL + "Project"):
        branches.append(certify_branch(old, new))
        frame = "root_filter_project"
    else:
        require(a.get("class") == b.get("class") == LOGICAL + "Join",
                "NOT_DIRECT_FILTER_PROJECT_OR_JOIN")
        require(a == b and a.get("num-children") == 2 and
                a.get("left") == 0 and a.get("right") == 1,
                "JOIN_FRAME_CHANGED")
        require(a.get("joinType") == INNER, "NOT_EXACT_INNER_JOIN")
        require(len(achildren) == len(bchildren) == 2,
                "NOT_BINARY_JOIN")
        for before, after in zip(achildren, bchildren):
            if before != after:
                branches.append(certify_branch(before, after))
        frame = "unchanged_inner_join"
    require(branches, "NO_CERTIFIABLE_SWAP")
    return {"status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "identity_or_alias_project_filter_commutation",
            "frame": frame,
            "rewritten_branches": len(branches),
            "input_attributes_checked_per_branch":
                [x["input_attributes"] for x in branches],
            "orbits_checked_per_branch": [x["orbits_checked"] for x in branches],
            "aliases_per_branch": [x["aliases"] for x in branches],
            "sqlite_verified": all(x["sqlite_verified"] for x in branches),
            "semantic_law": "Filter(P,Project(E,R))=Project(E,Filter(P∘E,R)) "
                            "as row bags for pure passthrough/alias E"}


def certify_project_filter_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as error:
        return {"status": "UNKNOWN", "reason": str(error)}
