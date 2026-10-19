"""Bag-sound INNER JOIN guard insertion under fixed row-local Project/Sort frames.

A new *exploratory* theorem developed after external cohort X01-X18 revealed
non-integer direct projections and Literal aliases. It must NOT retroactively
be counted as prospective evidence on that already-seen cohort.

If C is TRUE on an INNER JOIN row pair, and C entails inserted side guards,
guards cannot remove an output pair. All unaffected leaf bags (and duplicate
multiplicities) remain unchanged. Any identical deterministic row-local
Project maps equal bags to equal bags; Sort only permutes an unordered bag.

We admit only AttributeReference, Alias(AttributeReference), Alias(Literal)
frames, plus identical Sort frames; only scoped integer equality/NULL guards,
only exact INNER joins, and only explicit resolved leaf output attribute IDs.
"""
import json

from equality_orbits import (
    Unsupported, require, parse_expression, attributes, classify_trees,
    sqlite_validate,
)
from join_prefilter_certificate import (
    tree_from_preorder, guard_attribute_expr_ids, LOGICAL, EXPR, INNER,
)

ALLOWED_LEAVES = {
    "org.apache.spark.sql.execution.LogicalRDD",
    LOGICAL + "LocalRelation",
}
SCOPED_TYPES = {"integer", "long", "short", "byte", "string", "boolean",
                "double", "float", "decimal", "date", "timestamp"}


def eid(node):
    require(isinstance(node, dict) and
            node.get("class") == EXPR + "AttributeReference" and
            node.get("num-children") == 0, "NOT_SCOPED_ATTRIBUTE")
    ty = node.get("dataType")
    require(isinstance(ty, str) and ty in SCOPED_TYPES, "UNSUPPORTED_FRAME_DATA_TYPE")
    key = node.get("exprId")
    require(isinstance(key, dict) and type(key.get("id")) is int and
            isinstance(key.get("jvmId"), str), "BAD_FRAME_EXPR_ID")
    return json.dumps(key, sort_keys=True)


def named_project(node, input_ids):
    """Prove every output expression is a fixed, total row-local projection."""
    require(node.get("class") == LOGICAL + "Project" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == {"class", "num-children", "child", "projectList"},
            "BAD_PROJECT_FRAME")
    entries = node.get("projectList")
    require(isinstance(entries, list) and entries, "EMPTY_FRAME_PROJECT")
    outputs = set()
    constants = 0
    for item in entries:
        require(isinstance(item, list) and item, "EMPTY_PROJECT_ITEM")
        if len(item) == 1:
            out = eid(item[0])
            require(out in input_ids, "FRAME_ATTRIBUTE_OUT_OF_SCOPE")
        else:
            require(len(item) == 2, "NOT_ROW_LOCAL_PROJECT_ITEM")
            alias, target = item
            require(alias.get("class") == EXPR + "Alias" and
                    alias.get("num-children") == 1 and
                    alias.get("child") == 0, "NOT_DIRECT_ALIAS")
            alias_id = alias.get("exprId")
            require(isinstance(alias_id, dict) and
                    type(alias_id.get("id")) is int and
                    isinstance(alias_id.get("jvmId"), str),
                    "BAD_ALIAS_OUTPUT_ID")
            out = json.dumps(alias_id, sort_keys=True)
            if target.get("class") == EXPR + "AttributeReference":
                require(eid(target) in input_ids, "FRAME_ALIAS_TARGET_OUT_OF_SCOPE")
            else:
                require(target.get("class") == EXPR + "Literal" and
                        target.get("num-children") == 0,
                        "UNSUPPORTED_NONDETERMINISTIC_PROJECT")
                constants += 1
        require(out not in outputs, "DUPLICATE_FRAME_OUTPUT_ID")
        outputs.add(out)
    return outputs, constants


def leaf_outputs(tree):
    node, children = tree
    if node.get("class") == LOGICAL + "Filter":
        require(len(children) == 1 and node.get("child") == 0,
                "INVALID_EXISTING_FILTER")
        return leaf_outputs(children[0])
    if node.get("class") == LOGICAL + "Project":
        require(len(children) == 1, "BAD_CHILD_PROJECT")
        source = leaf_outputs(children[0])
        return named_project(node, source)[0]
    require(node.get("class") in ALLOWED_LEAVES and len(children) == 0 and
            node.get("num-children") == 0, "UNKNOWN_LEAF_SCHEMA")
    output = node.get("output")
    require(isinstance(output, list) and output, "NO_RESOLVED_LEAF_OUTPUT")
    ids = set()
    for item in output:
        require(isinstance(item, list) and len(item) == 1, "BAD_LEAF_OUTPUT_ENTRY")
        identifier = eid(item[0])
        require(identifier not in ids, "DUPLICATE_LEAF_OUTPUT_ID")
        ids.add(identifier)
    return ids


def core_certificate(old, new):
    a, ac = old
    b, bc = new
    require(a.get("class") == b.get("class") == LOGICAL + "Join" and
            len(ac) == len(bc) == 2, "NOT_CORE_JOIN")
    require(a == b and a.get("num-children") == 2 and
            a.get("left") == 0 and a.get("right") == 1,
            "JOIN_FRAME_OR_METADATA_CHANGED")
    require(a.get("joinType") == INNER, "JOIN_NOT_EXACTLY_INNER")
    join_predicate = parse_expression(a.get("condition"))
    left_ids = leaf_outputs(ac[0])
    right_ids = leaf_outputs(ac[1])
    require(left_ids.isdisjoint(right_ids), "JOIN_CHILD_EXPR_IDS_OVERLAP")
    require(guard_attribute_expr_ids(join_predicate) <= left_ids | right_ids,
            "JOIN_CONDITION_OUT_OF_SCOPE")
    joined_output = left_ids | right_ids
    inserted, sides = [], []
    for side, (before, after) in enumerate(zip(ac, bc)):
        if before == after:
            continue
        f, children = after
        require(f.get("class") == LOGICAL + "Filter" and
                f.get("num-children") == 1 and
                f.get("child") == 0 and
                set(f) == {"class", "num-children", "condition", "child"} and
                len(children) == 1 and children[0] == before,
                "SIDE_CHANGE_NOT_ONE_ADDED_FILTER")
        guard = parse_expression(f.get("condition"))
        require(guard_attribute_expr_ids(guard) <=
                (left_ids if side == 0 else right_ids),
                "INSERTED_FILTER_NOT_SIDE_SCOPED")
        inserted.append(guard)
        sides.append(side)
    require(inserted, "NO_INSERTED_GUARDS")
    conjunction = join_predicate
    for predicate in inserted:
        conjunction = ("And", conjunction, predicate)
    proof = classify_trees(join_predicate, conjunction, "TRUE_ONLY",
                           allow_remap=True)
    require(proof["status"] == "EQUIVALENT_IN_FRAGMENT",
            "GUARDS_NOT_ENTAILED_BY_JOIN")
    witness = sqlite_validate(join_predicate, conjunction, proof)
    return joined_output, {
        "guarded_sides": sides,
        "orbits_checked": proof["orbits_checked"],
        "sqlite_verified": witness["verified"],
    }


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    frames = []
    while True:
        a, ac = old
        b, bc = new
        if a.get("class") not in (LOGICAL + "Project", LOGICAL + "Sort"):
            break
        require(a == b and a.get("class") == b.get("class") and
                a.get("num-children") == 1 and len(ac) == len(bc) == 1,
                "CONTEXT_FRAME_CHANGED")
        if a.get("class") == LOGICAL + "Project":
            frames.append(("project", a))
        else:
            require(a.get("child") == 0, "BAD_SORT_FRAME")
            frames.append(("sort", a))
        old, new = ac[0], bc[0]
    scope, core = core_certificate(old, new)
    constant_count = 0
    for frame, node in reversed(frames):
        if frame == "project":
            scope, constants = named_project(node, scope)
            constant_count += constants
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "scoped_inner_join_implied_guards_with_unchanged_row_local_frames",
        "core": "inner_join_guard_with_schema_checked_leaves",
        "frames": [name for name, _ in frames],
        "constants": constant_count,
        **core,
    }


def certify_contextual_join_guard_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
