"""Round Q DEVELOPMENT: exact two-input ColumnPruning join projection certificate.

Recognizes only:
  Sort*(Aggregate(Join(L,R)))
    -> Sort*(Aggregate(Project(S, Join(Project(SL,L), Project(SR,R)))))
where unchanged Join is INNER/LEFT OUTER integral equality, each Project is
ordered exact AttributeReference identity selection, and parent Aggregate only
reads available original attrs. Input leaves are unchanged LogicalRDD/LocalRelation.

This is a conditional row-BAG theorem, not a theorem for errors, order, side
effects, physical execution, arbitrary Spark operators or an optimizer suffix.
Posthoc M29/M33 are development fixtures and must not gain prospective credit.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import LOGICAL, EXPR, tree_from_preorder
from collapse_project_certificate import _attribute
from grouping_repetition_certificate import _checked_aggregate_references

INTEGER = {"byte", "short", "integer", "long"}
LEAVES = {"org.apache.spark.sql.execution.LogicalRDD", LOGICAL + "LocalRelation"}
JOIN_TYPES = {
    "org.apache.spark.sql.catalyst.plans.Inner$": "inner",
    "org.apache.spark.sql.catalyst.plans.LeftOuter$": "left",
}


def _leaf_attributes(tree):
    head, children = tree
    require(head.get("class") in LEAVES and
            head.get("num-children") == 0 and not children,
            "JOIN_INPUT_NOT_DIRECT_LEAF")
    raw = head.get("output")
    require(isinstance(raw, list) and raw,
            "LEAF_OUTPUT_MISSING")
    keyed = {}
    order = []
    for item in raw:
        require(isinstance(item, list) and len(item) == 1,
                "LEAF_ATTRIBUTE_NOT_DIRECT")
        attr = item[0]
        key, dtype, name = _attribute(attr)
        require(isinstance(name, str) and name and key not in keyed,
                "INVALID_LEAF_ATTRIBUTE")
        keyed[key] = (dtype, name, item)
        order.append(key)
    return keyed, order


def _identity_prune(old_tree, new_tree, attrs, order):
    if old_tree == new_tree:
        return order[:], False
    header, children = new_tree
    require(header.get("class") == LOGICAL + "Project" and
            header.get("num-children") == 1 and
            header.get("child") == 0 and len(children) == 1 and
            set(header) == {"class", "num-children", "child", "projectList"} and
            children[0] == old_tree,
            "JOIN_SIDE_NOT_EXACT_INSERTED_PROJECT")
    items = header["projectList"]
    require(isinstance(items, list), "SIDE_PROJECT_LIST_MALFORMED")
    kept = []
    for item in items:
        require(isinstance(item, list) and len(item) == 1,
                "SIDE_PROJECT_COMPUTED_OR_DISTINCT")
        key, dtype, name = _attribute(item[0])
        require(key in attrs and
                (dtype, name, item) == attrs[key], "SIDE_PROJECT_NOT_SOURCE_IDENTITY")
        kept.append(key)
    require(len(set(kept)) == len(kept), "SIDE_PROJECT_DUPLICATES_EXPR_ID")
    require(kept == [k for k in order if k in kept],
            "SIDE_PROJECT_REORDERS_COLUMNS")
    return kept, True


def _outer_project(header, children, old_join_order, old_attrs):
    require(header.get("class") == LOGICAL + "Project" and
            header.get("num-children") == 1 and
            header.get("child") == 0 and len(children) == 1 and
            set(header) == {"class", "num-children", "child", "projectList"},
            "JOIN_TOP_PROJECT_MISSING")
    items = header.get("projectList")
    require(isinstance(items, list), "TOP_PROJECT_MALFORMED")
    kept = []
    for item in items:
        require(isinstance(item, list) and len(item) == 1,
                "TOP_PROJECT_COMPUTED")
        key, dtype, name = _attribute(item[0])
        require(key in old_attrs and
                (dtype, name, item) == old_attrs[key],
                "TOP_PROJECT_NOT_ORIGINAL_IDENTITY")
        kept.append(key)
    require(len(kept) == len(set(kept)), "TOP_PROJECT_DUPLICATES_EXPR_ID")
    require(kept == [k for k in old_join_order if k in kept],
            "TOP_PROJECT_REORDERS_COLUMNS")
    return kept


def _condition(head, lset, rset):
    raw = head.get("condition")
    require(isinstance(raw, list) and len(raw) == 3,
            "JOIN_CONDITION_NOT_BINARY_EQUATION")
    root, left, right = raw
    require(root.get("class") == EXPR + "EqualTo" and
            root.get("num-children") == 2 and
            root.get("left") == 0 and root.get("right") == 1 and
            set(root).issubset({"class", "num-children", "left", "right", "evalContext"}),
            "JOIN_CONDITION_NOT_DIRECT_EQUAL_TO")
    lk, lt, _ = _attribute(left)
    rk, rt, _ = _attribute(right)
    require(lt == rt and lt in INTEGER and
            (lk in lset and rk in rset or lk in rset and rk in lset),
            "JOIN_KEY_SCOPE_OR_TYPE_INVALID")
    return {lk, rk}


def _certify(event):
    before = tree_from_preorder(event["old"])
    after = tree_from_preorder(event["new"])
    require(before != after, "NO_CHANGED_EVENT")
    sorts = 0
    while before[0].get("class") == LOGICAL + "Sort":
        bhead, bc = before
        ahead, ac = after
        require(bhead == ahead and bhead.get("num-children") == 1
                and bhead.get("child") == 0 and len(bc) == len(ac) == 1,
                "SORT_FRAME_CHANGED")
        before, after = bc[0], ac[0]
        sorts += 1
    bagg, bc = before
    aagg, ac = after
    require(bagg.get("class") == LOGICAL + "Aggregate" and
            bagg == aagg and len(bc) == len(ac) == 1,
            "AGGREGATE_FRAME_CHANGED")
    live = _checked_aggregate_references(bagg)

    old_join, old_children = bc[0]
    new_project, pchildren = ac[0]
    require(old_join.get("class") == LOGICAL + "Join" and
            len(old_children) == 2, "INPUT_NOT_BINARY_JOIN")
    require(old_join.get("num-children") == 2 and
            old_join.get("left") == 0 and old_join.get("right") == 1,
            "MALFORMED_JOIN_METADATA")
    kind = old_join.get("joinType")
    require(isinstance(kind, dict) and
            set(kind) == {"object"} and kind["object"] in JOIN_TYPES,
            "UNSUPPORTED_JOIN_TYPE")

    lhs, lhs_order = _leaf_attributes(old_children[0])
    rhs, rhs_order = _leaf_attributes(old_children[1])
    require(lhs.keys().isdisjoint(rhs), "OVERLAPPING_JOIN_EXPR_IDS")
    original = dict(lhs, **rhs)
    joined_order = lhs_order + rhs_order
    join_refs = _condition(old_join, lhs, rhs)

    kept_top = _outer_project(new_project, pchildren, joined_order, original)
    new_join, new_children = pchildren[0]
    require(new_join == old_join and len(new_children) == 2,
            "JOIN_METADATA_OR_CONDITION_CHANGED")
    kept_left, left_changed = _identity_prune(old_children[0], new_children[0],
                                               lhs, lhs_order)
    kept_right, right_changed = _identity_prune(old_children[1], new_children[1],
                                                 rhs, rhs_order)
    require(left_changed or right_changed, "NO_JOIN_SIDE_PRUNING")
    available = set(kept_left) | set(kept_right)
    require(join_refs <= available, "JOIN_CONDITION_KEY_PRUNED")
    require(set(kept_top) <= available, "TOP_PROJECT_ATTRIBUTE_PRUNED")
    for key, dtype in live:
        require(key in kept_top and original[key][0] == dtype,
                "AGGREGATE_LIVE_ATTRIBUTE_DROPPED")

    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "exact_two_sided_join_identity_pruning_under_unchanged_aggregate",
        "join_kind": JOIN_TYPES[kind["object"]],
        "sort_frames": sorts,
        "live_aggregate_attributes": len(live),
        "condition_attributes": len(join_refs),
        "left_source_columns": len(lhs_order),
        "right_source_columns": len(rhs_order),
        "left_kept_columns": len(kept_left),
        "right_kept_columns": len(kept_right),
        "top_kept_columns": len(kept_top),
        "semantic_law": "bag-preserving identity projection under a pure unchanged keyed join",
        "limits": ["defined row bags only", "posthoc development only"],
    }


def certify_join_pruning_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError,
            AttributeError, ValueError, RecursionError) as e:
        return {"status": "UNKNOWN", "reason": str(e)}
