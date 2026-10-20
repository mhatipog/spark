"""POSTHOC Round V: duplicate GROUP BY idempotence over a fixed two-leaf join.

This extends only the input-scope recognition of the existing checked
grouping-repetition theorem. A logical Join subtree must remain byte-for-byte
identical; all changed AST structure is strictly duplicate grouping keys.
The keys must be proven total/pure expressions in a resolved joined scope.
This is a conditional row-bag law, not a proof of the Join operator itself.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import LOGICAL, tree_from_preorder
from grouping_repetition_certificate import (
    GROUP_METADATA, _checked_aggregate_references, _group_key)
from join_project_v1 import _leaf_attributes, _condition, JOIN_TYPES


def _certify(event):
    before = tree_from_preorder(event["old"])
    after = tree_from_preorder(event["new"])
    require(before != after, "NO_CHANGED_EVENT")
    sorts = 0
    while before[0].get("class") == LOGICAL + "Sort":
        header, children = before
        updated, newer = after
        require(header == updated and header.get("num-children") == 1
                and header.get("child") == 0 and len(children) == len(newer) == 1,
                "JOIN_GROUP_SORT_FRAME_CHANGED")
        before, after = children[0], newer[0]
        sorts += 1

    old_agg, oc = before
    new_agg, nc = after
    require(old_agg.get("class") == new_agg.get("class") == LOGICAL + "Aggregate"
            and len(oc) == len(nc) == 1 and oc[0] == nc[0],
            "JOIN_GROUP_INPUT_CHANGED")
    require(set(old_agg) == set(new_agg) == GROUP_METADATA
            and old_agg.get("num-children") == new_agg.get("num-children") == 1
            and old_agg.get("child") == new_agg.get("child") == 0,
            "JOIN_GROUP_AGGREGATE_METADATA_CHANGED")
    require(all(old_agg[k] == new_agg[k] for k in GROUP_METADATA
                if k != "groupingExpressions"),
            "JOIN_GROUP_OUTPUT_CHANGED")
    old_keys = old_agg["groupingExpressions"]
    new_keys = new_agg["groupingExpressions"]
    require(isinstance(old_keys, list) and isinstance(new_keys, list)
            and len(old_keys) > len(new_keys) >= 1,
            "JOIN_GROUP_NOT_STRICT_NONEMPTY_DEDUP")

    join, children = oc[0]
    require(join.get("class") == LOGICAL + "Join"
            and join.get("num-children") == 2
            and join.get("left") == 0 and join.get("right") == 1
            and len(children) == 2, "JOIN_GROUP_INPUT_NOT_DIRECT_TWO_WAY_JOIN")
    kind = join.get("joinType")
    require(isinstance(kind, dict) and set(kind) == {"object"}
            and kind["object"] in JOIN_TYPES, "JOIN_GROUP_UNSUPPORTED_JOIN_TYPE")
    left, _ = _leaf_attributes(children[0])
    right, _ = _leaf_attributes(children[1])
    require(set(left).isdisjoint(right), "JOIN_GROUP_OVERLAPPING_SOURCE_IDS")
    _condition(join, left, right)
    scope = {key: (dtype, name, ("attribute", dtype, key))
             for source in (left, right)
             for key, (dtype, name, _original) in source.items()}
    referenced = (_checked_aggregate_references(old_agg)
                  | _checked_aggregate_references(new_agg))
    require(all(key in scope and scope[key][0] == dtype for key, dtype in referenced),
            "JOIN_GROUP_AGGREGATE_REFERENCE_NOT_RESOLVED")

    # _group_key uses the previously audited total/pure grouping grammar;
    # identical canonical serialization is required, not a guess at equivalence.
    previous = [_group_key(expr, scope) for expr in old_keys]
    updated = [_group_key(expr, scope) for expr in new_keys]
    require(len(set(updated)) == len(updated),
            "JOIN_GROUP_OUTPUT_DUPLICATE_KEYS")
    require(set(previous) == set(updated),
            "JOIN_GROUP_KEY_SET_CHANGED")
    return {
        "status": "EQUIVALENT_IN_FRAGMENT",
        "proof_scope": "duplicate_total_grouping_keys_over_fixed_direct_join",
        "join_kind": JOIN_TYPES[kind["object"]],
        "keys_before": len(previous),
        "keys_after": len(updated),
        "removed_repetitions": len(previous) - len(updated),
        "source_left_columns": len(left),
        "source_right_columns": len(right),
        "sort_frames": sorts,
        "semantic_law": "partition(B,[g,g,...]) = partition(B,[g,...]) for pure total g",
        "limits": ["exact unchanged join input", "resolved join leaves",
                   "defined row-bag semantics", "development posthoc"],
    }


def certify_join_group_dedup_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as error:
        return {"status": "UNKNOWN", "reason": str(error)}
