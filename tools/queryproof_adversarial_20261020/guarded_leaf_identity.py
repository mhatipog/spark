"""Emergency fail-closed guard for unproven opaque Catalyst input identity.

Catalyst TreeNode.toJSON encodes LogicalRDD.rdd as JNull. Equality of such
JSON leaves does not imply equality of their input row bags. Until genuine
JVM-side input-object bindings are attached to local proof obligations,
all relation leaves except the intrinsic, immutable OneRowRelation abstain.

This is versioned research code. It never changes pinned historical checkers.
"""
SAFE_INTRINSIC_LEAVES = frozenset({
    "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation",
})


def certify_with_opaque_leaf_guard(event, local_checker):
    try:
        for side in ("old", "new"):
            plan = event[side]
            if not isinstance(plan, list) or not plan:
                return {"status": "UNKNOWN", "reason": "MISSING_PLAN"}
            leaves = [
                node for node in plan if isinstance(node, dict) and
                node.get("num-children") == 0
            ]
            if not leaves:
                return {"status": "UNKNOWN", "reason": "MISSING_PLAN_LEAVES"}
            for leaf in leaves:
                if leaf.get("class") not in SAFE_INTRINSIC_LEAVES:
                    return {"status": "UNKNOWN",
                            "reason": "UNATTESTED_INPUT_RELATION",
                            "leaf_class": leaf.get("class", "UNKNOWN")}
                if set(leaf) != {"class", "num-children"}:
                    return {"status": "UNKNOWN",
                            "reason": "UNTRUSTED_INTRINSIC_LEAF_FIELDS"}
    except (KeyError, TypeError, ValueError, AttributeError):
        return {"status": "UNKNOWN", "reason": "MALFORMED_PLAN"}
    return local_checker(event)
