"""Prototype input-identity attestation gate for Catalyst local certificates.

A trusted RuleExecutor observer must mint stable JVM-identity tokens for each
opaque relation's backing input and bind them to the same old/new rule event.
The token is not a hash of RDD.toString or TreeNode.toJSON.

This file tests the logical obligation, NOT authenticity of production
instrumentation. Attestation from arbitrary untrusted JSON is insufficient.
"""
from collections import Counter

OPAQUE_RDD = "org.apache.spark.sql.execution.LogicalRDD"
INTRINSIC = "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation"
SOURCE = "JVM_REFERENCE_ID_ALLOCATOR_V1"


def certify_with_leaf_attestation(event, checker, attestation):
    try:
        if (not isinstance(attestation, dict) or
                attestation.get("source") != SOURCE or
                attestation.get("complete") is not True):
            return {"status": "UNKNOWN", "reason": "MISSING_TRUSTED_LEAF_BINDING"}
        descriptions = {}
        for side, field in (("old", "before"), ("new", "after")):
            plan = event[side]
            if not isinstance(plan, list) or not plan:
                return {"status": "UNKNOWN", "reason": "MALFORMED_PLAN"}
            leaves = [node for node in plan if
                      isinstance(node, dict) and node.get("num-children") == 0]
            witnessed = attestation[field]
            if not isinstance(witnessed, list) or len(witnessed) != len(leaves):
                return {"status": "UNKNOWN", "reason": "INCOMPLETE_LEAF_BINDINGS"}
            desc = []
            for leaf, item in zip(leaves, witnessed):
                if not isinstance(item, dict) or item.get("class") != leaf.get("class"):
                    return {"status": "UNKNOWN", "reason": "LEAF_CLASS_WITNESS_MISMATCH"}
                cls = leaf.get("class")
                if cls == INTRINSIC:
                    if set(leaf) != {"class", "num-children"}:
                        return {"status": "UNKNOWN", "reason": "UNTRUSTED_INTRINSIC_LEAF"}
                    desc.append((cls, "intrinsic"))
                elif cls == OPAQUE_RDD:
                    token = item.get("rdd_token")
                    if type(token) is not int or token <= 0:
                        return {"status": "UNKNOWN", "reason": "INVALID_RDD_IDENTITY_TOKEN"}
                    desc.append((cls, token))
                else:
                    return {"status": "UNKNOWN", "reason": "UNMODELLED_OPAQUE_LEAF"}
            descriptions[side] = Counter(desc)
        if descriptions["old"] != descriptions["new"]:
            return {"status": "UNKNOWN", "reason": "INPUT_RELATION_IDENTITY_CHANGED"}
    except (KeyError, ValueError, TypeError, AttributeError):
        return {"status": "UNKNOWN", "reason": "MALFORMED_ATTESTATION"}
    return checker(event)
