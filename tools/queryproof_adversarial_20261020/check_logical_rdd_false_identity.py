"""Adversarial injection: real LogicalRDD identity collision beneath a pure proof.

The parent Aggregate/Project skeleton is an existing, known-positive synthetic
fixture; the two leaf JSONs come from different, actually executed JVM RDDs.
This is a verifier-falsification test, not an observed genuine optimizer rule.
"""
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
from test_generalized_projection_liveness import example
from generalized_projection_liveness_certificate import (
    certify_generalized_projection_liveness_event)
from scope_ledger import _chain_proof
from guarded_leaf_identity import certify_with_opaque_leaf_guard

data = json.loads(
    (ROOT / "artifacts" / "queryproof_logical_rdd_collision.json").read_text())
left, right = data["left"], data["right"]
assert isinstance(left, list) and isinstance(right, list)
assert len(left) == len(right) == 1
assert left == right, "Control requires real, bytewise-equal plan JSON"
leaf_left, leaf_right = left[0], right[0]
assert leaf_left["class"] == "org.apache.spark.sql.execution.LogicalRDD"
assert isinstance(leaf_left.get("output"), list)
assert len(leaf_left["output"]) == 2

attrs = {entry[0]["name"]: entry[0] for entry in leaf_left["output"]}
assert set(attrs) == {"v0", "v1"}


def replace_refs(value):
    if isinstance(value, list):
        return [replace_refs(v) for v in value]
    if isinstance(value, dict):
        if (value.get("class", "").endswith(".AttributeReference") and
                value.get("name") in attrs):
            return copy.deepcopy(attrs[value["name"]])
        return {k: replace_refs(v) for k, v in value.items()}
    return value


event = replace_refs(example())
event["old"][-1] = leaf_left
event["new"][-1] = leaf_right

# Scope ledger's exact-identity branch treats the two different JVM leaves as
# identical when the underlying RDD field is dropped from Catalyst JSON.
identity, reason = _chain_proof(left, right, [])
assert identity and reason == "EXACT_IDENTITY"
print("QUERYPROOF_SCOPE_LEDGER_RDD_SWAP_FALSE_IDENTITY=CONFIRMED")

result = certify_generalized_projection_liveness_event(event)
print("QUERYPROOF_RDD_SWAP_LOCAL_CERTIFICATE_STATUS", result["status"])
if result["status"] == "EQUIVALENT_IN_FRAGMENT":
    print("QUERYPROOF_ADVERSARIAL_FALSE_CERTIFIED=CONFIRMED")
else:
    print("QUERYPROOF_ADVERSARIAL_LOCAL_CHECKER_ABSTAINS=CONFIRMED")
    print("QUERYPROOF_UNKNOWN_REASON", result.get("reason", "unspecified")[:200])

# The independently versioned emergency guard must refuse the exact injected
# transition that the old checker certifies.
guarded = certify_with_opaque_leaf_guard(
    event, certify_generalized_projection_liveness_event)
assert guarded["status"] == "UNKNOWN", guarded
assert guarded["reason"] == "UNATTESTED_INPUT_RELATION", guarded
print("QUERYPROOF_OPAQUE_LEAF_GUARD_REJECTS_FALSE_CERTIFICATE=CONFIRMED")
