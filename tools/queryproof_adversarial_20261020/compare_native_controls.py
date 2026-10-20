"""Compare the same deliberately invalid Catalyst transitions to local checkers.

Never treat native plan-integrity acceptance as a correctness certificate.
The source control writes only two tiny synthetic AST pairs on the CI worker.
"""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))

from grouping_repetition_certificate import certify_grouping_repetition_event
from generalized_projection_liveness_certificate import (
    certify_generalized_projection_liveness_event)
from aggregate_framed_identity_certificate import (
    certify_aggregate_framed_identity_event)
from collapse_project_certificate import certify_collapse_project_event

CHECKERS = {
    "GroupingRepetition": certify_grouping_repetition_event,
    "ProjectionLiveness": certify_generalized_projection_liveness_event,
    "AggregateFramedIdentity": certify_aggregate_framed_identity_event,
    "CollapseProject": certify_collapse_project_event,
}

pairs = json.loads(
    (ROOT / "artifacts" / "queryproof_native_control_pairs.json").read_text())
assert set(pairs) == {"invalid_value_change", "invalid_schema_change"}

for name, event in sorted(pairs.items()):
    statuses = {}
    for checker_name, function in CHECKERS.items():
        result = function(event)
        status = result["status"]
        statuses[checker_name] = status
        if status == "EQUIVALENT_IN_FRAGMENT":
            raise SystemExit(
                "CRITICAL_FALSE_CERTIFIED:" + name + ":" + checker_name)
        if status != "UNKNOWN":
            raise SystemExit(
                "UNEXPECTED_CHECKER_RESULT:" + name + ":" + checker_name + ":" + status)
    print("QUERYPROOF_INVALID_PAIR", json.dumps({
        "case": name, "checker_statuses": statuses}, sort_keys=True))
print("QUERYPROOF_LOCAL_CHECKERS_FAIL_CLOSED_ON_NATIVE_NEGATIVE_CONTROLS")
