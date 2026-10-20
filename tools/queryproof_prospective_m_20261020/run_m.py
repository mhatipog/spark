"""First-run, non-mutating M scorer: compose pre-existing frozen H gates.

Never edit underlying certificate implementations or M CASES after observing
the first result. Raw SQL / plans stay in disposable private CI stderr.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_witness_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_adversarial_20261020"))

import run_external
from scope_ledger import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy


def score_m(path):
    report = PREVIOUS_UNION(path)
    scopes = score_scope_trace(path)
    state = score_state_witness(path, scopes)
    frame = score_frame(path, scopes)
    inputs = score_input_trace(path, scopes)
    rule_calls = audit_all_rules(path, scopes)
    tag_shape = score_taxonomy(path, scopes)
    baseline = scopes.get("certified_changed_optimizer_suffixes", 0)
    checks = (
        scopes.get("status") == "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER",
        not scopes.get("errors"),
        state.get("status") == "COMPLETE_JVM_WITNESSES",
        frame.get("status") == "COMPLETE_FRAME_COMPARISON",
        inputs.get("status") == "COMPLETE_INPUT_RETROSPECTIVE",
        rule_calls.get("status") == "COMPLETE_ALL_RULE_AUDIT",
        tag_shape.get("status") == "COMPLETE_HASHED_TAG_CENSUS",
    )
    # An intersection cannot generally be inferred from individual counts.
    # It is valid in this special case ONLY when each set equals the same
    # baseline set (and all corresponding record integrity gates complete).
    counts = [
        frame.get("newly_frame_preserved_count"),
        inputs.get("input_identity_eligible_conditional_certificates"),
        rule_calls.get("candidates_without_observed_silent_drift"),
        tag_shape.get("candidate_cases_passing_tag_shape_necessary_gate"),
    ]
    all_preserved = all(checks) and all(count == baseline for count in counts)
    report["m_frozen_full_observer_gate"] = {
        "status": "COMPLETE_ALL_GATES" if all(checks) else "INVALID_GATE_INTEGRITY",
        "candidate_count_from_structural_scope": baseline,
        "necessary_gate_candidate_counts": {
            "frame": counts[0], "input": counts[1],
            "all_rule_no_silent_drift": counts[2], "tag_shape": counts[3],
        },
        "conservative_intersection": baseline if all_preserved else None,
        "intersection_status": (
            "ALL_GATE_SETS_EQUAL_BASELINE" if all_preserved else
            "NOT_ESTABLISHED_DO_NOT_USE_MINIMUM_AS_INTERSECTION"
        ),
        "limits": [
            "Necessary guards and local proof fragments; not arbitrary JVM semantics",
            "No version-independent tag noninterference theorem",
            "No per-case intersection reported unless every entire candidate set passes",
            "Only an independently seeded 48-case synthetic SQL corpus",
        ],
    }
    report["hierarchical_scope_ledger"] = scopes
    report["posthoc_jvm_state_audit"] = state
    report["posthoc_frame_preservation"] = frame
    report["v4_live_input_attestation"] = inputs
    report["v5_all_rule_accounting"] = rule_calls
    report["v6_tag_type_census"] = tag_shape
    return report


PREVIOUS_UNION = run_external.union_coverage
run_external.union_coverage = score_m
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_prospective_m.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True


if __name__ == "__main__":
    raise SystemExit(run_external.parent())
