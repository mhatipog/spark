"""Prospective O32 runner: repair earlier N administrative-metadata failure with a genuinely unseen O dataset.

NEVER tune or rewrite after first O execution and call it the same prospective
cohort. Raw Catalyst data stays in disposable worker memory/temp folders.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for name in (
    "queryproof_external_postgresql_join_20261019",
    "queryproof_round_n_dev_20261020",
    "queryproof_context_validator",
    "queryproof_instrumented",
    "queryproof_holdout_20261018",
    "queryproof_scopes_20261020",
    "queryproof_state_witness_20261020",
    "queryproof_state_frame_20261020",
    "queryproof_adversarial_20261020",
):
    sys.path.insert(0, str(ROOT / "tools" / name))

import run_external
from scope_ledger_v2 import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy

ORIGINAL_FIVE = run_external.union_coverage

def score_n(private_path):
    legacy = ORIGINAL_FIVE(private_path)
    scopes = score_scope_trace(private_path)
    state = score_state_witness(private_path, scopes)
    frame = score_frame(private_path, scopes)
    inputs = score_input_trace(private_path, scopes)
    all_rules = audit_all_rules(private_path, scopes)
    tags = score_taxonomy(private_path, scopes)
    baseline = scopes.get("certified_changed_optimizer_suffixes", 0)
    statuses = {
        "scope": scopes.get("status"),
        "state": state.get("status"),
        "frame": frame.get("status"),
        "rdd_input": inputs.get("status"),
        "all_rule": all_rules.get("status"),
        "tags": tags.get("status"),
    }
    counts = {
        "frame": frame.get("newly_frame_preserved_count"),
        "rdd_input": inputs.get("input_identity_eligible_conditional_certificates"),
        "all_rule": all_rules.get("candidates_without_observed_silent_drift"),
        "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate"),
    }
    integrity = (
        statuses == {
            "scope": "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER",
            "state": "COMPLETE_JVM_WITNESSES",
            "frame": "COMPLETE_FRAME_COMPARISON",
            "rdd_input": "COMPLETE_INPUT_RETROSPECTIVE",
            "all_rule": "COMPLETE_ALL_RULE_AUDIT",
            "tags": "COMPLETE_HASHED_TAG_CENSUS",
        } and not scopes.get("errors")
    )
    same_set = integrity and all(x == baseline for x in counts.values())
    legacy["o32_full_frozen_observer_gate"] = {
        "status": "COMPLETE_ALL_GATES" if integrity else "INVALID_GATE_INTEGRITY",
        "structural_suffix_candidates": baseline,
        "guard_statuses": statuses,
        "necessary_gate_counts": counts,
        "conservative_intersection": baseline if same_set else None,
        "intersection_status": (
            "ALL_GATE_SETS_EQUAL_BASELINE" if same_set
            else "NOT_ESTABLISHED_DO_NOT_INFER_INTERSECTION"),
        "limits": [
            "New corpus, prospectively frozen before any O SQL executed",
            "Cohort purposively enriched for COUNT/MAX/MIN; not population estimate",
            "Necessary frame/RDD/rule/tag guards cannot prove arbitrary JVM semantics",
            "No claim of kernel-checked SQL rewrite equivalence",
        ],
    }
    legacy["o32_hierarchical_scope_ledger"] = scopes
    legacy["o32_posthoc_jvm_state_audit"] = state
    legacy["o32_frame_preservation"] = frame
    legacy["o32_rdd_attestation"] = inputs
    legacy["o32_all_rule_accounting"] = all_rules
    legacy["o32_tag_type_census"] = tags
    return legacy

run_external.union_coverage = score_n
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_prospective_o32.json"
run_external.DIAGNOSTIC_GENERALIZED = False
run_external.DIAGNOSTIC_GROUPING_REPETITION = False

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
