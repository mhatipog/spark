"""Round X: POSTHOC W32 state-set intersection audit, never first-run scoring.

Uses the same frozen W manifest and v5 checker but makes a new independently
labelled replay. The first W result remains immutable and controls priority.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for name in ("queryproof_prospective_w_20261020",
             "queryproof_join_filters_u_dev_20261020",
             "queryproof_join_suffix_s_dev_20261020",
             "queryproof_join_pruning_q_dev_20261020",
             "queryproof_round_n_dev_20261020",
             "queryproof_external_postgresql_join_20261019",
             "queryproof_context_validator", "queryproof_instrumented",
             "queryproof_holdout_20261018", "queryproof_scopes_20261020",
             "queryproof_state_witness_20261020",
             "queryproof_state_frame_20261020",
             "queryproof_adversarial_20261020"):
    sys.path.insert(0, str(ROOT / "tools" / name))

import run_external
import run_w
from scope_ledger_v5 import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy


def score_x(path):
    original = run_w.score_w(path)
    scopes = score_scope_trace(path)
    state = score_state_witness(path, scopes)
    frame = score_frame(path, scopes)
    inp = score_input_trace(path, scopes)
    all_rule = audit_all_rules(path, scopes)
    tags = score_taxonomy(path, scopes)

    candidates = {x["case"] for x in scopes.get("optimizer_suffix_case_counts", [])
                  if x["changed"] and x["proved"]}
    state_set = set(state.get("state_guarded_changed_suffix_cases", []))
    frame_set = set(frame.get("newly_frame_preserved_suffixes", []))
    assert state_set <= candidates and frame_set <= candidates
    four_counts = {
        "frame": len(frame_set),
        "rdd_input": inp.get("input_identity_eligible_conditional_certificates"),
        "all_rule": all_rule.get("candidates_without_observed_silent_drift"),
        "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate"),
    }
    expected_statuses = {
        "state": "COMPLETE_JVM_WITNESSES",
        "frame": "COMPLETE_FRAME_COMPARISON",
        "rdd_input": "COMPLETE_INPUT_RETROSPECTIVE",
        "all_rule": "COMPLETE_ALL_RULE_AUDIT",
        "tags": "COMPLETE_HASHED_TAG_CENSUS",
    }
    observed_statuses = {
        "state": state.get("status"), "frame": frame.get("status"),
        "rdd_input": inp.get("status"), "all_rule": all_rule.get("status"),
        "tags": tags.get("status"),
    }
    ready = (observed_statuses == expected_statuses and
             all(len(candidates) == n for n in four_counts.values()))
    # Under source-inspected code, input/all-rule/tag sets are each made by
    # deleting disqualified case IDs from the initial candidate set. Equal
    # cardinality then entails exact set equality, since finite A⊆C and
    # |A|=|C| => A=C. The state set is inspected explicitly below.
    original["round_x_posthoc"] = {
        "status": "SEPARATE_POSTHOC_W32_REPLAY_NOT_FIRST_RUN",
        "v5_full_suffix_cases": sorted(candidates),
        "source_state_guarded_suffix_cases": sorted(state_set),
        "source_frame_preserved_suffix_cases": sorted(frame_set),
        "state_blocked_cases": state.get("cases_blocked"),
        "state_guard_count": state.get("state_guarded_changed_suffix_count"),
        "observer_statuses": observed_statuses,
        "four_guard_candidate_counts": four_counts,
        "four_candidate_subsets_full": ready,
        "exact_five_way_survivors_if_ready": sorted(candidates & state_set)
            if ready else None,
        "exact_five_way_survivor_count_if_ready": len(candidates & state_set)
            if ready else None,
        "source_subset_justification": (
            "input, all-rule and tag scorer each computes subset of exact "
            "v5 suffix candidate case IDs; cardinality equality proves "
            "set equality; frame and state emit explicit case-ID sets"),
        "limitations": [
            "Not a prospectively untouched cohort; frozen W first-run remains separate",
            "Counters based on observed JVM metadata, not proof of all hidden effects",
            "Subset inference uses audited implementation invariants, not a proof assistant",
        ],
    }
    return original


run_external.union_coverage = score_x
run_external.MANIFEST = ROOT / "tools" / "queryproof_prospective_w_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_prospective_w_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_x_w32_posthoc.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
