"""Round S: changed-source v3 hierarchical ledger on ORIGINAL, already-seen M fixtures.

This is an explicit posthoc chain diagnostic. The independent R32 first-run
verifier and manifest are untouched; no retrospective R score changes.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_join_pruning_q_dev_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_round_n_dev_20261020"))
for name in ("queryproof_context_validator", "queryproof_instrumented",
             "queryproof_holdout_20261018", "queryproof_scopes_20261020",
             "queryproof_state_witness_20261020", "queryproof_state_frame_20261020",
             "queryproof_adversarial_20261020", "queryproof_external_postgresql_join_20261019"):
    sys.path.insert(0, str(ROOT / "tools" / name))
sys.path.insert(0, str(HERE))

import run_external
import run_q  # attaches Q's diagnostics but no raw output
from scope_ledger_v2 import score_scope_trace as score_v2
from scope_ledger_v3 import score_scope_trace as score_v3
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy

BASE = run_external.union_coverage


def diagnostic(path):
    original = BASE(path)
    old = score_v2(path)
    new = score_v3(path)
    state = score_state_witness(path, new)
    frame = score_frame(path, new)
    inp = score_input_trace(path, new)
    all_rule = audit_all_rules(path, new)
    tags = score_taxonomy(path, new)
    baseline = new.get("certified_changed_optimizer_suffixes", 0)
    original["round_s_v3"] = {
        "status": "POSTHOC_CHANGED_CHECKER_NO_R_HOLDOUT_CREDIT",
        "v2_certified_suffixes": old.get("certified_changed_optimizer_suffixes"),
        "v3_certified_suffixes": baseline,
        "v2_case_suffixes": old.get("optimizer_suffix_case_counts"),
        "v3_case_suffixes": new.get("optimizer_suffix_case_counts"),
        "v3_unknown_rule_types": new.get("unknown_rules"),
        "v3_changed_rule_events": new.get("recorded_rule_events"),
        "v3_scope_status": new.get("status"),
        "all_gate_statuses": {
            "state": state.get("status"),
            "frame": frame.get("status"),
            "rdd_input": inp.get("status"),
            "all_rule": all_rule.get("status"),
            "tags": tags.get("status"),
        },
        "gate_candidate_counts": {
            "frame": frame.get("newly_frame_preserved_count"),
            "rdd_input": inp.get("input_identity_eligible_conditional_certificates"),
            "all_rule": all_rule.get("candidates_without_observed_silent_drift"),
            "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate"),
        },
        "limitations": [
            "All four cases already observed as frozen M and Q training data",
            "Reuses the exact same disposable trace but new v3 checker; zero prospective credit",
            "Frame/input/all-rule/tag gates necessary, not sufficient",
            "Raw Catalyst AST and source rows never output",
        ],
    }
    return original


run_external.union_coverage = diagnostic
run_external.MANIFEST = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_s_posthoc_suffix.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
