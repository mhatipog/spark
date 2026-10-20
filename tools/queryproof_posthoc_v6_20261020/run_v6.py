"""V6 POSTHOC development test: exact OR-join predicate dependency frames.

Never edits or rescores frozen W first-run. Uses original W data only as
retrospective fixtures. Computes whole-suffix v6 chains and independently
rechecks state/frame/RDD/all-rule/tag necessary observations for the new
candidate set. Reports both negative mutants and any UNKNOWN result.
"""
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
for name in ("queryproof_posthoc_v6_20261020",
             "queryproof_posthoc_y_20261020",
             "queryproof_posthoc_x_20261020",
             "queryproof_prospective_w_20261020",
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
import run_y
from rank_ast import decode
from run_holdout import _rule_basename
from scope_ledger_v6 import score_scope_trace as v6_scope_trace
from join_project_v6 import certify_join_pruning_event as certify_v6
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy
from phase_guard import score_phase_local

OR_CASES = {"W21", "W22", "W23", "W24"}
MODIFICATIONS = (
    ("OR_TO_AND", 0, "And"),
    ("EQUALITY_TO_NULLSAFE", 1, "EqualNullSafe"),
)


def negative_mutants(event):
    """Mutate BOTH plan sides: identity of the join header remains unchanged."""
    answers = {}
    for label, offset, new_class in MODIFICATIONS:
        changed = copy.deepcopy(event)
        for side in ("old", "new"):
            joins = [n for n in changed[side] if isinstance(n, dict) and
                     n.get("class", "").endswith(".Join")]
            if len(joins) != 1:
                answers[label] = "MUTANT_NOT_APPLICABLE"
                break
            condition = joins[0]["condition"]
            if not isinstance(condition, list) or len(condition) != 7:
                answers[label] = "MUTANT_NOT_APPLICABLE"
                break
            condition[offset]["class"] = (
                "org.apache.spark.sql.catalyst.expressions." + new_class)
        else:
            answers[label] = certify_v6(changed)["status"]
    changed = copy.deepcopy(event)
    for side in ("old", "new"):
        joins = [n for n in changed[side] if isinstance(n, dict) and
                 n.get("class", "").endswith(".Join")]
        if len(joins) != 1:
            answers["FOREIGN_EXPR_ID"] = "MUTANT_NOT_APPLICABLE"
            break
        joins[0]["condition"][2]["exprId"]["id"] = 987654321
    else:
        answers["FOREIGN_EXPR_ID"] = certify_v6(changed)["status"]
    return answers


def score_v6(path):
    previous = run_y.score_y(path)
    v6 = v6_scope_trace(path)
    assert v6["status"] == "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER"
    prev = set(previous["round_x_posthoc"]["v5_full_suffix_cases"]) if (
        "round_x_posthoc" in previous) else set(
        previous["w32_full_suffix_first_run"]["cases"])
    candidates = {item["case"] for item in v6["optimizer_suffix_case_counts"]
                  if item["changed"] and item["proved"]}
    locals_ = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if event["case"] not in OR_CASES or event["old"] == event["new"] or (
            _rule_basename(event["rule"]) != "ColumnPruning"):
            continue
        result = certify_v6(event)
        locals_.append({
            "case": event["case"], "status": result["status"],
            "reason": result.get("reason", "PROVED_DEPENDENCY_FRAME"),
            "adversarial_controls": negative_mutants(event),
        })

    state = score_state_witness(path, v6)
    frame = score_frame(path, v6)
    inp = score_input_trace(path, v6)
    all_rule = audit_all_rules(path, v6)
    tags = score_taxonomy(path, v6)
    phase = score_phase_local(path, v6)
    previous["v6_posthoc_predicate_dependency"] = {
        "status": "DEVELOPMENT_POSTHOC_REPLAY_ONLY",
        "v5_frozen_structural_case_ids": sorted(prev),
        "v6_structural_case_ids": sorted(candidates),
        "v6_new_structural_case_ids": sorted(candidates - prev),
        "v6_regressed_structural_case_ids": sorted(prev - candidates),
        "v6_structural_count": len(candidates),
        "left_or_local_steps": locals_,
        "v6_phase_local_observed_case_ids": phase["phase_local_eligible_cases"],
        "v6_frame_preserved_case_ids": frame["newly_frame_preserved_suffixes"],
        "v6_legacy_strict_state_case_ids": state["state_guarded_changed_suffix_cases"],
        "v6_necessary_gate_counts": {
            "frame": frame["newly_frame_preserved_count"],
            "rdd_input": inp["input_identity_eligible_conditional_certificates"],
            "all_rule": all_rule["candidates_without_observed_silent_drift"],
            "tags": tags["candidate_cases_passing_tag_shape_necessary_gate"],
        },
        "v6_observer_statuses": {
            "phase": phase["status"], "frame": frame["status"],
            "state": state["status"], "rdd_input": inp["status"],
            "all_rule": all_rule["status"], "tags": tags["status"]},
        "limits": [
            "W32 was already seen; zero new prospective validation",
            "The OR-join rule is a copied, separately versioned Q/v5 checker",
            "No OR-based IsNotNull inference is admitted by this extension",
            "Only total/pure integral expression guards in the emitted grammar",
            "Phase guard is observational and lossy; not full JVM-state proof",
            "No proof about arbitrary Catalyst syntax or production Spark",
        ],
    }
    return previous


run_external.union_coverage = score_v6
run_external.MANIFEST = ROOT / "tools/queryproof_prospective_w_20261020/CASES.json"
run_external.PROTOCOL = ROOT / "tools/queryproof_prospective_w_20261020/PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts/queryproof_v6_w32_posthoc_predicate_dependency.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
