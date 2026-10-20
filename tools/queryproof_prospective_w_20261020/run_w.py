"""FIRST-RUN prospective W32: v5 joined proof-chain verifier, source-pinned.

Unlike posthoc M cases, W data and SQL variants were created after freezing
v5. The primary metric is changed complete post-FinishAnalysis logical
optimizer suffixes accepted by v5, with separately stated necessary JVM
state/frame/RDD input/all-rule/tag gates. UNKNOWN never counts as proof.
"""
from collections import Counter, defaultdict
from pathlib import Path
import json
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
from rank_ast import decode
from run_holdout import _rule_basename
from join_project_v1 import certify_join_pruning_event
from scope_ledger_v5 import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy

ORIGINAL = run_external.union_coverage
MANIFEST = json.loads((HERE / "CASES.json").read_text())
LOOKUP = {case["id"]: case for case in MANIFEST["cases"]}


def score_w(path):
    original = ORIGINAL(path)
    scopes = score_scope_trace(path)
    state = score_state_witness(path, scopes)
    frame = score_frame(path, scopes)
    inp = score_input_trace(path, scopes)
    all_rule = audit_all_rules(path, scopes)
    tags = score_taxonomy(path, scopes)

    local = defaultdict(list)
    changed_rules = Counter()
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if event.get("old") == event.get("new"):
            continue
        case = event.get("case")
        if case not in LOOKUP:
            raise ValueError("UNKNOWN_W32_CASE_OBSERVED")
        rule = _rule_basename(event["rule"])
        changed_rules[rule] += 1
        if rule == "ColumnPruning":
            cert = certify_join_pruning_event(event)
            local[case].append(cert["status"] == "EQUIVALENT_IN_FRAGMENT")

    suffixes = scopes.get("optimizer_suffix_case_counts", [])
    verdicts = {s["case"]: s for s in suffixes}
    per_family = defaultdict(lambda: {
        "cases": 0, "changed_suffix_cases": 0,
        "complete_certified_suffix_cases": 0,
        "cases_with_changed_pruning": 0,
        "all_changed_pruning_events_certified": 0,
    })
    per_case = []
    for item in MANIFEST["cases"]:
        case = item["id"]
        suffix = verdicts.get(case, {})
        family = per_family[item["family"]]
        family["cases"] += 1
        family["changed_suffix_cases"] += bool(suffix.get("changed"))
        family["complete_certified_suffix_cases"] += bool(
            suffix.get("changed") and suffix.get("proved"))
        family["cases_with_changed_pruning"] += bool(local[case])
        family["all_changed_pruning_events_certified"] += bool(
            local[case] and all(local[case]))
        per_case.append({
            "case": case, "family": item["family"], "group": item["group"],
            "changed_suffix": bool(suffix.get("changed")),
            "v5_complete_suffix_proved": bool(
                suffix.get("changed") and suffix.get("proved")),
            "v5_suffix_reason": suffix.get("reason", "MISSING"),
            "changed_pruning_events": len(local[case]),
            "locally_certified_join_pruning_events": sum(local[case]),
        })
    expected = {
        "scope": "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER",
        "state": "COMPLETE_JVM_WITNESSES",
        "frame": "COMPLETE_FRAME_COMPARISON",
        "rdd_input": "COMPLETE_INPUT_RETROSPECTIVE",
        "all_rule": "COMPLETE_ALL_RULE_AUDIT",
        "tags": "COMPLETE_HASHED_TAG_CENSUS",
    }
    statuses = {
        "scope": scopes.get("status"), "state": state.get("status"),
        "frame": frame.get("status"), "rdd_input": inp.get("status"),
        "all_rule": all_rule.get("status"), "tags": tags.get("status"),
    }
    counts = {
        "frame": frame.get("newly_frame_preserved_count"),
        "rdd_input": inp.get("input_identity_eligible_conditional_certificates"),
        "all_rule": all_rule.get("candidates_without_observed_silent_drift"),
        "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate"),
    }
    candidates = scopes.get("certified_changed_optimizer_suffixes", 0)
    original["w32_full_suffix_first_run"] = {
        "status": "NEW_INDEPENDENT_W32_HOLDOUT_FROZEN_V5",
        "case_count": len(MANIFEST["cases"]),
        "changed_optimizer_suffixes": scopes.get("changed_optimizer_suffixes"),
        "v5_certified_complete_changed_suffixes": candidates,
        "cohort_strata": dict(Counter(x["group"] for x in MANIFEST["cases"])),
        "families": {k: dict(v) for k, v in sorted(per_family.items())},
        "cases": per_case,
        "observed_changed_rule_classes": dict(sorted(changed_rules.items())),
        "unknown_rule_types": scopes.get("unknown_rules"),
        "gate_statuses": statuses,
        "necessary_gate_counts": counts,
        "all_guard_statuses_complete": statuses == expected and not scopes.get("errors"),
        "gate_counts_match_scope_candidates": all(v == candidates for v in counts.values()),
        "same_case_gate_intersection_not_assumed": True,
        "limitations": [
            "A structural step-by-step Catalyst logical row-bag certificate, not mechanized proof",
            "Synthetic family-correlated SQL and data; 2 predetermined variants per family",
            "Necessary guard counts do not establish same-case intersections by themselves",
            "No physical execution-order, UDF, nondeterminism, or runtime exception guarantees",
            "No retuning or retroactive scoring of frozen R32, O32, M48",
        ],
    }
    return original


run_external.union_coverage = score_w
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_prospective_w32.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
