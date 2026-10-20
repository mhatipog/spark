"""Independent prospective R32 join-STEP holdout; never retune first-run score.

The newly developed Round Q join checker is source-pinned before any R SQL.
Local rule-event certificates are the PRIMARY metric. Unrelated optimizer
rules remain UNKNOWN; suffix proof coverage reported separately under the
unchanged previous v2 ledger and independently checked necessary gates.
"""
from collections import Counter, defaultdict
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for name in (
    "queryproof_external_postgresql_join_20261019",
    "queryproof_round_n_dev_20261020",
    "queryproof_join_pruning_q_dev_20261020",
    "queryproof_context_validator", "queryproof_instrumented",
    "queryproof_holdout_20261018", "queryproof_scopes_20261020",
    "queryproof_state_witness_20261020", "queryproof_state_frame_20261020",
    "queryproof_adversarial_20261020",
):
    sys.path.insert(0, str(ROOT / "tools" / name))

import run_external
from rank_ast import decode
from run_holdout import _rule_basename
from join_project_v1 import certify_join_pruning_event
from scope_ledger_v2 import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy

ORIGINAL = run_external.union_coverage
MANIFEST = json.loads((HERE / "CASES.json").read_text())
CASE_LOOKUP = {row["id"]: row for row in MANIFEST["cases"]}


def score_r(path):
    old = ORIGINAL(path)
    results = defaultdict(list)
    all_changed_rule_events = Counter()
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        e = decode(line)
        if e.get("old") == e.get("new"):
            continue
        case = e.get("case")
        if case not in CASE_LOOKUP:
            raise ValueError("UNKNOWN_R_CASE_IN_OBSERVED_EVENT")
        rule = _rule_basename(e["rule"])
        all_changed_rule_events[rule] += 1
        if rule != "ColumnPruning":
            continue
        verdict = certify_join_pruning_event(e)
        results[case].append({
            "status": verdict["status"],
            "reason": verdict.get("reason", "CERTIFIED"),
            "join_kind": verdict.get("join_kind"),
            "left_kept_columns": verdict.get("left_kept_columns"),
            "right_kept_columns": verdict.get("right_kept_columns"),
            "live_aggregate_attributes": verdict.get("live_aggregate_attributes"),
        })
    by_family = defaultdict(lambda: {"total": 0, "observed_changed_pruning": 0,
                                     "all_join_pruning_events_certified": 0,
                                     "certified_events": 0, "unknown_events": 0})
    per_case = []
    for case in MANIFEST["cases"]:
        info = by_family[case["family"]]
        info["total"] += 1
        local = results[case["id"]]
        yes = sum(x["status"] == "EQUIVALENT_IN_FRAGMENT" for x in local)
        no = len(local) - yes
        info["certified_events"] += yes
        info["unknown_events"] += no
        info["observed_changed_pruning"] += bool(local)
        info["all_join_pruning_events_certified"] += bool(local) and no == 0
        per_case.append({"case": case["id"], "family": case["family"],
                         "group": case["group"], "changed_pruning_events": len(local),
                         "certified_local_events": yes, "all_local_pruning_certified": bool(local) and no == 0,
                         "verdicts": local})

    scopes = score_scope_trace(path)
    state = score_state_witness(path, scopes)
    frame = score_frame(path, scopes)
    inp = score_input_trace(path, scopes)
    all_rule = audit_all_rules(path, scopes)
    tags = score_taxonomy(path, scopes)
    baseline = scopes.get("certified_changed_optimizer_suffixes", 0)
    statuses = {
        "scope": scopes.get("status"), "state": state.get("status"),
        "frame": frame.get("status"), "rdd_input": inp.get("status"),
        "all_rule": all_rule.get("status"), "tags": tags.get("status")
    }
    counts = {
        "frame": frame.get("newly_frame_preserved_count"),
        "rdd_input": inp.get("input_identity_eligible_conditional_certificates"),
        "all_rule": all_rule.get("candidates_without_observed_silent_drift"),
        "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate")
    }
    expected = {
        "scope": "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER",
        "state": "COMPLETE_JVM_WITNESSES",
        "frame": "COMPLETE_FRAME_COMPARISON",
        "rdd_input": "COMPLETE_INPUT_RETROSPECTIVE",
        "all_rule": "COMPLETE_ALL_RULE_AUDIT",
        "tags": "COMPLETE_HASHED_TAG_CENSUS",
    }
    gate_integrity = statuses == expected and not scopes.get("errors")
    same_counts = gate_integrity and all(c == baseline for c in counts.values())
    old["r32_primary_local_join_steps"] = {
        "status": "INDEPENDENT_PROSPECTIVE_R32_FIRST_RUN",
        "source": "frozen pre-first-execution synthetic R32 cases",
        "case_count": len(MANIFEST["cases"]),
        "cases_with_observed_changed_columnpruning": len(results),
        "changed_columnpruning_events": sum(len(v) for v in results.values()),
        "certified_local_join_events": sum(
            x["status"] == "EQUIVALENT_IN_FRAGMENT" for v in results.values() for x in v),
        "all_changed_columnpruning_events_certified_cases": sum(
            bool(v) and all(x["status"] == "EQUIVALENT_IN_FRAGMENT" for x in v)
            for v in results.values()),
        "family_counts": {k: dict(v) for k, v in sorted(by_family.items())},
        "case_verdicts": per_case,
        "all_changed_rule_event_classes": dict(sorted(all_changed_rule_events.items())),
        "strict_unknown_policy": "No unsupported event can be counted as certified",
        "limitations": [
            "Local ColumnPruning step certificates only, NOT complete optimizer suffixes",
            "Purposively family-enriched, four data instances per SQL shape",
            "Defined row-bag semantics; no runtime errors/order/UDF equivalence",
            "Live RDD, frame, hidden state, all-rule/tag guards below are for suffix candidates, not automatic local-step sufficiency",
        ]
    }
    old["r32_unchanged_v2_suffix_and_guards"] = {
        "status": "COMPLETE_ALL_GATES" if gate_integrity else "INVALID_GATE_INTEGRITY",
        "v2_structural_changed_suffix_candidates": baseline,
        "changed_suffixes": scopes.get("changed_optimizer_suffixes"),
        "guard_statuses": statuses,
        "necessary_gate_counts": counts,
        "same_case_gate_intersection_not_assumed": True,
        "eligible_intersection_upper_bound": baseline if same_counts else None,
        "limits": ["Independent gates necessary not sufficient", "Q join checker not substituted into frozen v2 suffix scoring"],
    }
    return old


run_external.union_coverage = score_r
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_prospective_r32.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
