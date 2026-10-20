"""Round Z: privacy-preserving, POSTHOC proof-obligation failure localization.

Replays the unchanged frozen W32 SQL under the fixed v5 grammar, without
changing any W32/X/Y first-run claim. Emits only rule names, family IDs,
checker rejection reason codes, and batch-level scope diagnostics. Never
exports Catalyst ASTs, SQL literals, input data, or the private raw trace.
"""
from collections import Counter, defaultdict
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
for name in ("queryproof_posthoc_y_20261020",
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
from scope_ledger_v5 import score_scope_trace
from generalized_liveness_v2 import certify_generalized_projection_liveness_event
from aggregate_identity_v2 import certify_aggregate_framed_identity_event
from join_project_v1 import certify_join_pruning_event
from join_grouping_v1 import certify_join_group_dedup_event
from filter_steps_v1 import (
    certify_inferred_filter_event, certify_filter_project_push_event)
from grouping_repetition_certificate import certify_grouping_repetition_event

CASES = json.loads((ROOT / "tools/queryproof_prospective_w_20261020/CASES.json").read_text())
FAMILIES = {case["id"]: case["family"] for case in CASES["cases"]}
CHALLENGE = {case["id"] for case in CASES["cases"] if case["group"] == "JOIN_CHALLENGE"}


def _attempt(event, rule):
    if rule == "ColumnPruning":
        first = certify_generalized_projection_liveness_event(event)
        if first["status"] == "EQUIVALENT_IN_FRAGMENT":
            return first, "generalized_projection"
        second = certify_join_pruning_event(event)
        if second["status"] == "EQUIVALENT_IN_FRAGMENT":
            return second, "join_projection"
        return {"status": "UNKNOWN", "reason": (
            "GENERAL:" + first.get("reason", "UNKNOWN") +
            "|JOIN:" + second.get("reason", "UNKNOWN"))}, "both"

    if rule == "RemoveRepetitionFromGroupExpressions":
        first = certify_grouping_repetition_event(event)
        if first["status"] == "EQUIVALENT_IN_FRAGMENT":
            return first, "grouping_original"
        second = certify_join_group_dedup_event(event)
        if second["status"] == "EQUIVALENT_IN_FRAGMENT":
            return second, "grouping_join"
        return {"status": "UNKNOWN", "reason": (
            "GENERAL:" + first.get("reason", "UNKNOWN") +
            "|JOIN:" + second.get("reason", "UNKNOWN"))}, "both"

    handlers = {
        "CollapseProject": certify_aggregate_framed_identity_event,
        "InferFiltersFromConstraints": certify_inferred_filter_event,
        "PushDownPredicates": certify_filter_project_push_event,
    }
    if rule not in handlers:
        return {"status": "UNKNOWN", "reason": "NO_V5_RULE_CHECKER"}, "not_implemented"
    return handlers[rule](event), "exact_v5"


def score_z(path):
    original = run_y.score_y(path)
    scope = score_scope_trace(path)
    suffixes = {x["case"]: x for x in scope["optimizer_suffix_case_counts"]}
    obligations = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        case = event["case"]
        if case not in CHALLENGE or event["old"] == event["new"]:
            continue
        # Source-pinned optimizer executor, strict AFTER Finish Analysis.
        if ("BaseSessionStateBuilder$$anon$2" not in event["executor"] or
                event["batch"] == "Finish Analysis"):
            continue
        rule = _rule_basename(event["rule"])
        answer, strategy = _attempt(event, rule)
        obligations.append({
            "case": case, "family": FAMILIES[case],
            "batch": event["batch"], "rule": rule,
            "certified": answer["status"] == "EQUIVALENT_IN_FRAGMENT",
            "strategy": strategy,
            "reason": answer.get("reason", "ACCEPTED") if
                      answer["status"] != "EQUIVALENT_IN_FRAGMENT" else "PROVED_V5",
        })

    report_cases = []
    for case in sorted(CHALLENGE):
        result = suffixes[case]
        report_cases.append({
            "case": case, "family": FAMILIES[case],
            "original_v5_suffix_proved": result["proved"],
            "original_reason": result["reason"],
            "later_batches": result["later_batch_diagnostics"],
            "unproved_local_obligations": [
                o for o in obligations if o["case"] == case and not o["certified"]],
        })
    rejected = Counter((o["family"], o["rule"], o["reason"])
                       for o in obligations if not o["certified"])
    original["round_z_posthoc_failure_localization"] = {
        "status": "SEPARATE_POSTHOC_V5_UNKNOWN_DIAGNOSTIC",
        "unmodified_w32_challenge_unknown_count": sum(
            not x["original_v5_suffix_proved"] for x in report_cases),
        "challenge_cases": report_cases,
        "rejected_rules_by_family": [
            {"family": f, "rule": rule, "reason": reason, "count": n}
            for (f, rule, reason), n in sorted(rejected.items())],
        "observed_challenge_changed_rule_steps": len(obligations),
        "limitations": [
            "Retrospective diagnostics only; NO upgrade to any frozen W outcome",
            "Rule identities are source-pinned; no new v6 acceptance rule",
            "Raw trace, SQL, AST and literal values are discarded after private scoring",
            "A missing event in an observer cannot be detected from this summary alone",
        ],
    }
    return original


run_external.union_coverage = score_z
run_external.MANIFEST = ROOT / "tools/queryproof_prospective_w_20261020/CASES.json"
run_external.PROTOCOL = ROOT / "tools/queryproof_prospective_w_20261020/PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts/queryproof_round_z_w32_posthoc_obligation_report.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
