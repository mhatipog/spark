"""V9 POSTHOC inequality fragment replay, explicitly compared with validated V8.
Only direct strict LessThan(AttributeReference,AttributeReference) on integral
cross-side keys; OR and null-safe comparisons are NOT certified here.
"""
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_v7_strict_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_v9_inequality_20261020"))
import run_v8
import run_external
from rank_ast import decode
from run_holdout import _rule_basename
from scope_ledger_v9 import score_scope_trace as score_v9_scope
from join_project_v9 import certify_join_pruning_event
from filter_steps_v9 import (
    certify_inferred_filter_event, certify_filter_project_push_event)

LT_CASES = {"W17", "W18", "W19", "W20"}
CHECKERS = {
    "ColumnPruning": certify_join_pruning_event,
    "InferFiltersFromConstraints": certify_inferred_filter_event,
    "PushDownPredicates": certify_filter_project_push_event,
}

def nullsafe_mutant(event):
    altered = copy.deepcopy(event)
    for side in ("old", "new"):
        joins = [n for n in altered[side]
                 if isinstance(n, dict) and
                 n.get("class", "").endswith(".Join")]
        if len(joins) != 1:
            return None
        raw = joins[0].get("condition")
        if not (isinstance(raw, list) and len(raw) == 3 and
                isinstance(raw[0], dict) and
                raw[0].get("class", "").endswith(".LessThan")):
            return None
        raw[0]["class"] = (
            "org.apache.spark.sql.catalyst.expressions.EqualNullSafe")
    return altered

def score_v9(path):
    report = run_v8.strict_score(path)
    current = score_v9_scope(path)
    if current["status"] != "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER":
        raise RuntimeError("V9_INVALID_SCOPE: " + repr(current))
    old_ids = set(report["v8_strict_all_steps_replay"]["v8_strict_case_ids"])
    new_ids = {
        row["case"] for row in current["optimizer_suffix_case_counts"]
        if row["changed"] and row["proved"]
    }
    if not old_ids <= new_ids:
        raise RuntimeError("V9_REGRESSED_STRICT_V8_CASES")
    controls = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        name = _rule_basename(event["rule"])
        if event["case"] not in LT_CASES or name not in CHECKERS or (
                event["old"] == event["new"]):
            continue
        mutated = nullsafe_mutant(event)
        if mutated is None:
            continue
        check = CHECKERS[name]
        positive = check(event)["status"]
        negative = check(mutated)["status"]
        controls.append({
            "case": event["case"], "rule": name,
            "real_event_status": positive,
            "nullsafe_substitution_status": negative,
        })
        if negative != "UNKNOWN":
            raise RuntimeError("UNSOUND_V9_NULLSAFE_MUTANT_ACCEPTED")
    if len(controls) < 12:
        raise RuntimeError("V9_MISSING_INEQUALITY_EVENT_CONTROLS")
    report["v9_direct_inequality_dev"] = {
        "status": "POSTHOC_ADVERSARIAL_REPLAY_ONLY",
        "strict_v8_reference_count": len(old_ids),
        "v9_strict_count": len(new_ids),
        "v9_strict_case_ids": sorted(new_ids),
        "newly_covered_cases": sorted(new_ids - old_ids),
        "regressed_cases": sorted(old_ids - new_ids),
        "unproved_cases": sorted({
            row["case"] for row in current["optimizer_suffix_case_counts"]
            if row["changed"] and not row["proved"]
        }),
        "inequality_local_controls": controls,
        "strict_suffix_reason_counts": current["optimizer_suffix_reason_counts"],
        "rejected_case_unknown_rules": [
            x for x in current["unknown_rule_case_inventory"]
            if x["case"] not in new_ids
        ],
        "limits": [
            "W32 pre-seen; posthoc only, never a prospective score",
            "Coq strict comparison NULL lemma is abstract, no Catalyst AST interpretation verified",
            "No arbitrary type, expression, UDF, overflow, error or hidden-state proof",
            "ONLY direct integral LessThan allowed for inference; OR and nullsafe deliberately excluded",
        ],
    }
    return report

run_external.union_coverage = score_v9
run_external.OUTPUT = ROOT / "artifacts/queryproof_v9_w32_direct_strict_inequality.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
