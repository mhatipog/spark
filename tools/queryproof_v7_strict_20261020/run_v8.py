"""QueryProof V8: strict retrospective W32 replay against the UNMODIFIED V6 scorer.
The historical 20/32 score is a comparator, never a V8 score. No ASTs exported.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_posthoc_v6_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_v7_strict_20261020"))
import run_v6
import run_external
from scope_ledger_v7 import score_scope_trace as score_strict

original_score_v6 = run_v6.score_v6

def strict_score(path):
    report = original_score_v6(path)
    old_cases = set(report["v6_posthoc_predicate_dependency"]["v6_structural_case_ids"])
    strict = score_strict(path)
    if strict["status"] != "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER":
        raise RuntimeError("STRICT_TRACE_NOT_COMPLETE: " + repr(strict))
    strict_cases = {
        row["case"] for row in strict["optimizer_suffix_case_counts"]
        if row["changed"] and row["proved"]
    }
    all_cases = {row["case"] for row in strict["optimizer_suffix_case_counts"]}
    rejected_cases = all_cases - strict_cases
    all_rule_status = report["v6_posthoc_predicate_dependency"]["v6_observer_statuses"]["all_rule"]
    if all_rule_status != "COMPLETE_ALL_RULE_AUDIT":
        raise RuntimeError("RULE_AUDIT_DID_NOT_ESTABLISH_EVENT_COMPLETENESS: " + all_rule_status)
    if not strict_cases <= old_cases:
        raise RuntimeError("STRICT_VERIFIER_ACCEPTED_CASE_NOT_ACCEPTED_BY_V6")
    if len(old_cases) != 20:
        raise RuntimeError("V6_REPLAY_DRIFT: expected retrospective frozen comparator 20")
    report["v8_strict_all_steps_replay"] = {
        "status": "POSTHOC_ADVERSARIAL_REPLAY_ONLY",
        "v6_reference_count": len(old_cases),
        "v6_reference_case_ids": sorted(old_cases),
        "v8_strict_count": len(strict_cases),
        "v8_strict_case_ids": sorted(strict_cases),
        "newly_rejected_case_ids": sorted(old_cases - strict_cases),
        "rejected_case_ids": sorted(rejected_cases),
        "rejected_case_unknown_rules": [
            row for row in strict["unknown_rule_case_inventory"]
            if row["case"] in rejected_cases
        ],
        "all_rule_audit_status": all_rule_status,
        "impossible_extra_acceptances": sorted(strict_cases - old_cases),
        "strict_scope_reason_counts": strict["by_reason"],
        "strict_suffix_reason_counts": strict["optimizer_suffix_reason_counts"],
        "strict_suffix_details": strict["optimizer_suffix_case_counts"],
        "recorded_rule_events": strict["recorded_rule_events"],
        "limitations": [
            "No newly frozen prospective cases: W32 replay only",
            "No theorem connecting Catalyst hidden-state behavior to plan meaning",
            "Only effective changes according to Spark fastEquals are traced",
            "Event capture completeness requires no overflow or serialization errors",
            "Executor scopes are structural chain accounting, not a soundness theorem",
        ],
    }
    return report

run_external.union_coverage = strict_score
run_external.MANIFEST = ROOT / "tools/queryproof_prospective_w_20261020/CASES.json"
run_external.PROTOCOL = ROOT / "tools/queryproof_prospective_w_20261020/PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts/queryproof_v8_w32_strict_all_steps.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
