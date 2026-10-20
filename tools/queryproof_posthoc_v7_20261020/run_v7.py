"""Round V7: strict effective-rule completeness audit, POSTHOC, W32 reused.

Never promote retrospective counts to the frozen W32 prospective first run.
Compare V6's permissive identity-boundary summaries against a source-versioned
auditor that requires *every* observed post-cut changed rule to carry its
own structural certificate, even in net-identity intermediate batches.
"""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
for name in ("queryproof_posthoc_v7_20261020",
             "queryproof_posthoc_v6_20261020",
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
import run_v6
from scope_ledger_v7_all_steps import score_scope_trace as strict_score


def score_v7(path):
    past = run_v6.score_v6(path)
    old = past["v6_posthoc_predicate_dependency"]
    now = strict_score(path)
    assert now["status"] == "POSTHOC_STRICT_EVENT_COMPLETENESS_AUDIT", now
    strict = {x["case"] for x in now["optimizer_suffix_case_counts"]
              if x["changed"] and x["proved"]}
    prior = set(old["v6_structural_case_ids"])
    assert strict <= prior, "STRICT_LEDGER_ADDED_NEW_CANDIDATES"
    post_bypasses = now["postcut_identity_bypass_events"]
    past["v7_posthoc_strict_interior_rule_audit"] = {
        "status": "DEVELOPMENT_POSTHOC_STRICT_EFFECTIVE_RULE_AUDIT",
        "v6_observational_structural_cases": sorted(prior),
        "strict_event_complete_cases": sorted(strict),
        "strict_event_complete_count": len(strict),
        "previously_positive_now_rejected": sorted(prior - strict),
        "postcut_identity_unknown_cycle_observations": post_bypasses,
        "postcut_identity_unknown_case_ids": sorted(
            {x["case"] for x in post_bypasses}),
        "cases_with_identity_unknown_cycles_and_v6_positive": sorted(
            prior & {x["case"] for x in post_bypasses}),
        "strict_suffix_reasons": now["optimizer_suffix_reason_counts"],
        "legacy_x_strict_hidden_state_count": len(
            old["v6_legacy_strict_state_case_ids"]),
        "definitions": {
            "old": "v6 accepts exact AST identity at batch/executor/suffix even if effective enclosed events lack rule certificates",
            "new": "strict event-complete requires each observed effective event, including net cycles, and every batch boundary to connect",
        },
        "limitations": [
            "This is stricter than an extensional plan equivalence theorem",
            "Identity at boundaries could still be semantically sound if serialization and hidden-effect congruence are proven",
            "Recorded rule events are only those observed, not a guarantee of no unseen JVM effects",
            "Strict count measures trace proof completeness, not query-result discrepancies",
            "All W32 cases are reused, not new prospective cases; no frozen result revised",
        ],
    }
    return past


run_external.union_coverage = score_v7
run_external.MANIFEST = ROOT / "tools/queryproof_prospective_w_20261020/CASES.json"
run_external.PROTOCOL = ROOT / "tools/queryproof_prospective_w_20261020/PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts/queryproof_v7_w32_posthoc_strict_interiors.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
