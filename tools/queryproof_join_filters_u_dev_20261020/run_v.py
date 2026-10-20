"""Round V posthoc M join grouping closure, isolated from historical holdouts.

Composes a new join-scope grouping law only after the existing old checker
abstains. The M cases and SQL were seen long before this development.
"""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for name in ("queryproof_join_filters_u_dev_20261020",
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
import run_u
from rank_ast import decode
from run_holdout import _rule_basename
from grouping_repetition_certificate import certify_grouping_repetition_event
from join_grouping_v1 import certify_join_group_dedup_event
from scope_ledger_v4 import score_scope_trace as score_v4
from scope_ledger_v5 import score_scope_trace as score_v5
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy


def _changed_group_event_mutations(event):
    reasons = {}
    for name in ("new_group_exprid_changed", "all_groups_removed",
                 "right_outer_not_admitted", "join_nullsafe_key",
                 "input_source_changed", "aggregate_output_changed"):
        mutant = deepcopy(event)
        newer = [n for n in mutant["new"] if isinstance(n, dict)]
        aggs = [n for n in newer if n.get("class", "").endswith(".Aggregate")]
        joins = [n for n in newer if n.get("class", "").endswith(".Join")]
        leaves = [n for n in newer if n.get("class", "").endswith(".LogicalRDD")]
        assert len(aggs) == len(joins) == 1 and leaves
        aggregate = aggs[0]
        if name == "new_group_exprid_changed":
            refs = [x for g in aggregate["groupingExpressions"] for x in g
                    if x.get("class", "").endswith(".AttributeReference")]
            assert refs
            refs[0]["exprId"]["id"] += 700001
        elif name == "all_groups_removed":
            aggregate["groupingExpressions"] = []
        elif name == "right_outer_not_admitted":
            for plan in ("old", "new"):
                n = next(n for n in mutant[plan] if isinstance(n, dict)
                         and n.get("class", "").endswith(".Join"))
                n["joinType"] = {"object": "org.apache.spark.sql.catalyst.plans.RightOuter$"}
        elif name == "join_nullsafe_key":
            for plan in ("old", "new"):
                n = next(n for n in mutant[plan] if isinstance(n, dict)
                         and n.get("class", "").endswith(".Join"))
                n["condition"][0]["class"] = (
                    "org.apache.spark.sql.catalyst.expressions.EqualNullSafe")
        elif name == "input_source_changed":
            leaves[0]["__mutated_source__"] = True
        elif name == "aggregate_output_changed":
            aggregate["__mutated_output__"] = True
        verdict = certify_join_group_dedup_event(mutant)
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            raise AssertionError("UNSOUND_JOIN_GROUP_MUTANT_ACCEPTED:" + name)
        reasons[name] = verdict.get("reason", "UNKNOWN")
    return reasons


def score_v(path):
    original = run_u.diagnostic(path)
    grouping = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if event.get("case") not in ("M29", "M33"):
            continue
        if _rule_basename(event["rule"]) != "RemoveRepetitionFromGroupExpressions":
            continue
        old = certify_grouping_repetition_event(event)
        new = certify_join_group_dedup_event(event)
        grouping.append({
            "case": event["case"],
            "rule": "RemoveRepetitionFromGroupExpressions",
            "old_checker": old["status"],
            "old_reason": old.get("reason"),
            "join_checker": new,
            "negative_rejections": (_changed_group_event_mutations(event)
                                    if new["status"] == "EQUIVALENT_IN_FRAGMENT"
                                    else {}),
        })
    old = score_v4(path)
    new = score_v5(path)
    state = score_state_witness(path, new)
    frame = score_frame(path, new)
    inp = score_input_trace(path, new)
    all_rule = audit_all_rules(path, new)
    tags = score_taxonomy(path, new)
    original["round_v"] = {
        "status": "POSTHOC_JOIN_GROUPING_V5_DEVELOPMENT_ONLY",
        "grouping_events": grouping,
        "grouping_local_proved": sum(
            x["join_checker"]["status"] == "EQUIVALENT_IN_FRAGMENT" for x in grouping),
        "grouping_negative_rejections": sum(
            len(x["negative_rejections"]) for x in grouping),
        "prior_v4_complete_suffixes": old["certified_changed_optimizer_suffixes"],
        "new_v5_complete_suffixes": new["certified_changed_optimizer_suffixes"],
        "v5_suffixes": new["optimizer_suffix_case_counts"],
        "v5_unknown_rules": new["unknown_rules"],
        "v5_scope_status": new["status"],
        "v5_scope_errors": new["errors"],
        "necessary_guard_statuses": {
            "state": state.get("status"), "frame": frame.get("status"),
            "rdd_input": inp.get("status"), "all_rule": all_rule.get("status"),
            "tags": tags.get("status"),
        },
        "necessary_guard_counts": {
            "frame": frame.get("newly_frame_preserved_count"),
            "rdd_input": inp.get("input_identity_eligible_conditional_certificates"),
            "all_rule": all_rule.get("candidates_without_observed_silent_drift"),
            "tags": tags.get("candidate_cases_passing_tag_shape_necessary_gate"),
        },
        "limitations": [
            "Posthoc development on previously seen M29/M33 SQL, not fresh prospective proof",
            "Full suffix is a defined logical row-bag certificate only",
            "Source RDD, hidden state, full rule/tag/frame gates necessary, not sufficient",
            "R32 first-run outcomes and frozen M/O holdout remain unchanged",
        ],
    }
    return original


run_external.union_coverage = score_v
run_external.MANIFEST = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_v_posthoc_v5.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
