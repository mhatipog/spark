"""Round U posthoc M join inference + filter commutation chain.

The frozen independent R32 holdout and historic M score are NOT rewritten.
All new controls are mutations of real observed Spark plan events, not new JVM
counterexamples or prospective tests.
"""
from copy import deepcopy
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for sub in ("queryproof_join_suffix_s_dev_20261020",
            "queryproof_join_filters_u_dev_20261020",
            "queryproof_join_pruning_q_dev_20261020",
            "queryproof_round_n_dev_20261020",
            "queryproof_external_postgresql_join_20261019",
            "queryproof_context_validator", "queryproof_instrumented",
            "queryproof_holdout_20261018", "queryproof_scopes_20261020",
            "queryproof_state_witness_20261020", "queryproof_state_frame_20261020",
            "queryproof_adversarial_20261020"):
    sys.path.insert(0, str(ROOT / "tools" / sub))

import run_external
import run_s
from rank_ast import decode
from run_holdout import _rule_basename
from filter_steps_v1 import (certify_inferred_filter_event,
                             certify_filter_project_push_event)
from scope_ledger_v3 import score_scope_trace as score_v3
from scope_ledger_v4 import score_scope_trace as score_v4
from state_guard import score_state_witness
from frame_guard import score_frame
from replay_input_h import score_input_trace
from replay_silent_h import audit_all_rules
from tag_taxonomy_h import score_taxonomy

BASE = run_external.union_coverage
RULES = {
    "InferFiltersFromConstraints": certify_inferred_filter_event,
    "PushDownPredicates": certify_filter_project_push_event,
}


def _negative_mutations(event, checker):
    result = {}
    for name in ("null_guard_inverted", "guard_exprid_changed",
                 "source_leaf_changed", "join_type_changed",
                 "outer_frame_changed"):
        x = deepcopy(event)
        nodes = [n for n in x["new"] if isinstance(n, dict)]
        filters = [n for n in nodes if n.get("class", "").endswith(".Filter")]
        leaves = [n for n in nodes if n.get("class", "").endswith(".LogicalRDD")]
        joins = [n for n in nodes if n.get("class", "").endswith(".Join")]
        aggs = [n for n in nodes if n.get("class", "").endswith(".Aggregate")]
        assert filters and leaves and len(joins) == len(aggs) == 1
        if name == "null_guard_inverted":
            filters[0]["condition"][0]["class"] = (
                "org.apache.spark.sql.catalyst.expressions.IsNull")
        elif name == "guard_exprid_changed":
            filters[0]["condition"][1]["exprId"]["id"] += 90001
        elif name == "source_leaf_changed":
            leaves[0]["__not_the_same_backing_source__"] = True
        elif name == "join_type_changed":
            joins[0]["joinType"] = {
                "object": "org.apache.spark.sql.catalyst.plans.FullOuter$"}
        elif name == "outer_frame_changed":
            aggs[0]["__changed_aggregate_frame__"] = True
        verdict = checker(x)
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            raise AssertionError("FALSE_ACCEPT_MUTATED_JOIN_FILTER_STEP:" + name)
        result[name] = verdict.get("reason", "UNKNOWN")
    return result



def _attribute_identity_diagnostics(event):
    """Report ONLY which fields agree across matched Project and Filter refs."""
    from collapse_project_certificate import _identity
    new = [node for node in event["new"] if isinstance(node, dict)]
    candidates = [
        item[0] for p in new if p.get("class", "").endswith(".Project")
        for item in p.get("projectList", [])
        if isinstance(item, list) and len(item) == 1
        and item[0].get("class", "").endswith(".AttributeReference")
    ]
    observations = []
    for f in new:
        if not f.get("class", "").endswith(".Filter"):
            continue
        cond = f.get("condition", [])
        if not isinstance(cond, list) or len(cond) != 2:
            continue
        ref = cond[1]
        if not isinstance(ref, dict) or not ref.get("class", "").endswith(".AttributeReference"):
            continue
        same = [p for p in candidates if _identity(p) == _identity(ref)]
        observations.append({
            "same_exprid_project_candidates": len(same),
            "filter_keys": sorted(ref),
            "candidate_project_keys": [sorted(p) for p in same],
            "differing_fields": [
                sorted(k for k in set(ref) | set(p) if ref.get(k) != p.get(k))
                for p in same
            ],
        })
    return observations


def diagnostic(path):
    result = BASE(path)
    local = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        rule = _rule_basename(event["rule"])
        if event.get("case") not in ("M29", "M33") or rule not in RULES:
            continue
        checker = RULES[rule]
        verdict = checker(event)
        local.append({
            "case": event["case"],
            "rule": rule,
            "verdict": verdict,
            "redacted_attribute_identity_diagnostics": _attribute_identity_diagnostics(event),
            "negative_rejections": (
                _negative_mutations(event, checker)
                if verdict["status"] == "EQUIVALENT_IN_FRAGMENT" else {}),
        })
    old = score_v3(path)
    new = score_v4(path)
    state = score_state_witness(path, new)
    frame = score_frame(path, new)
    inp = score_input_trace(path, new)
    all_rule = audit_all_rules(path, new)
    tags = score_taxonomy(path, new)
    result["round_u"] = {
        "status": "POSTHOC_DEV_LOCAL_FILTER_THEOREMS_ONLY",
        "local_filter_rule_events": local,
        "local_events_proved": sum(
            e["verdict"]["status"] == "EQUIVALENT_IN_FRAGMENT" for e in local),
        "negative_rejections": sum(len(e["negative_rejections"]) for e in local),
        "v3_suffix_cases": old.get("optimizer_suffix_case_counts"),
        "v4_suffix_cases": new.get("optimizer_suffix_case_counts"),
        "v3_certified_suffixes": old.get("certified_changed_optimizer_suffixes"),
        "v4_certified_suffixes": new.get("certified_changed_optimizer_suffixes"),
        "v4_status": new.get("status"),
        "v4_unknown_rule_types": new.get("unknown_rules"),
        "all_guard_statuses": {
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
            "New v4 verifier development only, all four examples previously observed M",
            "R32 and original M/O holdout numbers unchanged",
            "No raw Catalyst AST, data or RDD identity emitted",
            "Independent guards necessary not sufficient",
            "No runtime error, order or UDF proof",
        ]
    }
    return result


run_external.union_coverage = diagnostic
run_external.MANIFEST = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_u_posthoc_v4.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
