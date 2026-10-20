"""Posthoc Round N diagnostic of FOUR original M cases. No M holdout credit."""
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_holdout_20261018"))

import run_external
from rank_ast import decode
from run_holdout import _rule_basename
from scope_ledger import score_scope_trace
from generalized_projection_liveness_certificate import certify_generalized_projection_liveness_event
from aggregate_framed_identity_certificate import certify_aggregate_framed_identity_event
from aggregate_identity_v2 import certify_aggregate_framed_identity_event as v2_identity

def _negative_identity_mutations(event):
    """Adversarial AST mutations of REAL Spark-reached events, never SQL holdout."""
    probes = {}
    for name in ("aggregate_changed", "identity_dropped",
                 "input_substituted", "unsafe_aggregate_operator"):
        modified = deepcopy(event)
        if name == "aggregate_changed":
            agg = next(n for n in modified["new"]
                       if str(n.get("class", "")).endswith(".Aggregate"))
            agg["__altered_semantics__"] = True
        elif name == "identity_dropped":
            identity = next(n for n in modified["old"]
                            if str(n.get("class", "")).endswith(".Project"))
            assert identity["projectList"]
            identity["projectList"] = identity["projectList"][:-1]
        elif name == "input_substituted":
            leaves = [n for n in modified["new"]
                      if str(n.get("class", "")).endswith(".LogicalRDD")]
            assert leaves
            leaves[0]["__swapped_input__"] = True
        else:
            for side in ("old", "new"):
                agg = next(n for n in modified[side]
                           if str(n.get("class", "")).endswith(".Aggregate"))
                funcs = [part for expr in agg["aggregateExpressions"]
                         for part in expr
                         if isinstance(part, dict)
                         and str(part.get("class", "")).endswith(
                             (".Count", ".Max", ".Min", ".Sum"))]
                assert funcs
                funcs[0]["class"] = (
                    "org.apache.spark.sql.catalyst.expressions.aggregate."
                    "UnsafeHypotheticalVolatile")
        verdict = v2_identity(modified)
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            raise AssertionError("UNSAFE_V2_FALSE_ACCEPT:" + name)
        probes[name] = verdict.get("reason", "UNKNOWN")
    return probes

ORIGINAL = run_external.union_coverage
CHECKS = {
    "ColumnPruning": certify_generalized_projection_liveness_event,
    "CollapseProject": certify_aggregate_framed_identity_event,
}

def diagnostic(path):
    result = ORIGINAL(path)
    per_case = defaultdict(Counter)
    safe_shape = defaultdict(list)
    v2_negative_controls = {}
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST" + chr(9)):
            continue
        event = decode(line)
        name = _rule_basename(event["rule"])
        if name not in CHECKS:
            continue
        verdict = CHECKS[name](event)
        case = event.get("case", "UNLABELED")
        key = name + ":" + (
            "CERTIFIED" if verdict["status"] == "EQUIVALENT_IN_FRAGMENT"
            else verdict.get("reason", "UNKNOWN"))
        per_case[case][key] += 1
        if name == "CollapseProject":
            v2 = v2_identity(event)
            if v2["status"] == "EQUIVALENT_IN_FRAGMENT":
                v2_negative_controls[case] = _negative_identity_mutations(event)
            per_case[case]["CollapseProjectV2:" + (
                "CERTIFIED" if v2["status"] == "EQUIVALENT_IN_FRAGMENT"
                else v2.get("reason", "UNKNOWN"))] += 1
        if len(safe_shape[case]) < 20:
            def topology(nodes):
                return [str(n.get("class", "?")).rsplit(".", 1)[-1]
                        for n in nodes if isinstance(n, dict)
                        and (str(n.get("class", "")).startswith(
                            "org.apache.spark.sql.catalyst.plans.logical.")
                             or str(n.get("class", "")) ==
                             "org.apache.spark.sql.execution.LogicalRDD")]
            safe_shape[case].append({
                "rule": name, "verdict": key,
                "before_operators": topology(event["old"])[:25],
                "after_operators": topology(event["new"])[:25],
            })
    scope = score_scope_trace(path)
    result["round_n_posthoc_diagnostic"] = {
        "status": "POSTHOC_DEVELOPMENT_ONLY_NO_PROSPECTIVE_CREDIT",
        "replayed_cases": sorted(per_case),
        "checker_reasons": {k: dict(v) for k, v in sorted(per_case.items())},
        "sanitized_rule_topologies": dict(safe_shape),
        "adversarial_negative_rejections": v2_negative_controls,
        "total_negative_rejections": sum(map(len, v2_negative_controls.values())),
        "changed_suffixes": scope.get("changed_optimizer_suffixes"),
        "certified_changed_suffixes_under_ORIGINAL_M_checkers":
            scope.get("certified_changed_optimizer_suffixes"),
        "unknown_rules": scope.get("unknown_rules"),
        "limits": ["No first-run claim", "No raw Catalyst AST or RDD contents emitted"],
    }
    return result

run_external.union_coverage = diagnostic
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_n_posthoc_diagnostic.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
