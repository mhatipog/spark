"""Round Q: replay ONLY already observed M29/M33 events; diagnostic not holdout."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for sub in ("queryproof_external_postgresql_join_20261019",
            "queryproof_context_validator", "queryproof_instrumented",
            "queryproof_scopes_20261020", "queryproof_holdout_20261018",
            "queryproof_state_witness_20261020", "queryproof_state_frame_20261020",
            "queryproof_adversarial_20261020", "queryproof_round_n_dev_20261020"):
    sys.path.insert(0, str(ROOT / "tools" / sub))
sys.path.insert(0, str(HERE))

import run_external
from rank_ast import decode
from run_holdout import _rule_basename
from join_project_v1 import certify_join_pruning_event
from collapse_project_certificate import _identity

ORIGINAL = run_external.union_coverage


def _nodes(event, name):
    return [node for node in event[name] if isinstance(node, dict)]


def _structural_fingerprint(event):
    old, new = _nodes(event, "old"), _nodes(event, "new")
    def ops(items):
        return [x["class"].rsplit(".", 1)[-1] for x in items
                if (x.get("class", "").startswith("org.apache.spark.sql.catalyst.plans.logical.")
                    or x.get("class", "") == "org.apache.spark.sql.execution.LogicalRDD")]
    joins = [x for x in new if x.get("class", "").endswith(".Join")]
    projects = [x for x in new if x.get("class", "").endswith(".Project")]
    return {
        "old_operator_shapes": ops(old),
        "new_operator_shapes": ops(new),
        "join_keys": sorted(joins[0]) if joins else [],
        "join_type": joins[0].get("joinType", {}).get("object", "").rsplit(".", 1)[-1] if joins else None,
        "condition_classes": [x.get("class", "").rsplit(".", 1)[-1]
                              for x in joins[0].get("condition", [])] if joins else [],
        "condition_node_keysets": [sorted(x) for x in joins[0].get("condition", [])] if joins else [],
        "project_output_lengths": [len(x.get("projectList", [])) for x in projects],
        "project_expression_first_classes": [
            [entry[0].get("class", "").rsplit(".", 1)[-1]
             for entry in p.get("projectList", [])] for p in projects],
    }


def _adversarial_negative_controls(event):
    probes = {}
    mutations = (
        "drop_left_join_key", "drop_right_join_key",
        "drop_top_aggregate_live", "join_type_changed",
        "join_condition_changed", "source_leaf_changed",
        "not_a_pure_project", "project_exprid_corrupted",
        "aggregate_frame_changed", "sort_frame_changed"
    )
    for name in mutations:
        x = deepcopy(event)
        nodes = _nodes(x, "new")
        p = [n for n in nodes if n.get("class", "").endswith(".Project")]
        joins = [n for n in nodes if n.get("class", "").endswith(".Join")]
        agg = [n for n in nodes if n.get("class", "").endswith(".Aggregate")]
        sorts = [n for n in nodes if n.get("class", "").endswith(".Sort")]
        leaves = [n for n in nodes if n.get("class", "").endswith(".LogicalRDD")]
        assert len(p) == 3 and len(joins) == len(agg) == len(sorts) == 1
        assert len(leaves) == 2
        if name == "drop_left_join_key":
            key = _identity(joins[0]["condition"][1])
            p[1]["projectList"] = [item for item in p[1]["projectList"]
                                    if _identity(item[0]) != key]
        elif name == "drop_right_join_key":
            key = _identity(joins[0]["condition"][2])
            p[2]["projectList"] = [item for item in p[2]["projectList"]
                                    if _identity(item[0]) != key]
        elif name == "drop_top_aggregate_live":
            p[0]["projectList"] = []
        elif name == "join_type_changed":
            joins[0]["joinType"] = {"object": "org.apache.spark.sql.catalyst.plans.FullOuter$"}
        elif name == "join_condition_changed":
            joins[0]["condition"][0]["class"] = "org.apache.spark.sql.catalyst.expressions.GreaterThan"
        elif name == "source_leaf_changed":
            leaves[0]["__different_source__"] = True
        elif name == "not_a_pure_project":
            p[1]["__distinct_rewrite__"] = True
        elif name == "project_exprid_corrupted":
            assert p[1]["projectList"]
            p[1]["projectList"][0][0]["exprId"]["id"] += 100000
        elif name == "aggregate_frame_changed":
            agg[0]["__altered_aggregation__"] = True
        elif name == "sort_frame_changed":
            sorts[0]["__altered_sort__"] = True
        if name.startswith("drop_") and name.endswith("join_key"):
            assert len(p[1 if name.startswith("drop_left") else 2]["projectList"]) >= 0
        verdict = certify_join_pruning_event(x)
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            raise AssertionError("UNSAFE_JOIN_FALSE_ACCEPT:" + name)
        probes[name] = verdict.get("reason", "UNKNOWN")
    return probes


def diagnostic(path):
    output = ORIGINAL(path)
    relevant = {}
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        case = event.get("case")
        if case not in ("M29", "M33") or _rule_basename(event["rule"]) != "ColumnPruning":
            continue
        verdict = certify_join_pruning_event(event)
        relevant[case] = {
            "checker": verdict,
            "redacted_structure": _structural_fingerprint(event),
            "adversarial_negative_rejections":
                _adversarial_negative_controls(event)
                if verdict["status"] == "EQUIVALENT_IN_FRAGMENT" else {},
        }
    output["round_q_posthoc"] = {
        "status": "POSTHOC_DEVELOPMENT_ONLY_NO_HOLDOUT_CREDIT",
        "cases": relevant,
        "executed_original_M_cases": ["M17", "M21", "M29", "M33"],
        "local_join_events_certified": sum(
            r["checker"]["status"] == "EQUIVALENT_IN_FRAGMENT"
            for r in relevant.values()),
        "negative_mutation_rejections": sum(
            len(r["adversarial_negative_rejections"]) for r in relevant.values()),
        "not_full_optimizer_suffix_proof": True,
        "no_raw_ast_or_relation_values_in_output": True,
    }
    return output


run_external.union_coverage = diagnostic
run_external.MANIFEST = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_q_posthoc_diagnostic.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
