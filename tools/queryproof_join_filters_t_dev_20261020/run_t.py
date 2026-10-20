"""Round T sanitized topology for the last two known join chain blockers.

Replays exclusively seen M29/M33 events. No raw AST, exprId, data, rule
arguments, or contents are exported. No previous holdout scores modified.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for sub in ("queryproof_join_suffix_s_dev_20261020",
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
from join_prefilter_certificate import tree_from_preorder

BASE = run_external.union_coverage
WATCH = {"InferFiltersFromConstraints", "PushDownPredicates"}


def _shape(tree):
    header, children = tree
    short = str(header.get("class", "?")).rsplit(".", 1)[-1]
    entry = {"node": short}
    if short in ("Filter", "Join", "Project", "Aggregate"):
        entry["keys"] = sorted(header.keys())
    if short in ("Filter", "Join"):
        condition = header.get("condition", [])
        if isinstance(condition, list):
            entry["condition_classes"] = [
                str(expr.get("class", "")).rsplit(".", 1)[-1]
                for expr in condition if isinstance(expr, dict)]
    if short == "Project":
        entry["output_count"] = len(header.get("projectList", []))
    if children:
        entry["children"] = [_shape(child) for child in children]
    return entry


def diagnostic(path):
    result = BASE(path)
    evidence = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        e = decode(line)
        rule = _rule_basename(e["rule"])
        if e.get("case") not in ("M29", "M33") or rule not in WATCH:
            continue
        evidence.append({
            "case": e["case"],
            "rule": rule,
            "old": _shape(tree_from_preorder(e["old"])),
            "new": _shape(tree_from_preorder(e["new"])),
        })
    result["round_t_sanitized_unknown_filter_transitions"] = {
        "status": "POSTHOC_DEVELOPMENT_ONLY",
        "events": evidence,
        "event_count": len(evidence),
        "privacy": "Only operator shapes, expression classes, key names, and output counts",
        "hypotheses": [
            "Inner join allows guards implied by a TRUE join predicate",
            "Left outer join permits right-side guard if never true for removed right rows",
            "No left preservation without proof; predicate movement requires full NULL treatment"
        ],
    }
    return result


run_external.union_coverage = diagnostic
run_external.MANIFEST = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_round_n_dev_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_t_sanitized_shapes.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
