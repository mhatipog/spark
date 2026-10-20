"""Add a boundary ledger to the frozen H-cohort private-run scorer.

The frozen verifier, verifier hashes, generated SQL, and reference oracle are
untouched. This pass is retrospective and never counted as held-out success.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external
from scope_ledger import score_scope_trace
sys.path.insert(0, str(ROOT / "tools" / "queryproof_composition_20261020"))
from chain_connectivity import score_chain_trace


original_union = run_external.union_coverage


def union_with_scopes(private_trace_path):
    report = original_union(private_trace_path)
    report["strict_chain_diagnostic"] = score_chain_trace(private_trace_path)
    report["hierarchical_scope_ledger"] = score_scope_trace(private_trace_path)
    return report


run_external.union_coverage = union_with_scopes
run_external.MANIFEST = (
    ROOT / "tools" / "queryproof_grouping_repetition_round2_20261020" / "CASES.json")
run_external.PROTOCOL = (
    ROOT / "tools" / "queryproof_composition_20261020" / "DIAGNOSTIC_PROTOCOL.json")
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_hierarchical_scopes.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
