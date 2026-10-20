"""Score plan-continuous proof chains from private raw Catalyst traces.

The original frozen five-checker union remains unchanged. Append only
sanitized chain statistics while the private trace exists temporarily.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external
from chain_connectivity import score_chain_trace

original_union_coverage = run_external.union_coverage


def union_with_chain(private_trace_path):
    original = original_union_coverage(private_trace_path)
    original["strict_chain_diagnostic"] = score_chain_trace(private_trace_path)
    return original


run_external.union_coverage = union_with_chain
run_external.MANIFEST = (
    ROOT / "tools" / "queryproof_grouping_repetition_round2_20261020" / "CASES.json")
run_external.PROTOCOL = HERE / "DIAGNOSTIC_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_exact_chain_diagnostic.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
