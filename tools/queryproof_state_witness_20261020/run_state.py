"""Retrospective H/K replay under the additional JVM state-witness audit.

Do not relabel either original frozen cohort as prospective for this method.
"""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(
    0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
from scope_ledger import score_scope_trace

from state_guard import score_state_witness


def main(cohort):
    original = run_external.union_coverage

    def audit(private_trace_path):
        result = original(private_trace_path)
        scopes = score_scope_trace(private_trace_path)
        result["hierarchical_scope_ledger"] = scopes
        result["posthoc_jvm_state_audit"] = score_state_witness(
            private_trace_path, scopes)
        return result

    run_external.union_coverage = audit
    if cohort == "H":
        run_external.MANIFEST = (
            ROOT / "tools" / "queryproof_grouping_repetition_round2_20261020"
            / "CASES.json")
        run_external.PROTOCOL = (
            ROOT / "tools" / "queryproof_composition_20261020"
            / "DIAGNOSTIC_PROTOCOL.json")
    elif cohort == "K":
        root_k = ROOT / "tools" / "queryproof_scopes_fresh_k_20261020"
        run_external.MANIFEST = root_k / "CASES.json"
        run_external.PROTOCOL = root_k / "PROTOCOL.json"
    else:
        raise ValueError("Unexpected cohort")
    run_external.OUTPUT = (
        ROOT / "artifacts" / ("queryproof_state_audit_" + cohort.lower() + ".json"))
    run_external.DIAGNOSTIC_GENERALIZED = True
    run_external.DIAGNOSTIC_GROUPING_REPETITION = True
    return run_external.parent()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", choices=("H", "K"), required=True)
    opts = parser.parse_args()
    raise SystemExit(main(opts.cohort))
