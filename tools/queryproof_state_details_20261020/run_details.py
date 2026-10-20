"""Second retrospective H/K replay with class-specific suffix evidence."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(
    0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_witness_20261020"))

import run_external
from scope_ledger import score_scope_trace
from state_guard import score_state_witness
from scope_sensitive_guard import score_details


def main(cohort):
    original = run_external.union_coverage

    def audit(path):
        report = original(path)
        scopes = score_scope_trace(path)
        report["hierarchical_scope_ledger"] = scopes
        report["posthoc_jvm_state_audit"] = score_state_witness(path, scopes)
        report["posthoc_suffix_class_details"] = score_details(path, scopes)
        return report

    run_external.union_coverage = audit
    if cohort == "H":
        run_external.MANIFEST = (
            ROOT / "tools" / "queryproof_grouping_repetition_round2_20261020"
            / "CASES.json")
        run_external.PROTOCOL = (
            ROOT / "tools" / "queryproof_composition_20261020"
            / "DIAGNOSTIC_PROTOCOL.json")
    else:
        root_k = ROOT / "tools" / "queryproof_scopes_fresh_k_20261020"
        run_external.MANIFEST = root_k / "CASES.json"
        run_external.PROTOCOL = root_k / "PROTOCOL.json"
    run_external.OUTPUT = ROOT / "artifacts" / ("queryproof_state_details_" + cohort + ".json")
    run_external.DIAGNOSTIC_GENERALIZED = True
    run_external.DIAGNOSTIC_GROUPING_REPETITION = True
    return run_external.parent()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", choices=("H", "K"), required=True)
    args = parser.parse_args()
    raise SystemExit(main(args.cohort))
