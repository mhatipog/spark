"""Diagnostic coexistence of independent local certificates on seen H cases."""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

run_external.MANIFEST = (
    ROOT / "tools" / "queryproof_grouping_repetition_round2_20261020" / "CASES.json")
run_external.PROTOCOL = HERE / "DIAGNOSTIC_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_two_family_composition_diagnostic.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
