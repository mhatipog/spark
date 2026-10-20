"""Diagnostic-only replay of the already-seen 60-case Spark corpus.
These cases have no new holdout status for grouping-key deduction.
Attribute diagnostics expose only field-key shapes, never values or ExprIds.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

run_external.MANIFEST = ROOT / "tools" / "queryproof_metamorphic_liveness_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_metamorphic_liveness_20261020" / "FROZEN_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_grouping_repetition_retrospective_20261020.json"
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
