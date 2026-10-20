"""One-time preregistered replay of fresh grouping-key SQL cohort H01-H48.
A syntactically targeted generated suite, not a third-party corpus.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "FROZEN_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_grouping_repetition_round2_20261020.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
