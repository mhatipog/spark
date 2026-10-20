"""Replay the preregistered 48 unseen generated grouping-key SQL cases.
Intentionally targeted and correlated; not independent third-party authored SQL.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "FROZEN_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_grouping_repetition_20261020.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
