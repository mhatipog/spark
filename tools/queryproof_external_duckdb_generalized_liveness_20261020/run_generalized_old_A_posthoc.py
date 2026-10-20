"""Post-hoc diagnostic only: generalized theorem on previously observed A01-A15.

This is NOT an independent or prospective confirmation. It answers only
whether the new compositional checker covers the A10 abstention that inspired
its design. Both the earlier 5/8 and this new retrospective score are retained.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external

OLD = ROOT / "tools" / "queryproof_external_duckdb_column_liveness_20261019"
run_external.MANIFEST = OLD / "CASES.json"
run_external.PROTOCOL = OLD / "FROZEN_PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_generalized_old_A_replay_DIAGNOSTIC.json"
run_external.DIAGNOSTIC_GENERALIZED = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
