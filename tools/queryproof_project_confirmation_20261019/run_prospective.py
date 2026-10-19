"""Fresh preregistered 2026-10-19 projection pushdown confirmation runner.

New manifest, source rows, and protocol fixed before first Spark replay.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_holdout_20261018"))
from run_holdout import parent

if __name__ == "__main__":
    raise SystemExit(parent(
        HERE / "CASES.json",
        HERE / "FROZEN_PROTOCOL.json",
        ROOT / "artifacts" / "queryproof_project_confirmation_20261019.json"))
