"""Prospectively fixed fifth Spark INNER JOIN WHERE-transport experiment."""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "tools" / "queryproof_holdout_20261018"))
from run_holdout import parent

if __name__ == "__main__":
    raise SystemExit(parent(
        HERE / "CASES.json", HERE / "FROZEN_PROTOCOL.json",
        ROOT / "artifacts" / "queryproof_transport_confirmation_20261019.json"))
