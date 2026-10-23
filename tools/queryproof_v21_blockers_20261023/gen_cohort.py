"""V21 held-out cohort: the frozen V19 templates (incl. one-side ON conjuncts)
with a NEW seed and fresh data. Writes COHORT.json. Never executes queries.
"""
import json
import random
import sys
from pathlib import Path

import importlib.util

HERE = Path(__file__).resolve().parent
# The V19 generator imports V17's as `gen_cohort`; this file shares that name,
# so put V17's directory first and load both by explicit path.
sys.path.insert(0, str(HERE.parent / "queryproof_v17_heldout_20261021"))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


v17 = _load("v17gen", HERE.parent / "queryproof_v17_heldout_20261021" / "gen_cohort.py")
sys.modules["gen_cohort"] = v17
v19 = _load("v19gen", HERE.parent / "queryproof_v19_wide_20261022" / "gen_cohort.py")

SEED = 20261025
N_QUERIES = 150


def main(out):
    rng = random.Random(SEED)
    tables = {name: v17.table(rng, cols) for name, cols in v17.COLS.items()}
    cases = [{"id": f"B{i:03d}", "sql": v19.query(rng), "tables": tables}
             for i in range(1, N_QUERIES + 1)]
    doc = {"schema_version": 1, "benchmark_id": "queryproof-v21-blockers-heldout-20261023",
           "nature": "HELD-OUT cohort, generated and frozen before any execution",
           "seed": SEED, "cases": cases}
    Path(out).write_bytes(json.dumps(doc, indent=1).encode("utf-8"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(HERE / "COHORT.json"))
