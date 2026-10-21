"""V18 held-out cohort: the frozen V17 templates with a NEW seed (fresh data
and queries). Writes COHORT.json. Never executes queries.
"""
import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_v17_heldout_20261021"))
import gen_cohort as v17  # noqa: E402

SEED = 20261022
N_QUERIES = 150


def main(out):
    rng = random.Random(SEED)
    tables = {name: v17.table(rng, cols) for name, cols in v17.COLS.items()}
    cases = [{"id": f"K{i:03d}", "sql": v17.query(rng), "tables": tables}
             for i in range(1, N_QUERIES + 1)]
    doc = {"schema_version": 1, "benchmark_id": "queryproof-v18-validated-heldout-20261021",
           "nature": "HELD-OUT cohort, generated and frozen before any execution",
           "seed": SEED, "cases": cases}
    Path(out).write_bytes(json.dumps(doc, indent=1).encode("utf-8"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(HERE / "COHORT.json"))
