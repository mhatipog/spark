"""V19 held-out cohort: frozen V17 templates plus ON clauses with one-side
conjuncts (to exercise ON-clause pushdown), new seed and fresh data.
Writes COHORT.json. Never executes queries.
"""
import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_v17_heldout_20261021"))
import gen_cohort as v17  # noqa: E402

SEED = 20261023
N_QUERIES = 150


def on_with_local(rng):
    """Cross-side ON predicate plus one or two single-side ON conjuncts."""
    cond = v17.on_condition(rng)
    for _ in range(rng.randint(1, 2)):
        cond += " AND " + v17.atom(rng, [rng.choice(["ta", "tb"])])
    return cond


def query(rng):
    if rng.random() < 0.3:
        join = rng.choice(["JOIN", "JOIN", "LEFT JOIN"])
        wh = v17.where(rng, ["ta", "tb"], 2)
        return (f"SELECT ta.a, tb.b FROM ta {join} tb ON {on_with_local(rng)}"
                + (f" WHERE {wh}" if wh else ""))
    return v17.query(rng)


def main(out):
    rng = random.Random(SEED)
    tables = {name: v17.table(rng, cols) for name, cols in v17.COLS.items()}
    cases = [{"id": f"Q{i:03d}", "sql": query(rng), "tables": tables}
             for i in range(1, N_QUERIES + 1)]
    doc = {"schema_version": 1, "benchmark_id": "queryproof-v19-wide-heldout-20261022",
           "nature": "HELD-OUT cohort, generated and frozen before any execution",
           "seed": SEED, "cases": cases}
    Path(out).write_bytes(json.dumps(doc, indent=1).encode("utf-8"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(HERE / "COHORT.json"))
