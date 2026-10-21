"""V17 held-out cohort generator (deterministic, seed frozen in PROTOCOL.json).

Fresh table names and data (never W32 or V16 probes). NULL-rich, duplicate-
rich integer tables; query templates spanning INNER/LEFT/cross/three-way joins,
strict, null-safe, multi-key and OR join conditions, one-side, cross-side, OR
and deliberately unsupported WHERE atoms, subquery filters, GROUP BY/HAVING,
DISTINCT and single-table filters. Writes COHORT.json. Never executes queries.
"""
import json
import random
import sys
from pathlib import Path

SEED = 20261021
N_QUERIES = 150
COLS = {"ta": ["a", "b", "c"], "tb": ["a", "b", "c"], "tc": ["k", "v"]}


def table(rng, cols):
    rows = []
    for _ in range(rng.randint(18, 36)):
        rows.append([None if rng.random() < 0.25 else rng.randint(-4, 4) for _ in cols])
    for _ in range(rng.randint(2, 5)):          # exact duplicate rows
        rows.append(list(rng.choice(rows)))
    rng.shuffle(rows)
    return {"columns": [{"name": c, "type": "integer"} for c in cols], "rows": rows}


def on_condition(rng, lt="ta", rt="tb"):
    op = rng.choice(["=", "=", "<", "<=", ">", ">=", "<=>"])
    lc, rc = rng.choice(COLS[lt][:2]), rng.choice(COLS[rt][:2])
    cond = f"{lt}.{lc} {op} {rt}.{rc}"
    r = rng.random()
    if r < 0.2:
        cond += f" AND {lt}.c = {rt}.c"
    elif r < 0.3:
        cond = f"({cond} OR {lt}.c = {rt}.c)"
    return cond


def atom(rng, tables):
    t = rng.choice(tables)
    c = rng.choice(COLS[t])
    lit = rng.randint(-2, 2)
    kind = rng.random()
    if kind < 0.40:
        return f"{t}.{c} {rng.choice(['<', '<=', '>', '>=', '='])} {lit}"
    if kind < 0.55 and len(tables) > 1:
        a, b = tables[0], tables[1]
        return f"{a}.c {rng.choice(['<', '>', '='])} {b}.c"
    if kind < 0.65:
        return f"{t}.{c} IS NOT NULL"
    if kind < 0.72:
        return f"{t}.{c} IS NULL"
    if kind < 0.82:
        return f"({t}.{c} > {lit} OR {t}.{c} < {-lit})"
    if kind < 0.90:
        return f"{t}.{c} + 1 > {lit}"                    # arithmetic: expected UNKNOWN
    return f"{t}.{c} IN ({lit}, {lit + 1})"              # IN: expected UNKNOWN


def where(rng, tables, k_max=3):
    k = rng.randint(0, k_max)
    return " AND ".join(atom(rng, tables) for _ in range(k))


def query(rng):
    form = rng.choice(["inner", "inner", "left", "cross", "agg", "distinct",
                       "subq", "three", "single"])
    w2 = ["ta", "tb"]
    if form in ("inner", "left"):
        join = "JOIN" if form == "inner" else "LEFT JOIN"
        wh = where(rng, w2)
        return (f"SELECT ta.a, tb.b FROM ta {join} tb ON {on_condition(rng)}"
                + (f" WHERE {wh}" if wh else ""))
    if form == "cross":
        wh = where(rng, w2, 2)
        cond = f"ta.{rng.choice(['a', 'b'])} {rng.choice(['=', '<'])} tb.{rng.choice(['a', 'b'])}"
        return f"SELECT ta.a, tb.c FROM ta, tb WHERE {cond}" + (f" AND {wh}" if wh else "")
    if form == "agg":
        join = rng.choice(["JOIN", "LEFT JOIN"])
        wh = where(rng, w2, 2)
        q = (f"SELECT ta.a, COUNT(*) FROM ta {join} tb ON {on_condition(rng)}"
             + (f" WHERE {wh}" if wh else "") + " GROUP BY ta.a")
        return q + (f" HAVING COUNT(*) > {rng.randint(0, 2)}" if rng.random() < 0.5 else "")
    if form == "distinct":
        wh = where(rng, w2, 2)
        return (f"SELECT DISTINCT ta.a, tb.b FROM ta JOIN tb ON {on_condition(rng)}"
                + (f" WHERE {wh}" if wh else ""))
    if form == "subq":
        inner = where(rng, ["ta"], 2) or "ta.b > 0"
        outer = f"s.a {rng.choice(['=', '<', '<=>'])} tb.a"
        wh = rng.choice(["", f" WHERE s.c > {rng.randint(-2, 2)}", " WHERE tb.b IS NOT NULL"])
        return f"SELECT s.a, tb.b FROM (SELECT * FROM ta WHERE {inner}) s JOIN tb ON {outer}{wh}"
    if form == "three":
        wh = where(rng, w2, 2)
        return (f"SELECT ta.a, tc.v FROM ta JOIN tb ON {on_condition(rng)} "
                f"JOIN tc ON tb.b {rng.choice(['=', '<'])} tc.k" + (f" WHERE {wh}" if wh else ""))
    wh = where(rng, ["ta"], 3) or "ta.a > 0"
    return f"SELECT a, b FROM ta WHERE {wh.replace('ta.', '')}"


def main(out):
    rng = random.Random(SEED)
    tables = {name: table(rng, cols) for name, cols in COLS.items()}
    cases = [{"id": f"H{i:03d}", "sql": query(rng), "tables": tables}
             for i in range(1, N_QUERIES + 1)]
    doc = {"schema_version": 1, "benchmark_id": "queryproof-v17-heldout-20261021",
           "nature": "HELD-OUT cohort, generated and frozen before any execution",
           "seed": SEED, "cases": cases}
    Path(out).write_bytes(json.dumps(doc, indent=1).encode("utf-8"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "COHORT.json")
