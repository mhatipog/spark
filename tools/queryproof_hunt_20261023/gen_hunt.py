"""QueryProof bug hunt: random queries from historically bug-prone SQL families.

Usage: python gen_hunt.py SEED N OUT.json [DATASETS]
Every query is meant to have the same result bag in Spark (ANSI) and SQLite:
integer values only, no division, scalar subqueries return at most one row,
LIMIT/OFFSET only under an ORDER BY over all selected columns. Each query is
paired with DATASETS fresh NULL- and duplicate-rich databases.
"""
import json
import random
import sys

COLS = {"ta": ["a", "b", "c"], "tb": ["a", "b", "c"], "tc": ["k", "v"]}


def table(rng, cols):
    rows = [[None if rng.random() < 0.3 else rng.randint(-3, 3) for _ in cols]
            for _ in range(rng.randint(0, 14))]
    if rows:
        rows += [list(rng.choice(rows)) for _ in range(rng.randint(0, 3))]
    rng.shuffle(rows)
    return {"columns": [{"name": c, "type": "integer"} for c in cols], "rows": rows}


def lit(rng):
    return str(rng.randint(-2, 2))


def cmp(rng):
    return rng.choice(["=", "<", "<=", ">", ">=", "<>"])


def pred(rng, t, depth=0):
    c = rng.choice(COLS[t])
    r = rng.random()
    if r < 0.45:
        return f"{t}.{c} {cmp(rng)} {lit(rng)}"
    if r < 0.6:
        return f"{t}.{c} IS {'NOT ' if rng.random() < 0.5 else ''}NULL"
    if r < 0.7:
        return f"{t}.{c} IN ({lit(rng)}, {lit(rng)})"
    if r < 0.8:
        return f"COALESCE({t}.{c}, {lit(rng)}) {cmp(rng)} {lit(rng)}"
    if r < 0.9 and depth < 1:
        return f"({pred(rng, t, depth + 1)} OR {pred(rng, t, depth + 1)})"
    return f"NOT ({t}.{c} {cmp(rng)} {lit(rng)})"


def maybe(rng, text, p=0.5):
    return text if rng.random() < p else ""


def q_exists(rng):
    neg = "NOT " if rng.random() < 0.5 else ""
    corr = f"tb.{rng.choice(['a', 'b'])} {rng.choice(['=', '<', '>'])} ta.{rng.choice(['a', 'b'])}"
    inner = corr + maybe(rng, f" AND {pred(rng, 'tb')}")
    outer = maybe(rng, f" {rng.choice(['AND', 'OR'])} {pred(rng, 'ta')}", 0.5)
    return f"SELECT ta.a, ta.b FROM ta WHERE {neg}EXISTS (SELECT 1 FROM tb WHERE {inner}){outer}"


def q_in(rng):
    neg = "NOT " if rng.random() < 0.5 else ""
    col = rng.choice(["a", "b", "c"])
    sub = f"SELECT tb.{rng.choice(['a', 'b', 'c'])} FROM tb" + maybe(rng, f" WHERE {pred(rng, 'tb')}")
    if rng.random() < 0.4:          # correlated
        sub = (f"SELECT tb.{rng.choice(['a', 'b'])} FROM tb WHERE tb.c = ta.c"
               + maybe(rng, f" AND {pred(rng, 'tb')}"))
    return (f"SELECT ta.a, ta.c FROM ta WHERE ta.{col} {neg}IN ({sub})"
            + maybe(rng, f" AND {pred(rng, 'ta')}", 0.4))


def q_scalar(rng):
    agg = rng.choice(["COUNT(*)", "COUNT(tb.b)", "SUM(tb.b)", "MAX(tb.b)", "MIN(tb.c)"])
    corr = f"tb.a = ta.a" + maybe(rng, f" AND {pred(rng, 'tb')}")
    sub = f"(SELECT {agg} FROM tb WHERE {corr})"
    if rng.random() < 0.5:
        return f"SELECT ta.a, {sub} AS s FROM ta" + maybe(rng, f" WHERE {pred(rng, 'ta')}")
    return f"SELECT ta.a, ta.b FROM ta WHERE {sub} {cmp(rng)} {rng.randint(0, 2)}"


def q_outer(rng):
    j1 = rng.choice(["LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "JOIN"])
    on1 = f"ta.{rng.choice(['a', 'b'])} = tb.{rng.choice(['a', 'b'])}" + maybe(rng, f" AND {pred(rng, rng.choice(['ta', 'tb']))}", 0.4)
    q = f"SELECT ta.a, tb.b, tc.v FROM ta {j1} tb ON {on1}"
    j2 = rng.choice(["LEFT JOIN", "JOIN", "FULL JOIN"])
    q += f" {j2} tc ON tc.k = {rng.choice(['ta.c', 'tb.c'])}" + maybe(rng, f" AND {pred(rng, 'tc')}", 0.3)
    w = [pred(rng, rng.choice(["ta", "tb", "tc"])) for _ in range(rng.randint(0, 2))]
    if rng.random() < 0.3:
        w.append(f"{rng.choice(['tb.a', 'tc.k'])} IS NULL")
    return q + (" WHERE " + " AND ".join(w) if w else "")


def q_agg(rng):
    join = rng.choice(["LEFT JOIN", "JOIN"])
    aggs = rng.sample(["COUNT(*)", "COUNT(tb.b)", "SUM(tb.c)", "MIN(tb.b)", "MAX(ta.b)",
                       "COUNT(DISTINCT tb.c)"], 2)
    q = (f"SELECT ta.a, {aggs[0]} AS x, {aggs[1]} AS y FROM ta {join} tb ON ta.b = tb.b"
         + maybe(rng, f" WHERE {pred(rng, rng.choice(['ta', 'tb']))}") + " GROUP BY ta.a")
    return q + maybe(rng, f" HAVING {aggs[0]} {cmp(rng)} {rng.randint(0, 2)}")


def q_setop(rng):
    op = rng.choice(["UNION", "UNION ALL", "INTERSECT", "EXCEPT"])
    return (f"SELECT ta.a, ta.b FROM ta WHERE {pred(rng, 'ta')} {op} "
            f"SELECT tb.a, tb.b FROM tb WHERE {pred(rng, 'tb')}")


def q_limit(rng):
    k, off = rng.randint(0, 4), rng.randint(0, 2)
    inner = (f"SELECT ta.a, ta.b FROM ta" + maybe(rng, f" WHERE {pred(rng, 'ta')}")
             + f" ORDER BY ta.a NULLS FIRST, ta.b NULLS FIRST LIMIT {k}"
             + (f" OFFSET {off}" if rng.random() < 0.5 else ""))
    if rng.random() < 0.5:
        return f"SELECT s.a, tb.c FROM ({inner}) s JOIN tb ON s.a = tb.a"
    return f"SELECT s.a, COUNT(*) AS n FROM ({inner}) s GROUP BY s.a"


def q_case(rng):
    k = lit(rng)
    return (f"SELECT CASE WHEN ta.a {cmp(rng)} {k} THEN ta.b ELSE ta.c END AS x, "
            f"COALESCE(ta.b, {lit(rng)}) AS y FROM ta WHERE "
            f"NULLIF(ta.a, {lit(rng)}) IS {'NOT ' if rng.random() < 0.5 else ''}NULL"
            + maybe(rng, f" OR {pred(rng, 'ta')}"))


def q_window(rng):
    w = rng.choice(["COUNT(*)", "SUM(ta.b)", "MAX(ta.c)", "COUNT(ta.c)"])
    inner = (f"SELECT ta.a, ta.b, {w} OVER (PARTITION BY ta.a) AS w FROM ta"
             + maybe(rng, f" WHERE {pred(rng, 'ta')}"))
    return f"SELECT x.a, x.b, x.w FROM ({inner}) x" + maybe(rng, f" WHERE x.w {cmp(rng)} {rng.randint(0, 2)}")


def q_derived(rng):
    j = rng.choice(["LEFT JOIN", "JOIN"])
    return (f"SELECT DISTINCT d.x FROM (SELECT ta.a AS x, tb.c AS y FROM ta {j} tb "
            f"ON ta.b = tb.b) d WHERE {rng.choice(['d.y IS NULL', 'd.y IS NOT NULL', 'd.x > 0', 'd.y < d.x'])}")


def q_count_bug(rng):
    agg = rng.choice(["COUNT(*)", "COUNT(tb.c)", "COALESCE(SUM(tb.c), 0)", "COUNT(*) + 1"])
    sub = f"(SELECT {agg} FROM tb WHERE tb.a = ta.a" + maybe(rng, f" AND {pred(rng, 'tb')}") + ")"
    return f"SELECT ta.a, ta.b FROM ta WHERE {sub} {rng.choice(['=', '<', '>='])} {rng.randint(0, 1)}"


def q_nested(rng):
    neg1 = "NOT " if rng.random() < 0.5 else ""
    neg2 = "NOT " if rng.random() < 0.5 else ""
    return (f"SELECT ta.a FROM ta WHERE {neg1}EXISTS (SELECT 1 FROM tb WHERE tb.a = ta.a AND "
            f"{neg2}EXISTS (SELECT 1 FROM tc WHERE tc.k = tb.c" + maybe(rng, " AND tc.v = ta.b") + "))")


def q_semi_or(rng):
    a = f"EXISTS (SELECT 1 FROM tb WHERE tb.a = ta.a AND {pred(rng, 'tb')})"
    b = rng.choice([f"ta.b IN (SELECT tc.v FROM tc)", f"ta.c NOT IN (SELECT tc.k FROM tc WHERE tc.v IS NOT NULL)",
                    pred(rng, "ta")])
    return f"SELECT ta.a, ta.b FROM ta WHERE {a} {rng.choice(['OR', 'AND'])} {b}"


def q_rank(rng):
    fn = rng.choice(["RANK()", "DENSE_RANK()"])
    order = rng.choice(["ta.b NULLS FIRST", "ta.b DESC NULLS LAST", "ta.b NULLS FIRST, ta.c NULLS FIRST"])
    part = maybe(rng, "PARTITION BY ta.a ", 0.7)
    inner = (f"SELECT ta.a, ta.b, ta.c, {fn} OVER ({part}ORDER BY {order}) AS r FROM ta"
             + maybe(rng, f" WHERE {pred(rng, 'ta')}", 0.4))
    return f"SELECT x.a, x.b, x.r FROM ({inner}) x WHERE x.r {rng.choice(['<=', '<', '='])} {rng.randint(1, 2)}"


def q_filter_agg(rng):
    return (f"SELECT ta.a, COUNT(*) FILTER (WHERE {pred(rng, 'ta')}) AS f, "
            f"SUM(ta.c) FILTER (WHERE {pred(rng, 'ta')}) AS g FROM ta GROUP BY ta.a"
            + maybe(rng, " HAVING COUNT(*) > 1", 0.3))


def q_distinct_agg(rng):
    return (f"SELECT ta.a, COUNT(DISTINCT ta.b) AS x, COUNT(DISTINCT ta.c) AS y, SUM(ta.b) AS z FROM ta"
            + maybe(rng, f" WHERE {pred(rng, 'ta')}") + " GROUP BY ta.a")


def q_spark_only(rng):
    # Spark-specific constructs: compared FULL vs MINIMAL only (SQLite errors are recorded).
    t = rng.randint(0, 4)
    if t == 0:
        return f"SELECT ta.a, ta.b, SUM(ta.c) AS s FROM ta GROUP BY ROLLUP(ta.a, ta.b)"
    if t == 1:
        return (f"SELECT ta.a, ta.b FROM ta WHERE {pred(rng, 'ta')} "
                f"{rng.choice(['INTERSECT ALL', 'EXCEPT ALL'])} SELECT tb.a, tb.b FROM tb")
    if t == 2:
        return (f"SELECT ta.a, l.m FROM ta, LATERAL (SELECT MAX(tb.c) AS m FROM tb WHERE tb.a = ta.a"
                + maybe(rng, f" AND {pred(rng, 'tb')}") + ") l")
    if t == 3:
        return f"SELECT ta.a, ta.b FROM ta LEFT ANTI JOIN tb ON ta.a = tb.a" + maybe(rng, f" AND {pred(rng, 'tb')}")
    return (f"SELECT ta.a, COUNT(*) AS n FROM ta WHERE (ta.a, ta.b) "
            f"{'NOT ' if rng.random() < 0.5 else ''}IN (SELECT tb.a, tb.b FROM tb) GROUP BY ta.a")


FAMILIES = [q_exists, q_in, q_scalar, q_outer, q_agg, q_setop, q_limit, q_case, q_window, q_derived,
            q_count_bug, q_nested, q_semi_or, q_rank, q_filter_agg, q_distinct_agg, q_spark_only]


def main(seed, n, out, datasets=3):
    rng = random.Random(int(seed))
    cases = []
    for i in range(int(n)):
        fam = FAMILIES[i % len(FAMILIES)]
        cases.append({"id": f"H{int(seed) % 1000:03d}_{i:05d}", "family": fam.__name__,
                      "sql": fam(rng),
                      "datasets": [{t: table(rng, c) for t, c in COLS.items()}
                                   for _ in range(int(datasets))]})
    with open(out, "w", newline="\n") as f:
        json.dump({"seed": int(seed), "cases": cases}, f)
    print("QUERYPROOF_HUNT_GENERATED", len(cases))


if __name__ == "__main__":
    main(*sys.argv[1:5])
