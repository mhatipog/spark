"""QueryProof rich hunt: composed queries over INT, STRING, DECIMAL(5,2) and DATE columns.

Usage: python gen_rich.py SEED N OUT.json [DATASETS] [MAX_DEPTH] [CASES_PER_DATASET]
Extends gen_deep (whose output stays reproducible) with a table `te(k, d DATE,
m DECIMAL(5,2), s)`, typed literals, and typed aggregates. Every case also carries a
ternary-logic-partitioning predicate `tlp` over the query's output columns: the bag of
Q must equal Q WHERE p, plus Q WHERE NOT p, plus Q WHERE p IS NULL. Several
consecutive cases share their databases so the runner writes each one once.
Decimal values only appear in comparisons, COALESCE, CASE and SUM/MIN/MAX, so DuckDB
computes them exactly; cases with DECIMAL arithmetic are marked spark-only.
"""
import json
import random
import re
import sys

import gen_deep
from gen_deep import STRS, Gen

TABLES = dict(gen_deep.TABLES, te=[("k", "int"), ("d", "date"), ("m", "dec"), ("s", "str")])
DECS = ["-1.50", "0.00", "0.25", "1.25", "2.00"]
DATES = [f"2024-01-0{i}" for i in range(1, 6)]
EXTRA = ("str", "dec", "date")
TYPE_NAMES = {"int": "integer", "str": "string", "dec": "decimal(5,2)", "date": "date"}


def table(rng, cols):
    def val(ty):
        if rng.random() < 0.25:
            return None
        return {"int": lambda: rng.randint(-3, 3), "str": lambda: rng.choice(STRS),
                "dec": lambda: rng.choice(DECS), "date": lambda: rng.choice(DATES)}[ty]()
    rows = [[val(ty) for _, ty in cols] for _ in range(rng.randint(0, 24))]
    if rows:
        rows += [list(rng.choice(rows)) for _ in range(rng.randint(0, 4))]
    rng.shuffle(rows)
    return {"columns": [{"name": c, "type": TYPE_NAMES[ty]} for c, ty in cols], "rows": rows}


class RichGen(Gen):
    def base(self):
        t = self.rng.choice(list(TABLES) + ["te"])          # te twice as likely
        return {"sql": t, "cols": TABLES[t]}

    def lit(self, ty):
        if ty == "dec":
            return f"CAST('{self.rng.choice(DECS)}' AS DECIMAL(5, 2))"
        if ty == "date":
            return f"DATE '{self.rng.choice(DATES)}'"
        return super().lit(ty)

    def types_in(self, refs):
        return [t for t in EXTRA if self.pick(refs, t)]

    def expr(self, refs, ty, depth=0):
        if ty not in ("dec", "date"):
            return super().expr(refs, ty, depth)
        ref = self.pick(refs, ty)
        r = self.rng.random()
        if ref is None:
            return self.lit(ty)
        if r < 0.55 or depth > 1:
            return ref
        if r < 0.8:
            return f"COALESCE({ref}, {self.lit(ty)})"
        return (f"CASE WHEN {self.pred(refs, 0, False)} THEN {self.expr(refs, ty, depth + 1)} "
                f"ELSE {self.expr(refs, ty, depth + 1)} END")

    def pred(self, refs, depth, allow_sub=True):
        extra = self.types_in(refs)
        ty = self.rng.choice(extra) if extra and self.rng.random() < 0.45 else "int"
        ref = self.pick(refs, ty)
        r = self.rng.random()
        if allow_sub and depth < 2 and r < 0.3:
            return self.sub_pred(refs, depth)
        if r < 0.45:
            return f"{ref} {self.cmp()} {self.lit(ty)}"
        if r < 0.55:
            return f"{ref} IS {'NOT ' if self.rng.random() < 0.5 else ''}NULL"
        if r < 0.63:
            return f"{ref} {'NOT ' if self.rng.random() < 0.3 else ''}IN ({self.lit(ty)}, {self.lit(ty)})"
        if r < 0.73:
            return f"{ref} {self.cmp()} {self.pick(refs, ty)}"
        if r < 0.80:
            return f"COALESCE({ref}, {self.lit(ty)}) {self.cmp()} {self.lit(ty)}"
        if r < 0.92:
            op = self.rng.choice(["AND", "OR"])
            return f"({self.pred(refs, depth + 1, allow_sub)} {op} {self.pred(refs, depth + 1, allow_sub)})"
        return f"NOT ({self.pred(refs, depth + 1, allow_sub)})"

    def project(self, refs, width=None):
        width = width or self.rng.randint(1, 4)
        extra = self.types_in(refs)
        types = ["int"] + [(self.rng.choice(extra) if extra and self.rng.random() < 0.45 else "int")
                           for _ in range(width - 1)]
        self.rng.shuffle(types)
        items = [(self.expr(refs, ty), ty) for ty in types]
        sel = ", ".join(f"{e} AS c{i}" for i, (e, _) in enumerate(items))
        return sel, [(f"c{i}", ty) for i, (_, ty) in enumerate(items)]

    def q_agg(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        extra = self.types_in(refs)
        kty = self.rng.choice(extra) if extra and self.rng.random() < 0.4 else "int"
        key = self.pick(refs, kty)
        v = self.pick(refs, "int")
        aggs = [("COUNT(*)", "int"), (f"COUNT({v})", "int"), (f"SUM({v})", "int"), (f"MIN({v})", "int"),
                (f"MAX({v})", "int"), (f"COUNT(DISTINCT {v})", "int")]
        for t in extra:
            x = self.pick(refs, t)
            aggs += [(f"MAX({x})", t), (f"MIN({x})", t), (f"COUNT(DISTINCT {x})", "int")]
            if t == "dec":
                aggs.append((f"SUM({x})", "dec"))
        chosen = self.rng.sample(aggs, self.rng.randint(1, 3))
        if kty != "int" and all(ty != "int" for _, ty in chosen):
            chosen.append(("COUNT(*)", "int"))  # every relation keeps an integer column
        sel = ", ".join([f"{key} AS c0"] + [f"{e} AS c{i + 1}" for i, (e, _) in enumerate(chosen)])
        where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.4 else ""
        having = ""
        if self.rng.random() < 0.35:
            having = f" HAVING {self.rng.choice(['COUNT(*)', f'SUM({v})', f'MIN({v})'])} {self.cmp()} {self.rng.randint(0, 2)}"
        return (f"SELECT {sel} FROM {r['sql']} {a}{where} GROUP BY {key}{having}",
                [("c0", kty)] + [(f"c{i + 1}", ty) for i, (_, ty) in enumerate(chosen)])

    def q_dpp(self, depth):
        """UNION ALL of partitioned and other scans joined to a filtered relation (DPP shapes)."""
        parts = {"ta": "a", "tb": "a", "te": "k"}
        sides = []
        for _ in range(self.rng.randint(2, 3)):
            t = self.rng.choice(["ta", "tb", "te", "tc", "td", "rel"])
            if t == "rel":
                r = self.rel(self.rng.randint(0, max(0, depth - 2)))
            else:
                r = {"sql": t, "cols": TABLES[t]}
            a = self.alias()
            refs = self.refs(r, a)
            key = f"{a}.{parts[t]}" if t in parts else self.pick(refs, "int")
            if self.rng.random() < 0.25:
                key = f"{key} {self.rng.choice(['+', '-'])} 1"
            val = self.expr(refs, "int")
            where = f" WHERE {self.pred(refs, 1, False)}" if self.rng.random() < 0.3 else ""
            sides.append(f"SELECT {key} AS c0, {val} AS c1 FROM {r['sql']} {a}{where}")
        u = self.alias()
        dim = self.rel(self.rng.randint(0, max(0, depth - 1)))
        b = self.alias()
        brefs = self.refs(dim, b)
        dkey = self.pick(brefs, "int")
        jt = self.rng.choice(["JOIN", "JOIN", "JOIN", "LEFT SEMI JOIN", "RIGHT JOIN"])
        sel = f"{u}.c0 AS c0, {u}.c1 AS c1" if jt == "LEFT SEMI JOIN" else \
            f"{u}.c0 AS c0, {u}.c1 AS c1, {self.expr(brefs, 'int')} AS c2"
        cols = [("c0", "int"), ("c1", "int")] + ([] if jt == "LEFT SEMI JOIN" else [("c2", "int")])
        where = f" WHERE {self.pred(brefs, 1, False)}"
        return (f"SELECT {sel} FROM ({' UNION ALL '.join(sides)}) {u} {jt} {dim['sql']} {b} "
                f"ON {u}.c0 = {dkey}{where}", cols)

    def rel(self, depth):
        if depth > 0 and self.rng.random() < 0.08:
            sql, cols = self.q_dpp(depth)
            return {"sql": f"({sql})", "cols": cols}
        return super().rel(depth)

    def query_cols(self):
        depth = self.rng.randint(2, self.max_depth)
        op = self.rng.choice([self.q_select, self.q_join, self.q_join, self.q_agg, self.q_distinct,
                              self.q_setop, self.q_window, self.q_limit, self.q_dpp, self.q_dpp])
        return op(depth)


def main(seed, n, out, datasets=2, max_depth=4, per_dataset=8):
    rng = random.Random(int(seed))
    cases, shared = [], None
    for i in range(int(n)):
        if i % int(per_dataset) == 0:
            shared = [{t: table(rng, c) for t, c in TABLES.items()} for _ in range(int(datasets))]
        spark_only = rng.random() < 0.1
        g = RichGen(rng, int(max_depth), spark_only)
        sql, cols = g.query_cols()
        tlp = g.pred([(f"t.{c}", ty) for c, ty in cols], 1, False)
        uses_rich = bool(re.search(r"\bte\b", sql))
        cases.append({"id": f"R{int(seed) % 1000:03d}_{i:05d}",
                      "family": "rich" + ("_dec" if spark_only else "") + ("_te" if uses_rich else ""),
                      "sql": sql, "tlp": tlp, "group": i // int(per_dataset),
                      "sqlite": not spark_only and not uses_rich, "duckdb": not spark_only,
                      "datasets": shared})
    with open(out, "w", newline="\n") as f:
        json.dump({"seed": int(seed), "cases": cases}, f)
    print("QUERYPROOF_RICH_GENERATED", len(cases))


if __name__ == "__main__":
    main(*sys.argv[1:7])
