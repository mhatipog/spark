"""QueryProof deep hunt: random composed queries (nested derived tables).

Usage: python gen_deep.py SEED N OUT.json [DATASETS] [MAX_DEPTH]
A query is a tree of relational operators; every inner node is a derived table
`(SELECT ... AS c0, ...) dN`. Operators: filter/project with expressions and
subquery predicates (EXISTS, IN, scalar aggregates, correlated or not), joins
(inner, left, right, full; equality or null-safe keys), aggregation with HAVING,
DISTINCT, set operations, windows (rank filter, partition count, running sum
with the default RANGE frame), and ORDER BY over every output column with
LIMIT/OFFSET. Columns are integers or strings.
Results must be deterministic bags that SQLite computes identically, except for
cases marked "sqlite": false (DECIMAL arithmetic), which compare Spark
configurations only.
"""
import json
import random
import sys

TABLES = {
    "ta": [("a", "int"), ("b", "int"), ("c", "int")],
    "tb": [("a", "int"), ("b", "int"), ("c", "int")],
    "tc": [("k", "int"), ("v", "int")],
    "td": [("k", "int"), ("s", "str")],
}
STRS = ["a", "b", "ab", "B", ""]


def table(rng, cols):
    def val(ty):
        if rng.random() < 0.25:
            return None
        return rng.randint(-3, 3) if ty == "int" else rng.choice(STRS)
    rows = [[val(ty) for _, ty in cols] for _ in range(rng.randint(0, 24))]
    if rows:
        rows += [list(rng.choice(rows)) for _ in range(rng.randint(0, 4))]
    rng.shuffle(rows)
    return {"columns": [{"name": c, "type": "integer" if ty == "int" else "string"} for c, ty in cols],
            "rows": rows}


class Gen:
    def __init__(self, rng, max_depth, spark_only):
        self.rng, self.max_depth, self.spark_only, self.n = rng, max_depth, spark_only, 0

    def alias(self):
        self.n += 1
        return f"d{self.n}"

    # ---- leaves and relations -------------------------------------------------
    def base(self):
        t = self.rng.choice(list(TABLES))
        return {"sql": t, "cols": TABLES[t]}

    def rel(self, depth):
        if depth <= 0 or self.rng.random() < 0.15:
            return self.base()
        op = self.rng.choice([self.q_select, self.q_select, self.q_join, self.q_join, self.q_agg,
                              self.q_distinct, self.q_setop, self.q_window, self.q_limit])
        sql, cols = op(depth)
        return {"sql": f"({sql})", "cols": cols}

    def refs(self, rel, a):
        return [(f"{a}.{c}", ty) for c, ty in rel["cols"]]

    # ---- scalar expressions and predicates ----------------------------------
    def lit(self, ty):
        return str(self.rng.randint(-2, 2)) if ty == "int" else "'" + self.rng.choice(STRS) + "'"

    def pick(self, refs, ty):
        same = [r for r, t in refs if t == ty]
        return self.rng.choice(same) if same else None

    def expr(self, refs, ty, depth=0):
        r = self.rng.random()
        ref = self.pick(refs, ty)
        if ref is None or (r < 0.08 and ty == "int"):
            s = self.pick(refs, "str")
            if ty == "int" and s is not None:
                return f"LENGTH({s})"
            return self.lit(ty)
        if r < 0.45 or depth > 1:
            return ref
        if ty == "int":
            if r < 0.58:
                return f"{ref} {self.rng.choice(['+', '-', '*'])} {self.rng.randint(1, 2)}"
            if r < 0.68:
                return f"COALESCE({ref}, {self.lit('int')})"
            if r < 0.74:
                return f"NULLIF({ref}, {self.lit('int')})"
            if r < 0.80 and self.spark_only:
                return f"CAST(CAST({ref} AS DECIMAL(8, 2)) * 1.25 AS DECIMAL(10, 2))"
        else:
            if r < 0.6:
                return f"COALESCE({ref}, {self.lit('str')})"
            if r < 0.7:
                return f"UPPER({ref})"
            if r < 0.8:
                return f"{ref} || {self.lit('str')}"
        return (f"CASE WHEN {self.pred(refs, 0, False)} THEN {self.expr(refs, ty, depth + 1)} "
                f"ELSE {self.expr(refs, ty, depth + 1)} END")

    def cmp(self):
        return self.rng.choice(["=", "<", "<=", ">", ">=", "<>"])

    def pred(self, refs, depth, allow_sub=True):
        r = self.rng.random()
        ty = "int" if self.rng.random() < 0.75 or not self.pick(refs, "str") else "str"
        ref = self.pick(refs, ty) or self.pick(refs, "int")
        ty = "int" if self.pick(refs, ty) is None else ty
        if allow_sub and depth < 2 and r < 0.3:
            return self.sub_pred(refs, depth)
        if r < 0.45:
            return f"{ref} {self.cmp()} {self.lit(ty)}"
        if r < 0.55:
            return f"{ref} IS {'NOT ' if self.rng.random() < 0.5 else ''}NULL"
        if r < 0.63:
            return f"{ref} {'NOT ' if self.rng.random() < 0.3 else ''}IN ({self.lit(ty)}, {self.lit(ty)})"
        if r < 0.73:
            other = self.pick(refs, ty)
            return f"{ref} {self.cmp()} {other}"
        if r < 0.80:
            return f"COALESCE({ref}, {self.lit(ty)}) {self.cmp()} {self.lit(ty)}"
        if r < 0.92:
            op = self.rng.choice(["AND", "OR"])
            return f"({self.pred(refs, depth + 1, allow_sub)} {op} {self.pred(refs, depth + 1, allow_sub)})"
        return f"NOT ({self.pred(refs, depth + 1, allow_sub)})"

    def sub_pred(self, refs, depth):
        inner = self.rel(self.rng.randint(0, 1))
        a = self.alias()
        irefs = self.refs(inner, a)
        outer = self.pick(refs, "int")
        key = self.pick(irefs, "int")
        corr = self.rng.random() < 0.6
        where = []
        if corr:
            where.append(f"{key} {self.rng.choice(['=', '=', '<', '<>'])} {outer}")
        if self.rng.random() < 0.5:
            where.append(self.pred(irefs, 2, False))
        w = (" WHERE " + " AND ".join(where)) if where else ""
        k = self.rng.random()
        neg = "NOT " if self.rng.random() < 0.4 else ""
        if k < 0.4:
            return f"{neg}EXISTS (SELECT 1 FROM {inner['sql']} {a}{w})"
        if k < 0.75:
            col = self.pick(irefs, "int")
            return f"{outer} {neg}IN (SELECT {col} FROM {inner['sql']} {a}{w})"
        agg = self.rng.choice(["COUNT(*)", f"COUNT({key})", f"SUM({key})", f"MAX({key})", f"MIN({key})"])
        return f"(SELECT {agg} FROM {inner['sql']} {a}{w}) {self.cmp()} {self.rng.randint(-1, 2)}"

    # ---- projections ---------------------------------------------------------
    def project(self, refs, width=None):
        width = width or self.rng.randint(1, 4)
        types = ["int"] + [("str" if self.pick(refs, "str") and self.rng.random() < 0.3 else "int")
                           for _ in range(width - 1)]
        self.rng.shuffle(types)
        items = [(self.expr(refs, ty), ty) for ty in types]
        sel = ", ".join(f"{e} AS c{i}" for i, (e, _) in enumerate(items))
        return sel, [(f"c{i}", ty) for i, (_, ty) in enumerate(items)]

    # ---- operators: each returns (SELECT sql, output columns) -----------------
    def q_select(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        sel, cols = self.project(refs)
        where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.8 else ""
        return f"SELECT {sel} FROM {r['sql']} {a}{where}", cols

    def q_join(self, depth):
        l, r = self.rel(depth - 1), self.rel(self.rng.randint(0, depth - 1))
        a, b = self.alias(), self.alias()
        lr, rr = self.refs(l, a), self.refs(r, b)
        jt = self.rng.choice(["JOIN", "JOIN", "LEFT JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"])
        on = f"{self.pick(lr, 'int')} {self.rng.choice(['=', '=', '=', '<=>'])} {self.pick(rr, 'int')}"
        if self.rng.random() < 0.35:
            on += f" AND {self.pred(self.rng.choice([lr, rr, lr + rr]), 1, False)}"
        sel, cols = self.project(lr + rr)
        where = f" WHERE {self.pred(lr + rr, 0)}" if self.rng.random() < 0.5 else ""
        return f"SELECT {sel} FROM {l['sql']} {a} {jt} {r['sql']} {b} ON {on}{where}", cols

    def q_agg(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        kty = "str" if self.pick(refs, "str") and self.rng.random() < 0.3 else "int"
        key = self.pick(refs, kty)
        v = self.pick(refs, "int")
        s = self.pick(refs, "str")
        aggs = [("COUNT(*)", "int"), (f"COUNT({v})", "int"), (f"SUM({v})", "int"), (f"MIN({v})", "int"),
                (f"MAX({v})", "int"), (f"COUNT(DISTINCT {v})", "int")]
        if s:
            aggs += [(f"MAX({s})", "str"), (f"COUNT(DISTINCT {s})", "int")]
        chosen = self.rng.sample(aggs, self.rng.randint(1, 3))
        if kty == "str" and all(ty == "str" for _, ty in chosen):
            chosen.append(("COUNT(*)", "int"))  # every relation keeps an integer column
        sel = ", ".join([f"{key} AS c0"] + [f"{e} AS c{i + 1}" for i, (e, _) in enumerate(chosen)])
        where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.4 else ""
        having = ""
        if self.rng.random() < 0.35:
            having = f" HAVING {self.rng.choice(['COUNT(*)', f'SUM({v})', f'MIN({v})'])} {self.cmp()} {self.rng.randint(0, 2)}"
        return (f"SELECT {sel} FROM {r['sql']} {a}{where} GROUP BY {key}{having}",
                [("c0", kty)] + [(f"c{i + 1}", ty) for i, (_, ty) in enumerate(chosen)])

    def q_distinct(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        sel, cols = self.project(refs, self.rng.randint(1, 2))
        where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.5 else ""
        return f"SELECT DISTINCT {sel} FROM {r['sql']} {a}{where}", cols

    def q_setop(self, depth):
        sides = []
        for _ in range(2):
            r = self.rel(self.rng.randint(0, depth - 1))
            a = self.alias()
            refs = self.refs(r, a)
            exprs = [self.expr(refs, "int") for _ in range(2)]
            where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.5 else ""
            sides.append(f"SELECT {exprs[0]} AS c0, {exprs[1]} AS c1 FROM {r['sql']} {a}{where}")
        op = self.rng.choice(["UNION", "UNION ALL", "INTERSECT", "EXCEPT"])
        # wrap so the set operation is a single derived relation
        b = self.alias()
        return (f"SELECT {b}.c0 AS c0, {b}.c1 AS c1 FROM ({sides[0]} {op} {sides[1]}) {b}",
                [("c0", "int"), ("c1", "int")])

    def q_window(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        cols = [c for c in refs]
        p = self.pick(refs, self.rng.choice(["int", "str"])) or self.pick(refs, "int")
        o = self.pick(refs, "int")
        kind = self.rng.random()
        if kind < 0.45:
            fn = self.rng.choice(["RANK()", "DENSE_RANK()"])
            w = f"{fn} OVER (PARTITION BY {p} ORDER BY {o} NULLS FIRST)"
        elif kind < 0.7:
            w = f"COUNT(*) OVER (PARTITION BY {p})"
        else:
            w = f"SUM({self.pick(refs, 'int')}) OVER (PARTITION BY {p} ORDER BY {o} NULLS FIRST)"
        sel = ", ".join(f"{e} AS c{i}" for i, (e, _) in enumerate(cols))
        n = len(cols)
        inner = f"SELECT {sel}, {w} AS c{n} FROM {r['sql']} {a}"
        b = self.alias()
        out = ", ".join(f"{b}.c{i} AS c{i}" for i in range(n + 1))
        filt = f" WHERE {b}.c{n} {self.rng.choice(['<=', '<', '=', '>='])} {self.rng.randint(1, 2)}" \
            if self.rng.random() < 0.7 else ""
        return f"SELECT {out} FROM ({inner}) {b}{filt}", [(f"c{i}", ty) for i, (_, ty) in enumerate(cols)] + [
            (f"c{n}", "int")]

    def q_limit(self, depth):
        r = self.rel(depth - 1)
        a = self.alias()
        refs = self.refs(r, a)
        sel = ", ".join(f"{e} AS c{i}" for i, (e, _) in enumerate(refs))
        order = ", ".join(f"{e} NULLS FIRST" for e, _ in refs)
        where = f" WHERE {self.pred(refs, 0)}" if self.rng.random() < 0.4 else ""
        # OFFSET 0 is left out: under MINIMAL it only rediscovers SPARK-59044.
        off = f" OFFSET {self.rng.randint(1, 3)}" if self.rng.random() < 0.4 else ""
        return (f"SELECT {sel} FROM {r['sql']} {a}{where} ORDER BY {order} LIMIT {self.rng.randint(0, 6)}{off}",
                [(f"c{i}", ty) for i, (_, ty) in enumerate(refs)])

    def query(self):
        depth = self.rng.randint(2, self.max_depth)
        op = self.rng.choice([self.q_select, self.q_join, self.q_join, self.q_agg, self.q_distinct,
                              self.q_setop, self.q_window, self.q_limit])
        return op(depth)[0]


def main(seed, n, out, datasets=2, max_depth=4):
    rng = random.Random(int(seed))
    cases = []
    for i in range(int(n)):
        spark_only = rng.random() < 0.15
        sql = Gen(rng, int(max_depth), spark_only).query()
        case = {"id": f"D{int(seed) % 1000:03d}_{i:05d}", "family": "deep" + ("_dec" if spark_only else ""),
                "sql": sql, "datasets": [{t: table(rng, c) for t, c in TABLES.items()}
                                         for _ in range(int(datasets))]}
        if spark_only:
            case["sqlite"] = False
        cases.append(case)
    with open(out, "w", newline="\n") as f:
        json.dump({"seed": int(seed), "cases": cases}, f)
    print("QUERYPROOF_DEEP_GENERATED", len(cases))


if __name__ == "__main__":
    main(*sys.argv[1:6])
