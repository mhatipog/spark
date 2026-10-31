"""QueryProof feature hunt: rich queries plus nested types, VARIANT and collations.

Usage: python gen_feat.py SEED N OUT.json [DATASETS] [MAX_DEPTH] [CASES_PER_DATASET]
Extends gen_rich with
  tf(k INT, st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>, js STRING), partitioned by k
  tv(k INT, v VARIANT), written by the runner as parse_json(tf.js)
Queries reach them through flattening derived tables that read nested fields
(st.x, st.y), array functions, LATERAL VIEW [OUTER] explode, variant_get /
try_variant_get, and a UTF8_LCASE-collated string column (type "strl"). These cases
compare Spark configurations, TLP and the materialization oracles only (no DuckDB or
SQLite).
"""
import json
import random
import sys

import gen_rich
from gen_rich import RichGen, table

FEAT_TABLES = dict(gen_rich.TABLES, tf=[("k", "int"), ("st", "struct"), ("arr", "array"), ("js", "json")])
TYPE_NAMES = dict(gen_rich.TYPE_NAMES, struct="struct<x:int,y:string>", array="array<int>", json="string")
JSONS = ['{"a": 1, "b": "x"}', '{"a": -2}', '{"b": "y"}', '{"a": null}', 'null', '{"a": 3, "c": [1, 2]}']


def feat_table(rng, cols):
    def val(ty):
        if rng.random() < 0.2:
            return None
        if ty == "struct":
            return [None if rng.random() < 0.2 else rng.randint(-3, 3),
                    None if rng.random() < 0.2 else rng.choice(gen_rich.STRS)]
        if ty == "array":
            return [None if rng.random() < 0.15 else rng.randint(-2, 2) for _ in range(rng.randint(0, 3))]
        if ty == "json":
            return rng.choice(JSONS)
        return rng.randint(-3, 3)
    rows = [[val(ty) for _, ty in cols] for _ in range(rng.randint(0, 20))]
    if rows:
        rows += [json.loads(json.dumps(rng.choice(rows))) for _ in range(rng.randint(0, 3))]
    rng.shuffle(rows)
    return {"columns": [{"name": c, "type": TYPE_NAMES[ty]} for c, ty in cols], "rows": rows}


class FeatGen(RichGen):
    def types_in(self, refs):
        return [t for t in ("str", "dec", "date", "strl") if self.pick(refs, t)]

    def lit(self, ty):
        return super().lit("str" if ty == "strl" else ty)

    def expr(self, refs, ty, depth=0):
        if ty != "strl":
            return super().expr(refs, ty, depth)
        ref = self.pick(refs, ty)
        if ref is None:
            return self.lit(ty)
        r = self.rng.random()
        if r < 0.6 or depth > 1:
            return ref
        if r < 0.8:
            return f"COALESCE({ref}, {self.lit(ty)})"
        return f"UPPER({ref})"

    def flat_tf(self):
        a = self.alias()
        items = [(f"{a}.k", "int")]
        pool = [(f"{a}.st.x", "int"), (f"{a}.st.y", "str"), (f"size({a}.arr)", "int"),
                (f"CASE WHEN array_contains({a}.arr, 1) THEN 1 ELSE 0 END", "int"),
                (f"try_element_at({a}.arr, 1)", "int"),
                (f"aggregate(coalesce({a}.arr, array()), 0, (acc, e) -> acc + coalesce(e, 0))", "int"),
                (f"try_variant_get(parse_json({a}.js), '$.a', 'int')", "int"),
                (f"try_variant_get(parse_json({a}.js), '$.b', 'string')", "str"),
                (f"COLLATE({a}.st.y, 'UTF8_LCASE')", "strl")]
        items += self.rng.sample(pool, self.rng.randint(1, 4))
        sel = ", ".join(f"{e} AS f{i}" for i, (e, _) in enumerate(items))
        where = ""
        if self.rng.random() < 0.5:
            where = " WHERE " + self.rng.choice([f"{a}.st.x > 0", f"{a}.st.y IS NOT NULL", f"{a}.st.x IS NULL",
                                                 f"size({a}.arr) > 1", f"array_contains({a}.arr, 0)",
                                                 f"{a}.st.y = 'ab'"])
        src = "tf"
        if self.rng.random() < 0.3:
            b = self.alias()
            outer = "OUTER " if self.rng.random() < 0.5 else ""
            src = f"tf LATERAL VIEW {outer}explode({a}.arr) {b} AS e"
            items.append(("e", "int"))
            sel += f", e AS f{len(items) - 1}"
            return {"sql": f"(SELECT {sel} FROM tf {a} LATERAL VIEW {outer}explode({a}.arr) {b} AS e{where})",
                    "cols": [(f"f{i}", ty) for i, (_, ty) in enumerate(items)]}
        return {"sql": f"(SELECT {sel} FROM {src} {a}{where})", "cols": [(f"f{i}", ty) for i, (_, ty) in enumerate(items)]}

    def flat_tv(self):
        a = self.alias()
        items = [(f"{a}.k", "int"),
                 (f"try_variant_get({a}.v, '$.a', 'int')", "int"),
                 (f"try_variant_get({a}.v, '$.b', 'string')", "str"),
                 (f"CASE WHEN is_variant_null({a}.v) THEN 1 ELSE 0 END", "int")]
        items = items[:1] + self.rng.sample(items[1:], self.rng.randint(1, 3))
        sel = ", ".join(f"{e} AS f{i}" for i, (e, _) in enumerate(items))
        where = f" WHERE try_variant_get({a}.v, '$.a', 'int') > 0" if self.rng.random() < 0.4 else ""
        return {"sql": f"(SELECT {sel} FROM tv {a}{where})", "cols": [(f"f{i}", ty) for i, (_, ty) in enumerate(items)]}

    def base(self):
        r = self.rng.random()
        if r < 0.25:
            return self.flat_tf()
        if r < 0.35:
            return self.flat_tv()
        return super().base()


def main(seed, n, out, datasets=2, max_depth=4, per_dataset=8):
    rng = random.Random(int(seed))
    cases, shared = [], None
    for i in range(int(n)):
        if i % int(per_dataset) == 0:
            shared = []
            for _ in range(int(datasets)):
                ds = {t: table(rng, c) for t, c in gen_rich.TABLES.items()}
                ds["tf"] = feat_table(rng, FEAT_TABLES["tf"])
                shared.append(ds)
        g = FeatGen(rng, int(max_depth), False)
        sql, cols = g.query_cols()
        tlp = g.pred([(f"t.{c}", ty) for c, ty in cols], 1, False)
        uses = [t for t in ("tf", "tv") if f" {t} " in sql]
        cases.append({"id": f"F{int(seed) % 1000:03d}_{i:05d}", "family": "feat" + "".join("_" + t for t in uses),
                      "sql": sql, "tlp": tlp, "group": i // int(per_dataset),
                      "sqlite": False, "duckdb": False, "datasets": shared})
    with open(out, "w", newline="\n") as f:
        json.dump({"seed": int(seed), "cases": cases}, f)
    print("QUERYPROOF_FEAT_GENERATED", len(cases))


if __name__ == "__main__":
    main(*sys.argv[1:7])
