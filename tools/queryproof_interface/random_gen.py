"""Matched random baseline for the interface hunt: random shapes and random queries.

Same type language, writers, engines, modes and oracle as the spec-driven method
(shapes.py, templates.py); only the data and the predicates are random instead of derived
from the contracts. Values come from a generic fuzzer distribution: uniform ranges with
NULLs, plus a small share of the constants generic fuzzers commonly use (0, -1, 1, the
type's limits, the empty string). It deliberately has no contract-derived values (NaN,
signed zero, code points above U+FFFF, calendar-gap dates, truncation lengths), since
those are what the spec-driven method claims to add.
Enabled with QP_IFACE_GEN=random:SEED (see shapes.py).
"""
import datetime
import os
import random
from decimal import Decimal

TYPES = ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float64", "float32", "string", "date", "ts", "decimal(9,2)",
         "struct<x:int32,y:string>", "list<int32>", "map<string,int32>",
         "struct<i:struct<c:int32>,z:int32>"]
FUZZ_CONSTANTS = {"int8": [0, -1, 1, 127, -128], "int16": [0, -1, 1, 32767, -32768],
                  "uint8": [0, 1, 255], "uint16": [0, 1, 65535], "uint32": [0, 1, 2 ** 32 - 1],
                  "uint64": [0, 1, 2 ** 64 - 1],
                  "ts": [datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)],
                  "int32": [0, -1, 1, 2 ** 31 - 1, -2 ** 31], "int64": [0, -1, 1, 2 ** 63 - 1, -2 ** 63],
                  "float64": [0.0, -1.0, 1.0], "float32": [0.0, -1.0, 1.0], "string": [""],
                  "date": [datetime.date(1970, 1, 1)], "decimal(9,2)": [Decimal("0.00"), Decimal("-1.00")]}
NULL_P = 0.15


def value(rng, ty):
    """A random value of the shapes.py type `ty` (struct and map in their pyarrow forms)."""
    if rng.random() < NULL_P:
        return None
    if ty in FUZZ_CONSTANTS and rng.random() < 0.05:
        return rng.choice(FUZZ_CONSTANTS[ty])
    if ty in ("int8", "int16", "int32"):
        return rng.randint(-100, 100)
    if ty in ("uint8", "uint16", "uint32"):  # pair three: the same unsigned types as spec3
        return rng.randint(0, 200)
    if ty == "uint64":
        return rng.randint(0, 10 ** 6)
    if ty == "ts":
        return (datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                + datetime.timedelta(seconds=rng.randint(0, 2 * 10 ** 9), microseconds=rng.randint(0, 999999)))
    if ty == "int64":
        return rng.randint(-10 ** 6, 10 ** 6)
    if ty in ("float64", "float32"):
        return round(rng.uniform(-100, 100), 1)
    if ty == "string":
        return "".join(rng.choice("abcxyz") for _ in range(rng.randint(0, 5)))
    if ty == "date":
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=rng.randint(0, 25000))
    if ty == "decimal(9,2)":
        return Decimal(rng.randint(-99999, 99999)).scaleb(-2)
    if ty == "struct<x:int32,y:string>":
        return {"x": value(rng, "int32"), "y": value(rng, "string")}
    if ty == "struct<i:struct<c:int32>,z:int32>":
        inner = None if rng.random() < NULL_P else {"c": value(rng, "int32")}
        return {"i": inner, "z": value(rng, "int32")}
    if ty == "list<int32>":
        return [value(rng, "int32") for _ in range(rng.randint(0, 3))]
    if ty == "map<string,int32>":
        keys = rng.sample(["a", "b", "c"], rng.randint(0, 3))
        return [(k, value(rng, "int32")) for k in keys]
    raise ValueError(ty)


def make_shapes(seed, n=32):
    from shapes import shape
    rng = random.Random(seed)
    out = []
    for i in range(n):
        cols = [("k", "int32")] + [(f"c{j}", rng.choice(TYPES)) for j in range(rng.randint(1, 3))]
        nrows = rng.randint(0, 12)
        rows = [tuple([r + 1] + [value(rng, ty) for _, ty in cols[1:]]) for r in range(nrows)]
        # Rows into 1-4 files in random order; a file may be empty.
        idx = list(range(nrows))
        rng.shuffle(idx)
        cuts = sorted(rng.sample(range(nrows + 1), min(rng.randint(0, 3), nrows + 1)))
        files = [idx[a:b] for a, b in zip([0] + cuts, cuts + [nrows])]
        out.append(shape(f"R{seed % 1000:03d}_{i:03d}", "random", cols, rows, files=files,
                         note="random baseline"))
        out[-1]["random_seed"] = seed * 1000 + i
    return out


def random_queries(shape, leaf_types, n=None):
    """n random predicate and aggregate queries over the shape's leaves (templates.py AST)."""
    # QP_IFACE_RANDOM_Q matches the query count of the spec set it is compared with (pair one 114
    # for 3,636 contract queries, pair two 76 for 2,434).
    n = n or int(os.environ.get("QP_IFACE_RANDOM_Q", "114"))
    rng = random.Random(shape["random_seed"])
    paths = [p for p in leaf_types if p != ("k",)] or [("k",)]

    def lit(path):
        return ("lit", value(random.Random(rng.random()), leaf_types[path]), leaf_types[path])

    def atom():
        path = rng.choice(paths)
        col = ("col", path)
        r = rng.random()
        if r < 0.55:
            return ("cmp", rng.choice(["=", "<>", "<", "<=", ">", ">="]), col, lit(path))
        if r < 0.65:
            return ("isnull", col)
        if r < 0.75:
            return ("notnull", col)
        if r < 0.85:
            return ("in", col, [lit(path) for _ in range(rng.randint(1, 3))])
        if r < 0.95:
            return ("between", col, lit(path), lit(path))
        return ("nseq", col, lit(path))

    def pred(depth=0):
        r = rng.random()
        if depth < 2 and r < 0.2:
            return (rng.choice(["and", "or"]), pred(depth + 1), pred(depth + 1))
        if depth < 2 and r < 0.3:
            return ("not", pred(depth + 1))
        return atom()

    qs = []
    for i in range(n):
        if rng.random() < 0.8:
            qs.append((f"r:{i}", ("rows", pred())))
        else:
            qs.append((f"a:{i}", ("agg", rng.choice(paths), pred() if rng.random() < 0.7 else None)))
    return qs
