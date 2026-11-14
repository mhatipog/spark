"""Hunt HI: pyarrow ORC writer/reader round trip over random nested schemas (family: writer soundness)."""
import datetime as dt, decimal as D, io, json, random, sys, traceback
from collections import Counter
import pyarrow as pa, pyarrow.orc as orc

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
N = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
rng = random.Random(SEED)
LEAVES = [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.float32(), pa.float64(), pa.bool_(), pa.string(), pa.binary(),
          pa.date32(), pa.timestamp("ns"), pa.decimal128(10, 2), pa.decimal128(38, 6), pa.large_string()]

def rtype(depth):
    r = rng.random()
    if depth >= 3 or r < 0.45: return rng.choice(LEAVES)
    if r < 0.7: return pa.struct([(f"f{i}", rtype(depth + 1)) for i in range(rng.randint(1, 3))])
    if r < 0.88: return pa.list_(rtype(depth + 1))
    return pa.map_(rng.choice([pa.int32(), pa.string()]), rtype(depth + 1))

def rval(t, pnull):
    if rng.random() < pnull: return None
    if pa.types.is_struct(t): return {f.name: rval(f.type, pnull) for f in t}
    if pa.types.is_map(t):
        ks = set()
        out = []
        for _ in range(rng.randint(0, 3)):
            k = rval(t.key_type, 0)
            if k in ks: continue
            ks.add(k); out.append((k, rval(t.item_type, pnull)))
        return out
    if pa.types.is_list(t): return [rval(t.value_type, pnull) for _ in range(rng.randint(0, 3))]
    if t == pa.int8(): return rng.randint(-128, 127)
    if t == pa.int16(): return rng.randint(-32768, 32767)
    if t == pa.int32(): return rng.choice([rng.randint(-2**31, 2**31 - 1), 0, 1, -1])
    if t == pa.int64(): return rng.choice([rng.randint(-2**63, 2**63 - 1), 0, 7])
    if t in (pa.float32(), pa.float64()): return rng.choice([0.5, -1.25, 3.0, 0.0])
    if t == pa.bool_(): return rng.random() < 0.5
    if t in (pa.string(), pa.large_string()): return rng.choice(["", "a", "xyz", "é", "\U0001f600", "x" * 50])
    if t == pa.binary(): return rng.choice([b"", b"\x00", b"ab"])
    if t == pa.date32(): return dt.date(rng.randint(1900, 2100), rng.randint(1, 12), rng.randint(1, 28))
    if pa.types.is_timestamp(t):
        v = dt.datetime(rng.randint(1970, 2100), 1, 1, rng.randint(0, 23), 0, 0, rng.choice([0, 1, 999999]))
        return v.replace(tzinfo=dt.timezone.utc) if t.tz else v
    if pa.types.is_decimal(t): return D.Decimal(rng.randint(-10**8, 10**8)).scaleb(-t.scale)
    raise ValueError(t)

def a1_shape(t, under_struct=False):
    if pa.types.is_struct(t):
        if under_struct: return True
        return any(a1_shape(f.type, True) for f in t)
    if pa.types.is_list(t): return a1_shape(t.value_type, under_struct)
    if pa.types.is_map(t): return a1_shape(t.item_type, under_struct)
    return False

def norm(x):
    if isinstance(x, dict): return {k: norm(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)): return [norm(v) for v in x]
    return x

found = Counter(); examples = {}
for it in range(N):
    ncols = rng.randint(1, 3)
    schema = pa.schema([(f"c{i}", rtype(0)) for i in range(ncols)])
    if any(a1_shape(f.type) for f in schema): continue
    nrows = rng.choice([1, 2, 3, 5, 17, 100, 1100])
    pnull = rng.choice([0.0, 0.1, 0.3, 0.6])
    try:
        cols = {f.name: pa.array([rval(f.type, pnull) for _ in range(nrows)], f.type) for f in schema}
        t = pa.table(cols, schema=schema)
    except Exception as e:
        continue
    kw = rng.choice([{}, {"batch_size": 1}, {"batch_size": 7}, {"stripe_size": 1024}, {"compression": "zstd"}, {"row_index_stride": 10}])
    buf = io.BytesIO()
    try:
        orc.write_table(t, buf, **kw)
        back = orc.read_table(pa.BufferReader(buf.getvalue()))
        back = back.cast(t.schema) if back.schema != t.schema else back
        a, b = norm(t.to_pylist()), norm(back.to_pylist())
        if a != b:
            key = ("mismatch", str(schema)[:120])
            found["mismatch"] += 1
            if len(examples) < 30: examples[f"m{it}"] = dict(schema=str(schema), kw=kw, nrows=nrows, pnull=pnull,
                                                         first_diff=next(((i, x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y), None))
    except Exception as e:
        k = type(e).__name__ + ": " + str(e).splitlines()[0][:100]
        found[k] += 1
        if k not in examples: examples[k] = dict(schema=str(schema), kw=kw, nrows=nrows, pnull=pnull)
print("SEED", SEED, "N", N, dict(found))
for k, v in list(examples.items())[:30]: print(k, json.dumps(v, default=str)[:700])
