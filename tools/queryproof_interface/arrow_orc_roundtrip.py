"""A1: pyarrow ORC writer round trip of struct<i: struct<c>> with a NULL inner struct and no NULL outer struct."""
import os, tempfile, pyarrow as pa, pyarrow.orc as orc
I = pa.struct([("c", pa.int32())])
t = pa.table({"o": pa.array([{"i": {"c": 1}}, {"i": None}, {"i": {"c": 3}}], pa.struct([("i", I)]))})
p = os.path.join(tempfile.mkdtemp(), "a.orc")
orc.write_table(t, p)
back = orc.read_table(p).to_pylist()
print(pa.__version__, "OK" if back == t.to_pylist() else "BAD", back)
