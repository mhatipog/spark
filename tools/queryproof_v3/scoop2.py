import json, subprocess, time
Q = {
  "I1 float literal": [("apache/iceberg-python", "FloatLiteral"), ("apache/iceberg-python", "float32 filter"), ("apache/iceberg-python", "float column equality filter"), ("apache/iceberg-python", "_value32")],
  "I2 signed zero": [("apache/iceberg-python", "negative zero"), ("apache/iceberg-python", "-0.0")],
  "I3 NaN identity partition": [("apache/iceberg-python", "NaN partition"), ("apache/iceberg-python", "ZeroDivisionError"), ("apache/iceberg-python", "bin_pack_arrow_table")],
  "day transform pin": [("apache/iceberg-python", "pyiceberg-core 0.11"), ("apache/iceberg-python", "day transform pre-epoch"), ("apache/iceberg-rust", "day transform pre-epoch")],
  "D1 decimal stats": [("delta-io/delta-rs", "decimal statistics"), ("delta-io/delta-rs", "decimal stats precision"), ("delta-io/delta-rs", "decimal min max stats f64")],
  "D2 NaN stats": [("delta-io/delta-rs", "NaN statistics"), ("delta-io/delta-rs", "NaN stats"), ("delta-io/delta-rs", "infinity stats"), ("duckdb/duckdb-delta", "NaN")],
}
for k, qs in Q.items():
    seen = {}
    for repo, q in qs:
        for kind in ("issues", "prs"):
            time.sleep(2.7)
            o = subprocess.run(["gh", "search", kind, "--repo", repo, q, "--limit", "12", "--json", "number,title,state,createdAt,url"], capture_output=True, text=True)
            if o.returncode: print("  GH ERROR", repo, q, o.stderr[:100]); continue
            for i in json.loads(o.stdout or "[]"): seen[i["url"]] = f"{i['createdAt'][:10]} {i['state']} {i['title']}"
    print("==", k); [print("  ", u, s) for u, s in seen.items()]
