"""Spec-driven storage-interface hunt: adversarial files x fixed templates x engines x modes.

Usage: python run_interface.py ENGINES BASE OUT.json [SHARD COUNT [SPARK_TAG]]
  ENGINES   comma list of spark, duckdb, datafusion, trino (a Trino server on localhost:8080)
  BASE      directory holding writers.py output and manifest.json
Shards split the shapes. Findings:
  skip_diff     a mode's bag differs from the same engine's "off" bag
  mode_error    a mode fails where "off" succeeds (or the reverse: off_error)
  semantic_diff "off" differs from the generator's Python answer (triaged separately)
Prints QUERYPROOF_CPU interface <engine> <core-seconds> for the bugs-per-CPU-hour accounting.
"""
import json
import os
import sys
import time
from collections import Counter

import engines as E
import writers
from shapes import SHAPES
from templates import bag, build_queries, expected, query_sql, shape_rows, supported


def main(engine_list, base, out_path, shard=0, count=1, spark_tag="4.2.1"):
    shard, count = int(shard), int(count)
    manifest = json.load(open(os.path.join(base, "manifest.json")))
    mine = [s for i, s in enumerate(SHAPES) if i % count == shard]
    names = engine_list.split(",")
    engines = []
    for n in names:
        engines.append({"spark": lambda: E.Spark(spark_tag), "duckdb": E.DuckDB, "datafusion": E.DataFusion,
                        "trino": lambda: E.Trino(base)}[n]())
    spark = next((e for e in engines if isinstance(e, E.Spark)), None)
    if spark is not None:  # Spark's own writers add file sets that every engine reads
        t0 = time.time()
        for s in mine:
            for wname, (fmt, opts) in writers.SPARK_WRITERS.items():
                d = os.path.join(base, s["id"], wname + "-" + spark.name)
                try:
                    spark.write(s, wname, fmt, opts, d)
                    manifest.append(writers.entry(s, wname, fmt, spark.name, d))
                except Exception as exc:  # noqa: BLE001
                    print("QUERYPROOF_IFACE_WRITE_FAILED", s["id"], wname, str(exc).split("\n")[0][:200],
                          file=sys.stderr)
        print("QUERYPROOF_IFACE_SPARK_WRITE_SECONDS", round(time.time() - t0, 1), file=sys.stderr)
    findings, counts, cpu = [], Counter(), Counter()
    for s in mine:
        qs, leaf_types = build_queries(s)
        rows = shape_rows(s)
        exp = {qid: expected(q, rows, leaf_types) for qid, q in qs}
        sets = [m for m in manifest if m["shape"] == s["id"]]
        for eng in engines:
            todo = [(qid, q) for qid, q in qs if supported(q, eng.dialect)]
            sqls = [query_sql(q, eng.dialect, leaf_types) for _, q in todo]
            for fs in sets:
                if fs["format"] not in eng.formats:
                    continue
                t0 = time.time()
                res = {mode: eng.run(mode, fs, s, sqls) for mode in eng.modes}
                cpu[eng.name] += (time.time() - t0) * eng.cores
                off = res["off"]
                for i, (qid, q) in enumerate(todo):
                    counts["checks"] += len(eng.modes)
                    base_r = off[i]
                    kinds = []
                    for mode, rr in res.items():
                        if mode == "off":
                            continue
                        r = rr[i]
                        if "rows" in base_r and "rows" in r and bag(r["rows"]) != bag(base_r["rows"]):
                            kinds.append(("skip_diff", mode))
                        elif "rows" in base_r and "rows" not in r:
                            kinds.append(("mode_error", mode))
                        elif "rows" not in base_r and "rows" in r:
                            kinds.append(("off_error", mode))
                    e = exp[qid]
                    if e is not None and "rows" in base_r and bag(base_r["rows"]) != sorted(
                            json.dumps(x, sort_keys=True, default=str) for x in e):
                        kinds.append(("semantic_diff", "off"))
                    for k, m in kinds:
                        counts[f"{k}:{eng.name}"] += 1
                    if kinds:
                        findings.append({
                            "engine": eng.name, "shape": s["id"], "concept": s["concept"],
                            "writer_set": fs["writer_set"], "writer": fs["writer"], "format": fs["format"],
                            "qid": qid, "sql": sqls[i], "kinds": kinds, "expected": e,
                            "results": {m: res[m][i] for m in ["off"] + sorted({m for _, m in kinds} - {"off"})}})
    for n, sec in cpu.items():
        print("QUERYPROOF_CPU interface", n, round(sec, 1))
    summary = {"shapes": len(mine), **dict(counts), "findings": len(findings),
               "cpu_seconds": {k: round(v, 1) for k, v in cpu.items()}}
    with open(out_path, "w", newline="\n") as fh:
        json.dump({"summary": summary, "findings": findings,
                   "file_sets": [m for m in manifest if m["shape"] in {s["id"] for s in mine}]},
                  fh, indent=1, default=str)
    print("QUERYPROOF_IFACE", json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:7])
