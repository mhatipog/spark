"""Re-run single rich-hunt cases over their Parquet tables and print root causes.

Usage: python meta_case.py CASES.json OUT.json CASE:DATASET[:VARIANT[+VARIANT]] ...
For each case the tables are written exactly as run_meta.py writes them; the query
runs under the default configuration and each listed variant (REPEATS times, env
QP_REPEATS, default 5), recording result bags, the Java root cause and the first
stack frames of any failure, and the executed plan of the first run per configuration.
"""
import json
import os
import shutil
import sys
import tempfile
from collections import Counter

from pyspark.sql import SparkSession

import run_meta as M


def root_cause(exc):
    c = getattr(exc, "java_exception", None)
    top = c.toString().split("\n")[0][:500] if c is not None else str(exc)[:500]
    while c is not None and c.getCause() is not None:
        c = c.getCause()
    if c is None:
        return {"top": top}
    return {"top": top, "root": c.toString().split("\n")[0][:500],
            "frames": [f.toString() for f in list(c.getStackTrace())[:25]]}


def main(cases_path, out_path, *specs):
    repeats = int(os.environ.get("QP_REPEATS", "5"))
    wh = tempfile.mkdtemp(prefix="qp_wh_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").config("spark.sql.warehouse.dir", wh)
             .getOrCreate())
    cases = {c["id"]: c for c in json.loads(open(cases_path).read())["cases"]}
    report = {}
    for spec in specs:
        parts = spec.split(":")
        cid, d = parts[0], int(parts[1])
        variants = ["default"] + (parts[2].split("+") if len(parts) > 2 and parts[2] else [])
        case = cases[cid]
        M.write_tables(spark, case["datasets"][d])
        rep = {}
        for v in variants:
            conf = {} if v == "default" else M.VARIANTS[v]
            for k, val in conf.items():
                spark.conf.set(k, val)
            seen, failure, plan = Counter(), None, None
            for _ in range(repeats):
                df = spark.sql(case["sql"])
                try:
                    rows = df.collect()
                    seen[json.dumps(sorted(Counter(tuple(r) for r in rows).items()), default=str)] += 1
                except Exception as exc:  # noqa: BLE001
                    seen["ERROR"] += 1
                    failure = failure or root_cause(exc)
                if plan is None:
                    try:
                        plan = df._jdf.queryExecution().executedPlan().toString()[:6000]
                    except Exception:  # noqa: BLE001
                        plan = "unavailable"
            for k in conf:
                spark.conf.unset(k)
            rep[v] = {"bags": dict(seen), "failure": failure, "plan": plan}
            print(cid, d, v, dict((k[:80], n) for k, n in seen.items()))
            if failure:
                print("   root:", failure.get("root"))
                for f in failure.get("frames", [])[:12]:
                    print("     at", f)
        report[spec] = {"sql": case["sql"], "results": rep}
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1, default=str)


if __name__ == "__main__":
    main(*sys.argv[1:])
