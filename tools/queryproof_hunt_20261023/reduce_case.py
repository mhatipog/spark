"""Greedy reduction of the excluded-rule set for a flaky wrong-result case.

Usage: python reduce_case.py CASES.json CASE_ID DATASET TRIES OUT.json
Starts from MINIMAL (every excludable rule except RewriteWithExpression excluded)
and re-enables one rule at a time; a rule stays re-enabled when the query still
returns a bag different from FULL in at least one of TRIES runs. Because the
failure is timing-dependent, the result is an upper bound on the rules that must
be excluded, not a proof that each one is necessary.
"""
import json
import sys
from collections import Counter

from pyspark.sql import SparkSession

from run_hunt import spark_schema

P = "org.apache.spark.sql.catalyst.optimizer."


def main(cases_path, case_id, ds, tries, out_path):
    tries = int(tries)
    case = next(c for c in json.loads(open(cases_path).read())["cases"] if c["id"] == case_id)
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    for name, t in case["datasets"][int(ds)].items():
        spark.createDataFrame([tuple(r) for r in t["rows"]], spark_schema(t)).createOrReplaceTempView(name)
    opt = spark._jsparkSession.sessionState().optimizer()
    names = set()
    b = opt.defaultBatches()
    for i in range(b.size()):
        rs = b.apply(i).rules()
        for j in range(rs.size()):
            names.add(rs.apply(j).ruleName())
    ne = opt.nonExcludableRules()
    keep = {ne.apply(i) for i in range(ne.size())} | {P + "RewriteWithExpression"}
    excluded = sorted(names - keep)

    def bag():
        return Counter(tuple(r) for r in spark.sql(case["sql"]).collect())

    spark.conf.unset("spark.sql.optimizer.excludedRules")
    full = bag()

    def wrong(excl):
        if excl:
            spark.conf.set("spark.sql.optimizer.excludedRules", ",".join(excl))
        else:
            spark.conf.unset("spark.sql.optimizer.excludedRules")
        try:
            hits = sum(1 for _ in range(tries) if bag() != full)
        except Exception:  # noqa: BLE001 - a crash is not this wrong result
            hits = 0
        spark.conf.unset("spark.sql.optimizer.excludedRules")
        return hits

    start = wrong(excluded)
    print("start", len(excluded), "rules excluded, wrong", start, "/", tries)
    need = list(excluded)
    if start:
        for rule in excluded:
            trial = [r for r in need if r != rule]
            if wrong(trial):
                need = trial
        print("reduced to", [r.replace(P, "") for r in need])
    final = wrong(need) if start else 0
    final_rate = sum(1 for _ in range(3) if wrong(need)) if start else 0
    spark.stop()
    result = {"case": case_id, "start_wrong": start, "tries": tries, "excluded": need,
              "final_wrong": final, "final_batches_wrong_of_3": final_rate}
    with open(out_path, "w", newline="\n") as f:
        json.dump(result, f, indent=1)
    print("QUERYPROOF_REDUCE", json.dumps(result))


if __name__ == "__main__":
    main(*sys.argv[1:6])
