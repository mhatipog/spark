"""Single-rule ablation: every excludable optimizer rule is excluded on its own.

Usage: python run_ablation.py CASES.json OUT.json [SHARD COUNT]
spark.sql.optimizer.excludedRules is a public configuration, so with any single rule
excluded Spark must still plan the query and return the FULL result bag. A failure
(crash, "No plan", INTERNAL_ERROR) or a different bag is a candidate. Rules are
sharded; every shard runs all cases on the first dataset only.
"""
import json
import sys
import time
from collections import Counter

from pyspark.sql import SparkSession

from run_hunt import bag, spark_rows

KNOWN = {  # upstream issues already reported for this exact configuration
    "org.apache.spark.sql.catalyst.optimizer.RewriteWithExpression": "SPARK-59019 (fixed on master)",
    "org.apache.spark.sql.catalyst.optimizer.EliminateOffsets": "SPARK-59044 (fixed on master)",
    "org.apache.spark.sql.catalyst.optimizer.ConstantFolding": "SPARK-57125 (open) for LIMIT+OFFSET",
    "org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases": "SPARK-59682 (fixed on master)",
}


def main(cases_path, out_path, shard=0, count=1):
    shard, count = int(shard), int(count)
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    optimizer = spark._jsparkSession.sessionState().optimizer()
    names = set()
    batches = optimizer.defaultBatches()
    for i in range(batches.size()):
        rules = batches.apply(i).rules()
        for j in range(rules.size()):
            names.add(rules.apply(j).ruleName())
    non_ex = optimizer.nonExcludableRules()
    keep = {non_ex.apply(i) for i in range(non_ex.size())}
    rules = sorted(names - keep)[shard::count]
    cases = json.loads(open(cases_path).read())["cases"]
    t0 = time.time()
    by_dataset = {}
    for case in cases:
        by_dataset.setdefault(json.dumps(case["datasets"][0], sort_keys=True), []).append(case)
    reference, findings, ran = {}, [], Counter()
    for key, group in by_dataset.items():
        for name, t in json.loads(key).items():
            schema = ", ".join(f"{c['name']} int" for c in t["columns"])
            spark.createDataFrame([tuple(r) for r in t["rows"]], schema).createOrReplaceTempView(name)
        for case in group:
            spark.conf.unset("spark.sql.optimizer.excludedRules")
            reference[case["id"]] = spark_rows(spark, case["sql"])
        for rule in rules:
            spark.conf.set("spark.sql.optimizer.excludedRules", rule)
            for case in group:
                ref = reference[case["id"]]
                if "rows" not in ref:
                    continue
                got = spark_rows(spark, case["sql"])
                ran[rule] += 1
                if "rows" not in got or bag(got["rows"]) != bag(ref["rows"]):
                    findings.append({"rule": rule, "known": KNOWN.get(rule), "case": case["id"],
                                     "family": case["family"], "sql": case["sql"],
                                     "kind": "ERROR" if "rows" not in got else "RESULT",
                                     "error": got.get("error"),
                                     "rows": None if "rows" not in got else {"full": ref["rows"],
                                                                             "ablated": got["rows"]}})
        spark.conf.unset("spark.sql.optimizer.excludedRules")
    spark.stop()
    summary = {"rules": len(rules), "cases": len(cases), "runs": sum(ran.values()),
               "seconds": round(time.time() - t0, 1), "findings": len(findings),
               "findings_by_rule": dict(Counter(f["rule"].rsplit(".", 1)[-1] + ":" + f["kind"] for f in findings))}
    with open(out_path, "w", newline="\n") as f:
        json.dump({"summary": summary, "rules": rules, "findings": findings}, f, indent=1)
    print("QUERYPROOF_ABLATION", json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:5])
