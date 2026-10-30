"""Re-run one hunt case many times per configuration to separate flaky results from stable ones.

Usage: python repeat_case.py CASES.json CASE_ID DATASET REPEATS OUT.json
Runs FULL and MINIMAL, each also with AQE off and with 1 and 8 shuffle partitions,
and records the distinct result bags seen per configuration with their counts and,
for every bag that differs from FULL, the executed physical plan.
"""
import json
import sys
from collections import Counter

from pyspark.sql import SparkSession

from run_hunt import spark_schema

P = "org.apache.spark.sql.catalyst.optimizer."


def main(cases_path, case_id, ds, repeats, out_path):
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
    minimal = ",".join(sorted(names - keep))
    configs = {}
    for opt_label, excl in (("full", None), ("minimal", minimal)):
        for extra_label, extra in (("", {}), ("+aqe_off", {"spark.sql.adaptive.enabled": "false"}),
                                   ("+sp1", {"spark.sql.shuffle.partitions": "1"}),
                                   ("+sp8", {"spark.sql.shuffle.partitions": "8"})):
            configs[opt_label + extra_label] = (excl, extra)
    report = {}
    for label, (excl, extra) in configs.items():
        if excl:
            spark.conf.set("spark.sql.optimizer.excludedRules", excl)
        else:
            spark.conf.unset("spark.sql.optimizer.excludedRules")
        for k, v in extra.items():
            spark.conf.set(k, v)
        seen, plans = Counter(), {}
        for _ in range(int(repeats)):
            df = spark.sql(case["sql"])
            key = json.dumps(sorted(Counter(tuple(r) for r in df.collect()).items()))
            seen[key] += 1
            if key not in plans:
                plans[key] = df._jdf.queryExecution().executedPlan().toString()[:8000]
        report[label] = {"bags": dict(seen), "plans": plans}
        print(label, dict(seen))
        for k in extra:
            spark.conf.unset(k)
        spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.unset("spark.sql.optimizer.excludedRules")
    spark.stop()
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:6])
