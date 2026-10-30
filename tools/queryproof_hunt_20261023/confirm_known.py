"""Attribute crash candidates to known upstream issues by re-enabling the rule.

Usage: python confirm_known.py CANDIDATES.json CASES.json OUT.json
CANDIDATES.json is a triage or ablation output. For every ERROR candidate the
query is rerun on its first dataset with the crashing configuration minus one
known rule at a time. A candidate is KNOWN when re-enabling that rule alone makes
it succeed and return the FULL bag; otherwise it stays UNEXPLAINED and is printed
with its root-cause message.
"""
import json
import sys

from pyspark.sql import SparkSession

from run_hunt import bag, spark_rows, spark_schema

P = "org.apache.spark.sql.catalyst.optimizer."
KNOWN = {
    P + "EliminateOffsets": "SPARK-59044",
    P + "ConstantFolding": "SPARK-57125",
    P + "RewriteWithExpression": "SPARK-59019",
    P + "RemoveRedundantAliases": "SPARK-59682",
}


def root_cause(spark, sql):
    try:
        spark.sql(sql).collect()
        return None
    except Exception as exc:  # noqa: BLE001
        c = getattr(exc, "java_exception", None)
        while c is not None and c.getCause() is not None:
            c = c.getCause()
        return (c.toString() if c is not None else str(exc)).split("\n")[0][:300]


def main(cands_path, cases_path, out_path):
    doc = json.loads(open(cands_path).read())
    cases = {c["id"]: c for c in json.loads(open(cases_path).read())["cases"]}
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    opt = spark._jsparkSession.sessionState().optimizer()
    names = set()
    b = opt.defaultBatches()
    for i in range(b.size()):
        rs = b.apply(i).rules()
        for j in range(rs.size()):
            names.add(rs.apply(j).ruleName())
    ne = opt.nonExcludableRules()
    keep = {ne.apply(i) for i in range(ne.size())} | {P + "RewriteWithExpression"}
    minimal = sorted(names - keep)
    out, seen = [], set()
    for c in doc["candidates"] if "candidates" in doc else doc["findings"]:
        if c["kind"] != "ERROR" or c["case"] in seen:
            continue
        seen.add(c["case"])
        case = cases[c["case"]]
        for name, t in case["datasets"][0].items():
            spark.createDataFrame([tuple(r) for r in t["rows"]], spark_schema(t)).createOrReplaceTempView(name)
        excluded = [c["rule"]] if "rule" in c else minimal
        spark.conf.unset("spark.sql.optimizer.excludedRules")
        ref = spark_rows(spark, case["sql"])
        spark.conf.set("spark.sql.optimizer.excludedRules", ",".join(excluded))
        cause = root_cause(spark, case["sql"])
        verdict = {"case": c["case"], "excluded": len(excluded), "root_cause": cause, "known": None}
        for rule, issue in KNOWN.items():
            if rule not in excluded:
                continue
            rest = [r for r in excluded if r != rule]
            if rest:
                spark.conf.set("spark.sql.optimizer.excludedRules", ",".join(rest))
            else:
                spark.conf.unset("spark.sql.optimizer.excludedRules")
            got = spark_rows(spark, case["sql"])
            if "rows" in got and "rows" in ref and bag(got["rows"]) == bag(ref["rows"]):
                verdict["known"] = issue
                break
        spark.conf.unset("spark.sql.optimizer.excludedRules")
        out.append(verdict)
        print(("KNOWN " + verdict["known"]) if verdict["known"] else "UNEXPLAINED", c["case"], cause)
    spark.stop()
    with open(out_path, "w", newline="\n") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
