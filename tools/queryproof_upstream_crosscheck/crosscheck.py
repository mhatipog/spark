"""Cross-check an existing upstream fix against the separate QueryProof Sort guard.

Precommitted frozen corpus, official PySpark 4.2.0 JARs, same per-arm test code.
This records failures; it does not pretend a green pipeline proves correctness.
"""
from collections import Counter
from pathlib import Path
import json
import sys
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "queryproof_synthesis"))
from generate import MANIFEST, Case, sql, reference

JIRA = """WITH top AS (
 SELECT id, CAST(id % 1 AS INT) AS k FROM range(0, 3, 1, 2)
 ORDER BY id ASC LIMIT 1 OFFSET 1
), b AS (
 SELECT id AS bid, CAST(id % 1 AS INT) AS k FROM range(0, 2, 1, 2)
)
SELECT top.id, top.k, b.bid FROM top LEFT JOIN b USING (k)
ORDER BY top.id ASC, b.bid ASC"""
MINIMIZED = """WITH top AS (
 SELECT id, CAST(id % 1 AS INT) AS k FROM range(0,3,1,1)
 ORDER BY id ASC LIMIT 2 OFFSET 1
), rhs AS (
 SELECT id AS bid, CAST(id % 1 AS INT) AS k FROM range(0,1,1,1)
)
SELECT top.id,top.k,rhs.bid FROM top LEFT JOIN rhs USING(k)
ORDER BY top.id ASC,rhs.bid ASC"""
JIRA_EXPECTED = [[1, 0, 0], [1, 0, 1]]
MIN_EXPECTED = [[1, 0, 0], [2, 0, 0]]
INLINE = "org.apache.spark.sql.catalyst.optimizer.InlineCTE"


def evaluate(spark, statement, expected, candidate=True):
    ss = spark.newSession()
    ss.conf.set("spark.sql.adaptive.enabled", "true")
    ss.conf.set("spark.sql.shuffle.partitions", "5")
    if candidate:
        ss.conf.set("spark.sql.optimizer.excludedRules", INLINE)
    try:
        df = ss.sql(statement)
        rows = [list(row) for row in df.collect()]
        same_bag = Counter(map(tuple, rows)) == Counter(map(tuple, expected))
        output = {
            "actual": rows, "bag_ok": same_bag,
            "order_ok": rows == expected,
        }
        if rows != expected:
            output["final_plan"] = df._jdf.queryExecution().executedPlan().toString()
        return output
    except Exception as ex:
        return {"error": repr(ex)}


def summarize(rows):
    return {
        "trials": len(rows),
        "errors": sum("error" in x for x in rows),
        "bag_violations": sum(x.get("bag_ok") is False for x in rows),
        "ordering_violations": sum(x.get("bag_ok") is True and x.get("order_ok") is False
                                   for x in rows),
    }


def main():
    arm = sys.argv[1]
    assert arm in {"baseline", "sort_guard", "upstream_cardinality"}, arm
    spark = (SparkSession.builder.master("local[2]")
        .appName("SPARK-59126-upstream-fix-control-" + arm)
        .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    report = {
        "version": spark.version, "arm": arm,
        "sql_reproducers": {}, "frozen_corpus": {},
        "controls": {},
    }
    try:
        for name, statement, expected in (
            ("jira_original", JIRA, JIRA_EXPECTED),
            ("queryproof_minimized", MINIMIZED, MIN_EXPECTED),
        ):
            for config, repetitions in (("candidate", 8), ("normal", 3)):
                results = [evaluate(spark, statement, expected, candidate=(config=="candidate"))
                           for _ in range(repetitions)]
                report["sql_reproducers"][name + "/" + config] = {
                    "runs": results, "summary": summarize(results),
                }
                print("QUERYPROOF_CROSSCHECK_SQL", spark.version, arm, name, config,
                      json.dumps(summarize(results)), flush=True)
        all_runs = []
        for entry in MANIFEST["cases"]:
            cid = entry["case_id"]
            case = Case(**{k: v for k, v in entry.items() if k not in ("case_id", "tier")})
            expected = reference(case)
            runs = [evaluate(spark, sql(case), expected) for _ in range(2)]
            all_runs.extend(runs)
            report["frozen_corpus"][cid] = {
                "tier": entry["tier"], "runs": runs,
            }
        summary = summarize(all_runs)
        report["frozen_corpus_summary"] = summary
        print("QUERYPROOF_CROSSCHECK_CORPUS", spark.version, arm, json.dumps(summary),
              flush=True)

        controls = {
            "one_row": ("SELECT id FROM range(1) ORDER BY id", [[0]]),
            "descending": ("SELECT id FROM range(5) ORDER BY id DESC",
                           [[4],[3],[2],[1],[0]]),
            "scalar_subquery": ("SELECT (SELECT 1) AS a", [[1]]),
        }
        for name, (statement, expected) in controls.items():
            report["controls"][name] = evaluate(spark, statement, expected, candidate=False)
        print("QUERYPROOF_CROSSCHECK_CONTROLS", arm,
              {k:v.get("order_ok") for k,v in report["controls"].items()}, flush=True)
    finally:
        spark.stop()
        Path("artifacts").mkdir(exist_ok=True)
        Path("artifacts/crosscheck_" + arm + ".json").write_text(
            json.dumps(report, indent=2), encoding="utf-8")

    summaries = [v["summary"] for v in report["sql_reproducers"].values()]
    summaries.append(report["frozen_corpus_summary"])
    errors = sum(s["errors"] for s in summaries)
    bags = sum(s["bag_violations"] for s in summaries)
    control_errors = sum(v.get("order_ok") is not True for v in report["controls"].values())
    print("QUERYPROOF_CROSSCHECK_VERDICT", arm,
          json.dumps({"errors": errors, "bag_violations": bags,
                      "control_failures": control_errors,
                      "corpus_wrong_order": report["frozen_corpus_summary"]["ordering_violations"],
                      "jira_wrong_order": report["sql_reproducers"]["jira_original/candidate"]["summary"]["ordering_violations"]}),
          flush=True)
    # The baseline is a known-bug observation arm, not a 'should pass' target.
    # Do not require the upstream fix arm to pass: we need its honest empirical result.
    # All arms must execute without errors, preserve bags, and pass simple controls.
    return 2 if errors or bags or control_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
