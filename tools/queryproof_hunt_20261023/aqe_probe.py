"""AQE probe: can OptimizeOneRowPlan drop a DISTINCT over a join under the default configuration?

Usage: python aqe_probe.py OUT.json [REPEATS]
LogicalQueryStage.computeStats takes the statistics of the first QueryStageExec in
its physical subtree. When that subtree is a join, the row count is the count of one
input, not of the join, so a 0- or 1-row input can make the join look like it has at
most one row. AQE's OptimizeOneRowPlan then removes a group-only Aggregate (DISTINCT)
or drops DISTINCT inside aggregate functions above it. Every probe joins a 0/1-row
side with a side that has many duplicate rows, under several join strategies, and is
repeated because the outcome depends on which stage materializes first. Expected bags
come from the same query with AQE disabled.
"""
import json
import sys
from collections import Counter

from pyspark.sql import SparkSession

SMALL = ["s1", "s0"]                       # one row / no rows
HINTS = ["", "/*+ BROADCAST(s) */ ", "/*+ MERGE(s, b) */ ", "/*+ SHUFFLE_HASH(s) */ "]
SIDES = {
    "table": "{small}",
    "distinct": "(SELECT DISTINCT k FROM {small})",
    "grouped": "(SELECT k, COUNT(*) AS n FROM {small} GROUP BY k)",
    "limited": "(SELECT k FROM {small} ORDER BY k LIMIT 1)",
}
SHAPES = {
    "distinct_inner": "SELECT {h}DISTINCT b.x FROM {side} s JOIN big b ON s.k = b.k",
    "distinct_right": "SELECT {h}DISTINCT b.x FROM {side} s RIGHT JOIN big b ON s.k = b.k",
    "distinct_left_big": "SELECT {h}DISTINCT b.x FROM big b LEFT JOIN {side} s ON s.k = b.k",
    "distinct_full": "SELECT {h}DISTINCT b.x FROM {side} s FULL JOIN big b ON s.k = b.k",
    "count_distinct": "SELECT {h}COUNT(DISTINCT b.x) AS n FROM {side} s RIGHT JOIN big b ON s.k = b.k",
    "group_only": "SELECT {h}b.x FROM {side} s RIGHT JOIN big b ON s.k = b.k GROUP BY b.x",
    "distinct_over_derived": ("SELECT DISTINCT y FROM (SELECT {h}b.x AS y FROM {side} s RIGHT JOIN big b "
                              "ON s.k = b.k) d"),
}


def main(out_path, repeats=8):
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.createDataFrame([(1,)], "k int").createOrReplaceTempView("s1")
    spark.createDataFrame([], "k int").createOrReplaceTempView("s0")
    spark.createDataFrame([(1, i % 3) for i in range(60)], "k int, x int").repartition(3).createOrReplaceTempView("big")
    findings, runs = [], 0
    for shape, tmpl in SHAPES.items():
        for side_name, side in SIDES.items():
            for small in SMALL:
                for h in HINTS:
                    sql = tmpl.format(h=h, side=side.format(small=small))
                    spark.conf.set("spark.sql.adaptive.enabled", "false")
                    try:
                        expected = Counter(tuple(r) for r in spark.sql(sql).collect())
                    except Exception as exc:  # noqa: BLE001
                        print("SKIP", sql, str(exc).split("\n")[0][:100])
                        continue
                    spark.conf.set("spark.sql.adaptive.enabled", "true")
                    wrong = Counter()
                    for _ in range(int(repeats)):
                        df = spark.sql(sql)
                        got = Counter(tuple(r) for r in df.collect())
                        runs += 1
                        if got != expected:
                            wrong[json.dumps(sorted(got.items()))] += 1
                            plan = df._jdf.queryExecution().executedPlan().toString()
                    if wrong:
                        findings.append({"sql": sql, "expected": sorted(expected.items()), "wrong": dict(wrong),
                                         "repeats": int(repeats), "plan": plan[:4000]})
                        print("WRONG", sum(wrong.values()), "/", repeats, sql)
    spark.stop()
    summary = {"runs": runs, "findings": len(findings)}
    with open(out_path, "w", newline="\n") as f:
        json.dump({"summary": summary, "findings": findings}, f, indent=1, default=str)
    print("QUERYPROOF_AQE_PROBE", json.dumps(summary))


if __name__ == "__main__":
    main(*sys.argv[1:3])
