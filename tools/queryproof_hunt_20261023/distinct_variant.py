"""Default-configuration DISTINCT variants of the SPARK-57956 reproduction.

Usage: python distinct_variant.py OUT.json [REPEATS]
SPARK-57956 shows AQE's EliminateLimits dropping a root LIMIT because a
non-materialized LogicalQueryStage reports an estimated rowCount as maxRows. The same
maxRows feeds AQE's OptimizeOneRowPlan, which drops a group-only Aggregate (DISTINCT)
and turns COUNT(DISTINCT) into COUNT. Each query is run with AQE off (expected) and
repeatedly with AQE on, under the default optimizer.
"""
import json
import sys
from collections import Counter

from pyspark.sql import SparkSession

TOP = "top AS (SELECT id % 1 AS k FROM range(0, 2, 1, 1) OFFSET 1)"
B = "b AS (SELECT * FROM VALUES (0), (0) AS b(k))"
B3 = "b AS (SELECT * FROM VALUES (0, 1), (0, 1), (0, 2) AS b(k, x))"
QUERIES = {
    "jira_limit": f"WITH {TOP}, {B} SELECT 1 AS c FROM top LEFT JOIN b USING (k) LIMIT 1",
    "distinct_const": f"WITH {TOP}, {B} SELECT DISTINCT 1 AS c FROM top LEFT JOIN b USING (k)",
    "distinct_key": f"WITH {TOP}, {B} SELECT DISTINCT k FROM top LEFT JOIN b USING (k)",
    "group_by": f"WITH {TOP}, {B} SELECT k FROM top LEFT JOIN b USING (k) GROUP BY k",
    "distinct_x": f"WITH {TOP}, {B3} SELECT DISTINCT b.x FROM top LEFT JOIN b ON top.k = b.k",
    "count_distinct": f"WITH {TOP}, {B3} SELECT COUNT(DISTINCT b.x) AS n FROM top LEFT JOIN b ON top.k = b.k",
    "sum_distinct": f"WITH {TOP}, {B3} SELECT SUM(DISTINCT b.x) AS n FROM top LEFT JOIN b ON top.k = b.k",
    "distinct_inner": f"WITH {TOP}, {B3} SELECT DISTINCT b.x FROM top JOIN b ON top.k = b.k",
}


def main(out_path, repeats=20):
    spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    report = {}
    for name, sql in QUERIES.items():
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        expected = Counter(tuple(r) for r in spark.sql(sql).collect())
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        seen, plan = Counter(), None
        for _ in range(int(repeats)):
            df = spark.sql(sql)
            got = Counter(tuple(r) for r in df.collect())
            seen["ok" if got == expected else json.dumps(sorted(got.items()))] += 1
            if got != expected and plan is None:
                plan = df._jdf.queryExecution().executedPlan().toString()
        report[name] = {"sql": sql, "expected": sorted(expected.items()), "seen": dict(seen), "plan": plan}
        print(name, "expected", sorted(expected.items()), dict(seen))
    spark.stop()
    with open(out_path, "w", newline="\n") as f:
        json.dump(report, f, indent=1, default=str)


if __name__ == "__main__":
    main(*sys.argv[1:3])
