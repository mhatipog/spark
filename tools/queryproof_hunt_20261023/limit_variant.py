"""Minimized default-configuration variants of hunt case D033_01115 (root LIMIT ignored).

Usage: python limit_variant.py OUT.json [REPEATS]
Shape: a derived table with ORDER BY ... LIMIT m OFFSET o, LEFT JOIN a table that has
several matches per key, then a root ORDER BY ... LIMIT n. Tables are DataFrame
temp views (LogicalRDD leaves), as in the hunt, so they are not folded into local
relations. Expected results come from the same query with AQE disabled.
"""
import json
import sys
from collections import Counter

from pyspark.sql import SparkSession

VARIANTS = {
    # the hunt case with its predicates and expressions stripped
    "offset_left_join_order_limit": (
        "SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l "
        "LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2"),
    "offset_left_join_limit": (
        "SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l "
        "LEFT JOIN r ON l.a = r.k LIMIT 2"),
    "offset_only_left_join_limit": (
        "SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a OFFSET 4) l "
        "LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2"),
    "limit_only_left_join_limit": (
        "SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 1) l "
        "LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2"),
    "offset_inner_join_limit": (
        "SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l "
        "JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2"),
    "offset_left_join_count": (
        "SELECT COUNT(*) AS n FROM (SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l "
        "LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2) q"),
    "offset_left_join_distinct": (
        "SELECT DISTINCT l.a FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l "
        "LEFT JOIN r ON l.a = r.k"),
}


def main(out_path, repeats=10):
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    # l: keys 1..5 (one row each); after ORDER BY a LIMIT 3 OFFSET 2 only a = 3 remains.
    spark.createDataFrame([(i,) for i in range(1, 6)], "a int").createOrReplaceTempView("l")
    # r: key 3 has three matches.
    spark.createDataFrame([(3, "x"), (3, "y"), (3, "z"), (4, "w")], "k int, s string").createOrReplaceTempView("r")
    report = {}
    for name, sql in VARIANTS.items():
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
