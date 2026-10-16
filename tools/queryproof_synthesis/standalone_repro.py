"""Standalone SPARK-59126 minimal generated counterexample.

Only dependency: official pyspark==4.0.1 or 4.2.0.
No QueryProof instrumentation, patches, data files or third-party libraries.
The SQL was automatically reduced by the QueryProof research generator.
There are 3 rows on the left, 1 on the right, 2 returned rows.
"""
from __future__ import annotations
import json
from collections import Counter
from pyspark.sql import SparkSession

SQL = """
WITH top AS (
  SELECT id, CAST(id % 1 AS INT) AS k
  FROM range(0,3,1,1)
  ORDER BY id ASC LIMIT 2 OFFSET 1
), rhs AS (
  SELECT id AS bid, CAST(id % 1 AS INT) AS k
  FROM range(0,1,1,1)
)
SELECT top.id,top.k,rhs.bid
FROM top LEFT JOIN rhs USING(k)
ORDER BY top.id ASC,rhs.bid ASC
"""
EXPECTED=[[1,0,0],[2,0,0]]
MODES={
    "candidate": {"spark.sql.adaptive.enabled":"true",
                  "spark.sql.optimizer.excludedRules":
                    "org.apache.spark.sql.catalyst.optimizer.InlineCTE"},
    "normal": {"spark.sql.adaptive.enabled":"true"},
    "aqe_off": {"spark.sql.adaptive.enabled":"false",
                "spark.sql.optimizer.excludedRules":
                  "org.apache.spark.sql.catalyst.optimizer.InlineCTE"},
    "no_broadcast": {"spark.sql.adaptive.enabled":"true",
                      "spark.sql.autoBroadcastJoinThreshold":"-1",
                      "spark.sql.adaptive.autoBroadcastJoinThreshold":"-1",
                      "spark.sql.optimizer.excludedRules":
                        "org.apache.spark.sql.catalyst.optimizer.InlineCTE"},
}

def main():
    spark=(SparkSession.builder.master("local[2]").appName("SPARK-59126-Generated-Reproducer")
           .config("spark.ui.enabled","false")
           .config("spark.sql.shuffle.partitions","5").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    report={"spark":spark.version,"sql":SQL.strip(),"expected":EXPECTED,"runs":{}}
    try:
        for mode,conf in MODES.items():
            seen=[];plans=[]
            for i in range(8):
                ss=spark.newSession()
                for name,value in conf.items():ss.conf.set(name,value)
                df=ss.sql(SQL)
                result=[list(r) for r in df.collect()]
                plan=df._jdf.queryExecution().executedPlan().toString()
                seen.append({"iteration":i,"actual":result,
                             "ordered_correctly":result==EXPECTED,
                             "same_rows_as_expected":Counter(map(tuple,result))==
                                                      Counter(map(tuple,EXPECTED))})
                plans.append(plan)
            report["runs"][mode]={"results":seen,"sample_plan":plans[0]}
            print("SPARK_MINIMAL_REPRO",spark.version,mode,
                  "wrong_order",sum(not x["ordered_correctly"] for x in seen),
                  "same_row_bag",sum(x["same_rows_as_expected"] for x in seen),
                  "sample",seen[0]["actual"],flush=True)
    finally:
        spark.stop()
    print("SPARK_MINIMAL_REPRO_SUMMARY",json.dumps({
        "spark":report["spark"],
        "wrong_orders":{mode:sum(not x["ordered_correctly"] for x in val["results"])
                      for mode,val in report["runs"].items()},
        "expected":EXPECTED},sort_keys=True),flush=True)
    return 0

if __name__=="__main__":
    raise SystemExit(main())
