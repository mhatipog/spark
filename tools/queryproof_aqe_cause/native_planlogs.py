"""Use Spark's OWN physical PlanChangeLogger to identify a vanished global Sort.

No JVM patch. Reports every trial and does not assume RemoveRedundantSorts is causal.
"""
import json,traceback
from pathlib import Path
from pyspark.sql import SparkSession
SQL="""WITH top AS (
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
OPTIONS={
"candidate":{"spark.sql.adaptive.enabled":"true","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE"},
"remove_sort_rule_off":{"spark.sql.adaptive.enabled":"true","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE","spark.sql.execution.removeRedundantSorts":"false"},
"normal":{"spark.sql.adaptive.enabled":"true"},
"aqe_off":{"spark.sql.adaptive.enabled":"false","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE"}
}
EXPECTED=[[1,0,0],[2,0,0]]
def main():
 s=(SparkSession.builder.master("local[2]").appName("QueryProof-AQE-Rule-Attribution")
    .config("spark.ui.enabled","false")
    .config("spark.sql.planChangeLog.level","WARN")
    .config("spark.sql.shuffle.partitions","5")
    .getOrCreate())
 s.sparkContext.setLogLevel("WARN")
 d={"spark":s.version,"cases":{}}
 try:
  for name,settings in OPTIONS.items():
   runs=[]
   for i in range(6):
    ss=s.newSession()
    for k,v in settings.items():ss.conf.set(k,v)
    try:
     q=ss.sql(SQL);qe=q._jdf.queryExecution()
     initial=qe.executedPlan().toString()
     rows=[list(x) for x in q.collect()]
     final=qe.executedPlan().toString()
     runs.append({"correct":rows==EXPECTED,"actual_rows":rows,
                  "plan_before":initial,"plan_after":final})
    except Exception as exc:
     runs.append({"error":repr(exc),"trace":traceback.format_exc()[-2800:]})
    print("QUERYPROOF_LOSS",s.version,name,i,
          "ERROR" if "error" in runs[-1] else "WRONG" if not runs[-1]["correct"] else "CORRECT",
          flush=True)
   d["cases"][name]={"config":settings,"runs":runs}
 finally:
  s.stop()
  Path("artifacts").mkdir(exist_ok=True)
  Path("artifacts/aqe_planlog_observations.json").write_text(json.dumps(d,indent=2))
 return 0
if __name__=="__main__":raise SystemExit(main())
