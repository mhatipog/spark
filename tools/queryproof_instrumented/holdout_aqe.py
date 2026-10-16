"""Frozen-method holdout: SPARK-57956 AQE root LIMIT correctness.

This case is NOT one of the four examples used to design the AST risk features.
AQE may be outside the logical RuleExecutor capture scope; report that honestly.
"""
import json,traceback
from pathlib import Path
from pyspark.sql import SparkSession

QUERY = """
WITH top AS (
 SELECT id % 1 AS k
 FROM range(0, 2, 1, 1)
 OFFSET 1
), b AS (
 SELECT * FROM VALUES (0), (0) AS b(k)
)
SELECT 1 AS c
FROM top LEFT JOIN b USING (k)
LIMIT 1
"""
def main():
    s = (SparkSession.builder.master("local[2]")
         .appName("QueryProof-heldout-AQE")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.autoBroadcastJoinThreshold","-1")
         .config("spark.sql.adaptive.autoBroadcastJoinThreshold","1048576")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    jvm=s._jvm.java.lang.System
    jvm.setProperty("queryproof.ast.enabled","true")
    jvm.setProperty("queryproof.query.id","SPARK-57956-HOLDOUT")
    output={"issue":"SPARK-57956","version":s.version,"expected_cardinality":1,
            "query":QUERY,"observations":[],
            "note":"Independent historical AQE bug, outside four development examples."}
    try:
        for aqe in (True, False):
            s.conf.set("spark.sql.adaptive.enabled",str(aqe).lower())
            for repeat in range(3 if aqe else 1):
                record={"aqe":aqe,"repeat":repeat}
                try:
                    df=s.sql(QUERY)
                    record["optimized_plan"]=df._jdf.queryExecution().optimizedPlan().toString()
                    records=[list(x) for x in df.collect()]
                    record["rows"]=records
                    record["cardinality"]=len(records)
                    record["within_limit"]=len(records)<=1
                    record["physical_plan"]=df._jdf.queryExecution().executedPlan().toString()
                    record["status"]="MATCHES_CARDINALITY" if len(records)<=1 else "LIMIT_VIOLATION"
                except Exception as ex:
                    record["status"]="INCONCLUSIVE_ERROR"
                    record["exception"]=repr(ex)
                    record["traceback"]=traceback.format_exc()[:9000]
                output["observations"].append(record)
                print("QUERYPROOF_HOLDOUT",aqe,repeat,record["status"],
                      record.get("cardinality"),flush=True)
    finally:
        s.stop()
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/aqe-holdout.json").write_text(json.dumps(output,indent=2))
if __name__=="__main__":
    main()
