"""Independent 2x3 repeatability study on *unmodified* PySpark wheels.

No RuleExecutor modifications are used. This tests whether the surprising
InlineCTE x AQE output-order interaction is reproducible and version-dependent.
"""
import json,sys,traceback
from collections import Counter
from pathlib import Path
from pyspark.sql import SparkSession

SQL="""WITH top AS (
SELECT id,CAST(id % 1 AS INT) AS k FROM range(0,3,1,2) ORDER BY id ASC LIMIT 1 OFFSET 1
),b AS (
SELECT id AS bid, CAST(id % 1 AS INT) AS k FROM range(0,2,1,2)
)
SELECT top.id,top.k,b.bid FROM top LEFT JOIN b USING(k)
ORDER BY top.id ASC,b.bid ASC"""
EXPECTED=[[1,0,0],[1,0,1]]
SETTINGS=[
 ("aqe_on_normal",{"spark.sql.adaptive.enabled":"true"}),
 ("aqe_on_without_inline_cte",{"spark.sql.adaptive.enabled":"true","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE"}),
 ("aqe_off_normal",{"spark.sql.adaptive.enabled":"false"}),
 ("aqe_off_without_inline_cte",{"spark.sql.adaptive.enabled":"false","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE"}),
 ("aqe_on_without_inline_cte_no_broadcast",{"spark.sql.adaptive.enabled":"true","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1"}),
 ("aqe_on_normal_no_broadcast",{"spark.sql.adaptive.enabled":"true","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1"}),
 ("aqe_on_without_inline_cte_all_broadcast_disabled",{"spark.sql.adaptive.enabled":"true","spark.sql.optimizer.excludedRules":"org.apache.spark.sql.catalyst.optimizer.InlineCTE","spark.sql.autoBroadcastJoinThreshold":"-1","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1"}),
 ("aqe_on_normal_all_broadcast_disabled",{"spark.sql.adaptive.enabled":"true","spark.sql.autoBroadcastJoinThreshold":"-1","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1"}),
]
def main():
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-OfficialWheel-Interaction-Repro")
        .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","5").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    result={"spark_version":spark.version,"official_release_unmodified":True,
            "expected_order":EXPECTED,"cases":{},"repetitions":8}
    try:
        for name,settings in SETTINGS:
            runs=[]
            for i in range(8):
                ss=spark.newSession()
                for key,value in settings.items():ss.conf.set(key,value)
                try:
                    df=ss.sql(SQL);q=df._jdf.queryExecution()
                    logical=q.optimizedPlan().toString()
                    actual=[list(row) for row in df.collect()]
                    physical=q.executedPlan().toString()
                    runs.append({"index":i,"rows":actual,"correct_order":actual==EXPECTED,
                                 "optimized_plan":logical,"physical_plan":physical})
                except Exception as ex:
                    runs.append({"index":i,"error":repr(ex),"traceback":traceback.format_exc()[-2500:]})
            counts=dict(Counter("error" if "error" in r else
                                "correct" if r["correct_order"] else "unexpected_order" for r in runs))
            result["cases"][name]={"settings":settings,"runs":runs,"counts":counts}
            print("QUERYPROOF_OFFICIAL_REPRO",spark.version,name,counts,
                  [x.get("rows") for x in runs[:2]],flush=True)
    finally:
        spark.stop()
    Path("artifacts").mkdir(exist_ok=True)
    Path(f"artifacts/official_spark_order_interaction_{result['spark_version']}.json").write_text(json.dumps(result,indent=2))
    return 0
if __name__=="__main__":
    raise SystemExit(main())
