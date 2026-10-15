"""Real Spark regression probes for three published optimizer correctness bugs.
Experimental research branch only. Logs are captured by GitHub Actions.
"""
import json, tempfile, platform, traceback
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

cases = {
"SPARK-58385": {
 "expected":[[5]], "rule":"org.apache.spark.sql.catalyst.optimizer.LimitPushDown",
 "sql":"SELECT count(*) FROM (SELECT * FROM la CROSS JOIN (SELECT * FROM lb LIMIT 2+3) LIMIT 100)",
 "control":"SELECT count(*) FROM (SELECT * FROM la CROSS JOIN (SELECT * FROM lb LIMIT 5) LIMIT 100)"},
"SPARK-58411": {
 "expected":[], "rule":"org.apache.spark.sql.catalyst.optimizer.RewriteCorrelatedScalarSubquery",
 "sql":"SELECT id, ref FROM q_outer WHERE ref = (SELECT code FROM q_inner WHERE q_outer.low < q_outer.high ORDER BY code ASC NULLS FIRST LIMIT 1) ORDER BY id",
 "control":"SELECT id, ref FROM q_outer WHERE ref = (SELECT code FROM q_inner ORDER BY code ASC NULLS FIRST LIMIT 1) ORDER BY id"},
"SPARK-59438-LIMIT":{
 "expected":[["c",0],["d",0],["f",0]],"rule":"org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughLimitAndOffset",
 "sql":"WITH s AS (SELECT id,name FROM p ORDER BY age DESC LIMIT 3) SELECT name,(SELECT count(*) FROM k WHERE k.a=s.id) AS c FROM s ORDER BY name",
 "control":"SELECT name,(SELECT count(*) FROM k WHERE k.a=p.id) AS c FROM p WHERE id IN (3,4,6) ORDER BY name"},
"SPARK-59438-OFFSET":{
 "expected":[["a",3],["b",1],["e",0]],"rule":"org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughLimitAndOffset",
 "sql":"WITH s AS (SELECT id,name FROM p ORDER BY age DESC OFFSET 3) SELECT name,(SELECT count(*) FROM k WHERE k.a=s.id) AS c FROM s ORDER BY name",
 "control":"SELECT name,(SELECT count(*) FROM k WHERE k.a=p.id) AS c FROM p WHERE id IN (1,2,5) ORDER BY name"}
}
def run(s,query):
 d=s.sql(query);qe=d._jdf.queryExecution()
 optimized=qe.optimizedPlan().toString()
 physical=qe.executedPlan().toString()
 return {"rows":[list(x) for x in d.collect()],"optimized":optimized,"physical":physical}
def main():
 s=(SparkSession.builder.master("local[2]").appName("QueryProof-Spark-Trace")
  .config("spark.ui.enabled","false").config("spark.sql.adaptive.enabled","false")
  .config("spark.sql.shuffle.partitions","5")
  .config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.sql.planChangeLog.level","WARN").getOrCreate())
 s.sparkContext.setLogLevel("WARN")
 result={"spark_version":s.version,"python":platform.python_version(),"cases":{},
         "warning":"Observational examples only, not unbounded correctness proofs. See the log for per-rule changes."}
 try:
  with tempfile.TemporaryDirectory() as td:
   s.createDataFrame([(1,)],"id INT").coalesce(1).write.mode("overwrite").parquet(td+"/la")
   s.range(0,40,1,4).write.mode("overwrite").parquet(td+"/lb")
   s.read.parquet(td+"/la").createOrReplaceTempView("la")
   s.read.parquet(td+"/lb").createOrReplaceTempView("lb")
   s.createDataFrame([(1,1,12,2)],"id INT,low INT,ref INT,high INT").createOrReplaceTempView("q_outer")
   schema=StructType([StructField("code",IntegerType(),True)])
   s.createDataFrame([(12,),(16,),(None,)],schema).createOrReplaceTempView("q_inner")
   s.createDataFrame([(1,"a",30),(2,"b",20),(3,"c",40),(4,"d",60),(5,"e",10),(6,"f",50)],"id INT,name STRING,age INT").repartition(5).createOrReplaceTempView("p")
   s.createDataFrame([(1,2),(1,3),(1,5),(2,3)],"a INT,b INT").createOrReplaceTempView("k")
   for name,c in cases.items():
    rec={"expected":c["expected"],"candidate_rule":c["rule"],"sql":c["sql"],"control_sql":c["control"]}
    result["cases"][name]=rec
    try:
     rec["actual"]=run(s,c["sql"]); rec["control"]=run(s,c["control"])
     rec["matches_expected"]=rec["actual"]["rows"]==c["expected"]
     rec["control_matches_expected"]=rec["control"]["rows"]==c["expected"]
     rec["status"]="MATCHES_EXPECTED" if rec["matches_expected"] else "WRONG_RESULT_OBSERVED"
     print("QUERYPROOF_CASE",name,rec["status"],rec["actual"]["rows"],flush=True)
    except Exception as exc:
     rec["status"]="ERROR";rec["error"]=repr(exc)
     rec["traceback"]=traceback.format_exc()
     print("QUERYPROOF_CASE",name,"ERROR",str(exc),flush=True)
 finally:
  s.stop()
 path=Path("artifacts/queryproof_spark_results.json")
 path.parent.mkdir(parents=True,exist_ok=True)
 path.write_text(json.dumps(result,indent=2,sort_keys=True))
 return 1 if any(r["status"]=="ERROR" for r in result["cases"].values()) else 0
if __name__=="__main__":
 raise SystemExit(main())
