"""Experimental ablation: request exclusion of each candidate Catalyst rule.
Separate SparkSession per rule so optimizer settings do not leak across cases.
The setting may be ignored by Spark; this is not proof of root cause.
"""
import json, tempfile, traceback
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType
from queryproof_spark_probe import cases,run

def setup(s,td):
 s.createDataFrame([(1,)],"id INT").coalesce(1).write.mode("overwrite").parquet(td+"/la")
 s.range(0,40,1,4).write.mode("overwrite").parquet(td+"/lb")
 s.read.parquet(td+"/la").createOrReplaceTempView("la")
 s.read.parquet(td+"/lb").createOrReplaceTempView("lb")
 s.createDataFrame([(1,1,12,2)],"id INT,low INT,ref INT,high INT").createOrReplaceTempView("q_outer")
 schema=StructType([StructField("code",IntegerType(),True)])
 s.createDataFrame([(12,),(16,),(None,)],schema).createOrReplaceTempView("q_inner")
 s.createDataFrame([(1,"a",30),(2,"b",20),(3,"c",40),(4,"d",60),(5,"e",10),(6,"f",50)],"id INT,name STRING,age INT").repartition(5).createOrReplaceTempView("p")
 s.createDataFrame([(1,2),(1,3),(1,5),(2,3)],"a INT,b INT").createOrReplaceTempView("k")

def main():
 root=(SparkSession.builder.master("local[2]").appName("QueryProof-Ablation")
  .config("spark.ui.enabled","false").config("spark.sql.adaptive.enabled","false")
  .config("spark.sql.shuffle.partitions","5").config("spark.sql.autoBroadcastJoinThreshold","-1")
  .config("spark.sql.planChangeLog.level","WARN").getOrCreate())
 root.sparkContext.setLogLevel("WARN")
 result={"spark_version":root.version,"cases":{},"caution":"Excluded rules may be mandatory or ignored; not automatic causal proof."}
 try:
  with tempfile.TemporaryDirectory() as td:
   for name,c in cases.items():
    session=root.newSession()
    session.conf.set("spark.sql.optimizer.excludedRules",c["rule"])
    rec={"excluded_rule_requested":c["rule"],"expected":c["expected"]}
    result["cases"][name]=rec
    try:
     setup(session,td+"/"+name)
     actual=run(session,c["sql"]);rec["actual"]=actual
     rec["matches_expected"]=actual["rows"]==c["expected"]
     rec["status"]="MATCHES_EXPECTED" if rec["matches_expected"] else "WRONG_RESULT_OBSERVED"
     print("QUERYPROOF_ABLATION",name,rec["status"],actual["rows"],flush=True)
    except Exception as ex:
     rec["status"]="ERROR";rec["error"]=repr(ex);rec["traceback"]=traceback.format_exc()
     print("QUERYPROOF_ABLATION",name,"ERROR",repr(ex),flush=True)
 finally: root.stop()
 out=Path("artifacts/queryproof_ablation.json");out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(result,indent=2))
 return 1 if any(r["status"]=="ERROR" for r in result["cases"].values()) else 0
if __name__=="__main__":raise SystemExit(main())
