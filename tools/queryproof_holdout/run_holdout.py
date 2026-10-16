"""Preregistered independent holdout suite. Does not import/alter AST heuristic.
Run on published Spark 4.2.0. Expected results from independent JIRA references.
"""
import json,os,sys,tempfile,traceback
from pathlib import Path
from pyspark.sql import SparkSession

ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT/"holdout_manifest.json").read_text())
def norm(v):
    if isinstance(v,tuple):return [norm(x) for x in v]
    if isinstance(v,list):return [norm(x) for x in v]
    if hasattr(v,"asDict"):return [norm(x) for x in v]
    return v
def run_sql(s,sql):
    d=s.sql(sql)
    qe=d._jdf.queryExecution()
    return {"rows":[norm(list(row)) for row in d.collect()],
            "optimized_plan":qe.optimizedPlan().toString(),
            "executed_plan":qe.executedPlan().toString(),
            "schema":d.schema.simpleString()}
def prepare(s,key,root):
    if key=="empty":
        s.sql("CREATE OR REPLACE TEMP VIEW events AS SELECT * FROM VALUES (1,'x','y') AS t(id,c1,c2) WHERE 1 = 0")
    elif key=="duplicates":
        s.sql("CREATE OR REPLACE TEMP VIEW oe AS SELECT * FROM VALUES (1,5,7),(1,5,7),(1,5,7),(1,6,8),(2,9,9) AS t(k,a,b)")
    elif key=="collation":
        dim = root+"/dim"
        fact = root+"/fact"
        s.sql("SELECT CAST('ABC' AS STRING) COLLATE UTF8_LCASE AS key, 'x' AS cat").write.mode("overwrite").parquet(dim)
        s.sql("SELECT concat('abc', CAST(id AS STRING)) COLLATE UTF8_LCASE AS k FROM range(5000)").write.mode("overwrite").parquet(fact)
        s.read.parquet(dim).createOrReplaceTempView("dim")
        s.read.parquet(fact).createOrReplaceTempView("fact")
def main():
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-Preregistered-Holdout")
      .config("spark.ui.enabled","false").config("spark.sql.adaptive.enabled","false")
      .config("spark.sql.shuffle.partitions","5")
      .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    out={"manifest_sha256":__import__("hashlib").sha256((ROOT/"holdout_manifest.json").read_bytes()).hexdigest(),
         "spark_version":spark.version,"observations":{},
         "warning":"Known Jira reproductions. Performance and root cause are NOT verified by results alone."}
    try:
      with tempfile.TemporaryDirectory() as td:
        for c in MANIFEST["cases"]:
          ss=spark.newSession()
          try:
            for k,v in c.get("config",{}).items():ss.conf.set(k,v)
            prepare(ss,c["setup"],td+"/"+c["id"])
            ss._jvm.java.lang.System.setProperty("queryproof.ast.enabled","true")
            ss._jvm.java.lang.System.setProperty("queryproof.query.id",c["id"])
            rec={"issue":c["issue"],"type":c["type"],"expected":c["expected"]}
            out["observations"][c["id"]]=rec
            try:
              rec["candidate"]=run_sql(ss,c["sql"])
              rec["candidate_matches_expected"]=rec["candidate"]["rows"]==c["expected"]
              rec["candidate_status"]="MATCH" if rec["candidate_matches_expected"] else "DIFFERENCE"
            except Exception as ex:
              rec["candidate_status"]="ERROR";rec["candidate_error"]=repr(ex)
              rec["candidate_traceback"]=traceback.format_exc()[-4000:]
            if c.get("control_sql") or c.get("control_config"):
              ctrl=spark.newSession()
              for k,v in c.get("config",{}).items():ctrl.conf.set(k,v)
              for k,v in c.get("control_config",{}).items():ctrl.conf.set(k,v)
              prepare(ctrl,c["setup"],td+"/"+c["id"]+"-ctrl")
              ctrl._jvm.java.lang.System.setProperty("queryproof.query.id",c["id"]+"-control")
              try:
                rec["control"]=run_sql(ctrl,c.get("control_sql",c["sql"]))
                rec["control_matches_expected"]=rec["control"]["rows"]==c.get("control_expected",c["expected"])
              except Exception as ex:
                rec["control_error"]=repr(ex)
            print("QUERYPROOF_HOLDOUT",c["id"],rec["candidate_status"],
                 rec.get("candidate",{}).get("rows"),flush=True)
          except Exception as ex:
            out["observations"][c["id"]]={"candidate_status":"SETUP_ERROR",
              "setup_error":repr(ex),"traceback":traceback.format_exc()[-4000:]}
            print("QUERYPROOF_HOLDOUT",c["id"],"SETUP_ERROR",repr(ex),flush=True)
          finally:
            spark._jvm.java.lang.System.clearProperty("queryproof.query.id")
    finally:
      spark._jvm.java.lang.System.clearProperty("queryproof.ast.enabled")
      spark.stop()
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/queryproof_holdout_observations.json").write_text(json.dumps(out,indent=2,default=str))
    return 0 if any(x.get("candidate_status") in ("MATCH","DIFFERENCE")
                for x in out["observations"].values()) else 2
if __name__=="__main__":sys.exit(main())
