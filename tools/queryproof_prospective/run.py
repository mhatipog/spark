"""Prospective Spark SQL benchmark runner. Expected answers are never read here.

The preregistered manifest is read only for setup, SQL, and comparison *mode*;
scoring correctness is deferred to score.py. Runs multiple released Spark versions.
No network and no production data. Note: temporary Parquet fixtures are local.
"""
import hashlib,json,os,platform,tempfile,traceback
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType

ROOT=Path(__file__).resolve().parent
MANIFEST_PATH=ROOT/"manifest.json"
MANIFEST=json.loads(MANIFEST_PATH.read_text())
OUT=Path("artifacts")
OUT.mkdir(exist_ok=True)

def enc(x):
    if hasattr(x,"asDict"):
        return [enc(y) for y in x]
    if isinstance(x,(list,tuple)):
        return [enc(y) for y in x]
    if isinstance(x,dict):
        return {str(k):enc(v) for k,v in x.items()}
    if isinstance(x,(int,float,str,bool)) or x is None:
        return x
    return str(x)

def fixture(s,key,root):
    if key=="none":return
    if key=="nullable_joins":
        tl=root+"/tl";tr=root+"/tr"
        s.createDataFrame([(0,10),(None,11)],"a INT,b INT").write.mode("overwrite").parquet(tl)
        s.createDataFrame([(None,20),(0,21),(1,22)],"x INT,y INT").write.mode("overwrite").parquet(tr)
        s.read.parquet(tl).createOrReplaceTempView("tl")
        s.read.parquet(tr).createOrReplaceTempView("tr")
    elif key=="in_null":
        t=root+"/t";tn=root+"/tn"
        s.createDataFrame([(1,),(2,)],"c INT").write.mode("overwrite").parquet(t)
        s.createDataFrame([(1,),(2,),(None,)],"c INT").write.mode("overwrite").parquet(tn)
        s.read.parquet(t).createOrReplaceTempView("t")
        s.read.parquet(tn).createOrReplaceTempView("tn")
    else:
        raise ValueError("unknown fixture "+key)

def run_once(s,sql,ncols=None):
    q=s.sql(sql)
    logical=q._jdf.queryExecution().optimizedPlan().toString()
    initial=q._jdf.queryExecution().executedPlan().toString()
    rows=[enc(list(x)[:ncols] if ncols else list(x)) for x in q.collect()]
    final=q._jdf.queryExecution().executedPlan().toString()
    return {"rows":rows,"logical_plan":logical,
            "physical_initial":initial,"physical_final":final,
            "schema":q.schema.simpleString()}

def configured_session(spark,c,control=False):
    s=spark.newSession()
    cfg=dict(c.get("settings",{}))
    if control:cfg.update(c.get("control_settings",{}))
    for k,v in cfg.items():s.conf.set(k,str(v).lower() if isinstance(v,bool) else str(v))
    return s,cfg

def main():
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-Prospective-Blind-Spark")
        .config("spark.ui.enabled","false")
        .config("spark.sql.adaptive.enabled","false")
        .config("spark.sql.shuffle.partitions","5")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    result={"schema_version":1,"spark_version":spark.version,
       "manifest_sha256":hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest(),
       "tool_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
       "python":platform.python_version(),"cases":{},
       "study":"prospective-blind-v2",
       "notes":"Raw Spark observations. The expected answers were never read by this runner."}
    try:
      with tempfile.TemporaryDirectory(prefix="queryproof_holdout_") as td:
        for c in MANIFEST["cases"]:
            cid=c["id"]
            evidence={"issue":c["issue"],"category":c["category"],"comparison":c["comparison"],
                      "observed_columns":c.get("observed_columns"),
                      "observations":[]}
            result["cases"][cid]=evidence
            try:
                s,cfg=configured_session(spark,c)
                fixture(s,c["fixture"],td+"/"+cid+"-baseline")
                evidence["settings"]=cfg
                s._jvm.java.lang.System.setProperty("queryproof.ast.enabled","true")
                s._jvm.java.lang.System.setProperty("queryproof.query.id",cid)
                try:
                    for i in range(3 if c["comparison"]=="sequence" else 2):
                        evidence["observations"].append(run_once(s,c["sql"],c.get("observed_columns")))
                finally:
                    s._jvm.java.lang.System.clearProperty("queryproof.query.id")
                if "control_sql" in c or "control_settings" in c:
                    cs,control_cfg=configured_session(spark,c,control=True)
                    fixture(cs,c["fixture"],td+"/"+cid+"-control")
                    evidence["control_settings"]=control_cfg
                    cs._jvm.java.lang.System.setProperty("queryproof.query.id",cid+"-control")
                    try:
                        evidence["control"]=run_once(cs,c.get("control_sql",c["sql"]),c.get("observed_columns"))
                    finally:
                        cs._jvm.java.lang.System.clearProperty("queryproof.query.id")
                evidence["status"]="OBSERVED"
            except Exception as e:
                evidence["status"]="ERROR"
                evidence["error"]=repr(e)
                evidence["traceback"]=traceback.format_exc()[-8000:]
            print("QUERYPROOF_PROSPECTIVE",spark.version,cid,evidence["status"],
                  [r["rows"] for r in evidence["observations"]],
                  evidence.get("control",{}).get("rows"),flush=True)
    finally:
        spark._jvm.java.lang.System.clearProperty("queryproof.ast.enabled")
        spark.stop()
    path=OUT/("prospective_observations_"+result["spark_version"]+".json")
    path.write_text(json.dumps(result,indent=2,sort_keys=True))
    return 0 if any(x["status"]=="OBSERVED" for x in result["cases"].values()) else 2

if __name__=="__main__":
    raise SystemExit(main())
