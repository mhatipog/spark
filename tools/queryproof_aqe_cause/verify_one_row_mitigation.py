"""Non-invasive intervention benchmark: test EXACT AQE OptimizeOneRowPlan exclusion.

Uses official, unmodified PySpark releases. SQL candidates and independent
reference semantics were frozen in queryproof_synthesis/candidate_manifest.json
before root-cause attribution. No JVM patch or instrumentation is installed.
"""
import json,sys
from collections import Counter
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"queryproof_synthesis"))
from generate import Case,MANIFEST,sql,diagnose

RULE="org.apache.spark.sql.catalyst.optimizer.OptimizeOneRowPlan"
INLINE="org.apache.spark.sql.catalyst.optimizer.InlineCTE"
REPEAT=2
def main():
    spark=(SparkSession.builder.master("local[2]")
        .appName("QueryProof-AQE-OneRowMitigation-IndependentBenchmark")
        .config("spark.ui.enabled","false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    result={"spark_version":spark.version,"cases":{},
      "intervention":"spark.sql.adaptive.optimizer.excludedRules="+RULE,
      "note":"This only validates a conservative workaround on one historical bug family."}
    try:
        for data in MANIFEST["cases"]:
            cid=data["case_id"]
            c=Case(**{k:v for k,v in data.items() if k not in ("case_id","tier")})
            row={"tier":data["tier"],"query":sql(c),"modes":{}}
            result["cases"][cid]=row
            for mode in ("known_failing_configuration","exclude_aqe_one_row_optimizer"):
                reports=[]
                for attempt in range(REPEAT):
                    ss=spark.newSession()
                    ss.conf.set("spark.sql.adaptive.enabled","true")
                    ss.conf.set("spark.sql.shuffle.partitions","5")
                    ss.conf.set("spark.sql.optimizer.excludedRules",INLINE)
                    if mode=="exclude_aqe_one_row_optimizer":
                        ss.conf.set("spark.sql.adaptive.optimizer.excludedRules",RULE)
                    try:
                        df=ss.sql(sql(c))
                        rows=[list(v) for v in df.collect()]
                        d=diagnose(c,rows)
                        reports.append({"rows":rows,"contract":d})
                    except Exception as ex:
                        reports.append({"error":repr(ex)})
                row["modes"][mode]=reports
                errors=sum("error" in r for r in reports)
                ordering=sum(r.get("contract",{}).get("order_violation",False) for r in reports)
                bagerrors=sum(r.get("contract",{}).get("bag_violation",False) for r in reports)
                print("QUERYPROOF_ONE_ROW_INTERVENTION",spark.version,cid,mode,
                      "wrong_order",ordering,"bag_wrong",bagerrors,"errors",errors,flush=True)
    finally:
        spark.stop()
    result["summary"]={}
    for mode in ("known_failing_configuration","exclude_aqe_one_row_optimizer"):
        vals=[attempt for r in result["cases"].values() for attempt in r["modes"][mode]]
        result["summary"][mode]={
          "attempts":len(vals),
          "errors":sum("error" in x for x in vals),
          "wrong_order":sum(x.get("contract",{}).get("order_violation",False) for x in vals),
          "wrong_bag":sum(x.get("contract",{}).get("bag_violation",False) for x in vals)}
    Path("artifacts").mkdir(exist_ok=True)
    Path(f"artifacts/queryproof_one_row_intervention_{result['spark_version']}.json").write_text(
        json.dumps(result,indent=2))
    print("QUERYPROOF_ONE_ROW_SUMMARY",spark.version,json.dumps(result["summary"]),flush=True)
    if any(x["errors"] for x in result["summary"].values()):return 2
    return 0
if __name__=="__main__":raise SystemExit(main())
