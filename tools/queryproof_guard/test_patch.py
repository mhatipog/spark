"""Test one narrow logical AQE guard patch in a disposable installed Spark wheel.

All correctness expectations calculated independently by generate.py; not a
soundness proof. Also run negative-controls for two-row and single-row SQL.
"""
import json,sys
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"queryproof_synthesis"))
from generate import MANIFEST,Case,sql,diagnose

def main():
    spark=(SparkSession.builder.master("local[2]")
        .appName("QueryProof-Experimental-AQE-OneRow-Guard")
        .config("spark.ui.enabled","false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    result={"spark_version":spark.version,"patched":True,"cases":{},
      "source_guard":"Skip Sort elimination on maxRows<=1 if child contains LogicalQueryStage",
      "negative_controls":{}}
    try:
        for case in MANIFEST["cases"]:
            cid=case["case_id"]
            c=Case(**{k:v for k,v in case.items() if k not in ("case_id","tier")})
            trials=[]
            for attempt in range(2):
                ss=spark.newSession()
                ss.conf.set("spark.sql.adaptive.enabled","true")
                ss.conf.set("spark.sql.shuffle.partitions","5")
                ss.conf.set("spark.sql.optimizer.excludedRules",
                    "org.apache.spark.sql.catalyst.optimizer.InlineCTE")
                # Important: do NOT exclude OptimizeOneRowPlan. This run tests the patch itself.
                assert not ss.conf.get("spark.sql.adaptive.optimizer.excludedRules","")
                try:
                    q=ss.sql(sql(c))
                    rows=[list(r) for r in q.collect()]
                    trial={"rows":rows,"diagnosis":diagnose(c,rows)}
                    if cid=="G000" and attempt==0:
                        trial["optimized_plan"]=q._jdf.queryExecution().optimizedPlan().toString()
                        trial["final_physical_plan"]=q._jdf.queryExecution().executedPlan().toString()
                except Exception as exc:
                    trial={"error":repr(exc)}
                trials.append(trial)
            result["cases"][cid]={"tier":case["tier"],"trials":trials}
        controls={
            "normal":["SELECT id FROM range(1) ORDER BY id",
                      "SELECT id FROM range(5) ORDER BY id DESC"],
            "scalar":["SELECT (SELECT 1) AS a"],
        }
        for typ,queries in controls.items():
            control=[]
            for sqlstr in queries:
                ss=spark.newSession()
                ss.conf.set("spark.sql.adaptive.enabled","true")
                try:
                    q=ss.sql(sqlstr)
                    rows=[list(r) for r in q.collect()]
                    control.append({"sql":sqlstr,"rows":rows,
                        "optimized_plan":q._jdf.queryExecution().optimizedPlan().toString()})
                except Exception as exc:
                    control.append({"sql":sqlstr,"error":repr(exc)})
            result["negative_controls"][typ]=control
    finally:
        spark.stop()
    all_trials=[t for c in result["cases"].values() for t in c["trials"]]
    summary={"attempts":len(all_trials),
        "errors":sum("error" in t for t in all_trials),
        "order_violations":sum(t.get("diagnosis",{}).get("order_violation",False)
                               for t in all_trials),
        "bag_violations":sum(t.get("diagnosis",{}).get("bag_violation",False)
                             for t in all_trials)}
    result["summary"]=summary
    Path("artifacts").mkdir(exist_ok=True)
    Path(f"artifacts/queryproof_experimental_guard_{result['spark_version']}.json").write_text(
      json.dumps(result,indent=2))
    print("QUERYPROOF_PATCH_GUARD_SUMMARY",spark.version,json.dumps(summary),flush=True)
    if summary["errors"] or summary["order_violations"] or summary["bag_violations"]:
        return 2
    return 0
if __name__=="__main__":raise SystemExit(main())
