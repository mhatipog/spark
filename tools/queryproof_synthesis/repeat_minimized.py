"""Repeat post-discovery minimized ordering witness on official Spark wheels.

This is independent confirmation, not further minimization or discovery.
Config modes are randomized to reduce run-order confounding.
"""
import json,random,sys,traceback
from collections import Counter
from pathlib import Path
from generate import Case,run_query,reference,sql

ROOT=Path(__file__).resolve().parent
SPEC=json.loads((ROOT/"minimized_witness.json").read_text())
def main():
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-Repeat-Minimal-Order-Bug")
           .config("spark.ui.enabled","false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    c=Case(**SPEC["case"])
    modes=SPEC["modes"];n=SPEC["repetitions_per_mode_per_version"]
    schedule=[mode for mode in modes for _ in range(n)]
    random.Random(20261016).shuffle(schedule)
    result={"spark_version":spark.version,"witness":SPEC["case"],
            "sql":sql(c),"reference_rows":reference(c),"repetitions_per_mode":n,
            "modes":{},"schedule":schedule}
    for name in modes:result["modes"][name]={"runs":[],"counts":{}}
    probe=spark._jvm.org.queryproof.PhysicalOrderingProbe
    try:
        for i,name in enumerate(schedule):
            try:
                row=run_query(spark,c,name,probe)
                result["modes"][name]["runs"].append({
                    "index":i,"rows":row["rows"],"diagnosis":row["diagnosis"],
                    "metadata":row.get("ordering_metadata",{}),
                    "physical_plan":row.get("physical_plan") if i==0 else None})
            except Exception as e:
                result["modes"][name]["runs"].append({
                    "index":i,"error":repr(e),"trace":traceback.format_exc()[-1000:]})
        for name,details in result["modes"].items():
            counts=Counter("ERROR" if "error" in r else
                    "ORDER_VIOLATION" if r["diagnosis"]["order_violation"] else
                    "BAG_ERROR" if r["diagnosis"]["bag_violation"] else
                    "CORRECT_OBSERVED" for r in details["runs"])
            metadata=Counter(r.get("metadata",{}).get("verdict","ERROR") for r in details["runs"])
            details["counts"]=dict(counts)
            details["metadata_verdict_counts"]=dict(metadata)
            print("QUERYPROOF_REPEAT",spark.version,name,dict(counts),
                  "metadata",dict(metadata),flush=True)
    finally:
        spark.stop()
        Path("artifacts").mkdir(exist_ok=True)
        Path(f"artifacts/queryproof_minimized_repeat_{result['spark_version']}.json").write_text(
            json.dumps(result,indent=2,default=str))
    if any(details["counts"].get("ERROR",0) for details in result["modes"].values()):
        return 2
    return 0
if __name__=="__main__":
    raise SystemExit(main())
