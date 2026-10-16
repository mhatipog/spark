"""Cardinality diagnostic experiment: official Spark wheel, disposable patched optimizer class."""
import json,traceback
from pathlib import Path
from pyspark.sql import SparkSession
from native_planlogs import SQL,EXPECTED,OPTIONS
def main():
    spark=(SparkSession.builder.master("local[2]")
      .appName("QueryProof-Catalyst-MaxRows-Cardinality-Diagnostic")
      .config("spark.ui.enabled","false")
      .config("spark.sql.shuffle.partitions","5")
      .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    jvm=spark._jvm.java.lang.System
    jvm.setProperty("queryproof.cardinality.enabled","true")
    results={"version":spark.version,"cases":{}}
    modes=["candidate","aqe_optimize_one_row_off","normal"]
    try:
      for mode in modes:
        ss=spark.newSession()
        for k,v in OPTIONS[mode].items():ss.conf.set(k,v)
        jvm.setProperty("queryproof.cardinality.case",mode)
        try:
          q=ss.sql(SQL); rows=[list(r) for r in q.collect()]
          result={"rows":rows,"correct":rows==EXPECTED,
                  "optimized_plan":q._jdf.queryExecution().optimizedPlan().toString(),
                  "final_plan":q._jdf.queryExecution().executedPlan().toString()}
        except Exception as e:
          result={"error":repr(e),"traceback":traceback.format_exc()[-3000:]}
        results["cases"][mode]=result
        print("QUERYPROOF_CARDINALITY_CASE",mode,
          "CORRECT" if result.get("correct") else "WRONG_OR_ERROR",result.get("rows"),flush=True)
    finally:
      jvm.clearProperty("queryproof.cardinality.enabled")
      jvm.clearProperty("queryproof.cardinality.case")
      spark.stop()
      Path("artifacts").mkdir(exist_ok=True)
      Path("artifacts/cardinality_observations.json").write_text(json.dumps(results,indent=2))
    return 0
if __name__=="__main__":raise SystemExit(main())
