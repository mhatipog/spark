"""Exploratory paired performance test of minimal stage-aware guard vs disabling rule.

Runs on the SAME patched official PySpark/JVM runtime. It makes no claims about
production latency or overhead confidence intervals: small local synthetic data,
five timed repeats, two warm-ups and shuffled ordering.
"""
from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path
from statistics import median
from time import perf_counter
import json,random,sys
from pyspark.sql import SparkSession
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"queryproof_synthesis"))
from generate import Case,sql,diagnose

small=Case(shape="cte",join="LEFT",left_start=0,right_start=0,
  left_n=3,right_n=1,left_partitions=1,right_partitions=1,
  modulus=1,offset=1,limit=2)
single=replace(small,limit=1)
large=Case(shape="cte",join="LEFT",left_start=0,right_start=0,
  left_n=5000,right_n=2,left_partitions=4,right_partitions=2,
  modulus=1,offset=1000,limit=1000)
CASES={"single_true_row":single,"previously_buggy_two_rows":small,
       "medium_multirun_ordering":large}
INLINE="org.apache.spark.sql.catalyst.optimizer.InlineCTE"
ONE="org.apache.spark.sql.catalyst.optimizer.OptimizeOneRowPlan"
MODES={
  "surgical_guard_enabled":{"spark.sql.optimizer.excludedRules":INLINE},
  "disable_all_aqe_one_row_optimizer":{
     "spark.sql.optimizer.excludedRules":INLINE,
     "spark.sql.adaptive.optimizer.excludedRules":ONE}
}
WARMUP=2
REPS=5

def run_one(spark,case,mode):
    ss=spark.newSession()
    ss.conf.set("spark.sql.adaptive.enabled","true")
    ss.conf.set("spark.sql.shuffle.partitions","5")
    for name,value in MODES[mode].items():ss.conf.set(name,value)
    start=perf_counter()
    # The correctness generator deliberately caps n at 40.
    # The exploratory cost probe accepts larger synthetic ranges without
    # changing the preregistered generator's validity rules.
    if case.valid():
        statement=sql(case)
    else:
        statement=(
            f"WITH top AS (SELECT id, CAST(id % {case.modulus} AS INT) AS k "
            f"FROM range({case.left_start},{case.left_start+case.left_n},1,{case.left_partitions}) "
            f"ORDER BY id ASC LIMIT {case.limit} OFFSET {case.offset}), "
            f"rhs AS (SELECT id AS bid, CAST(id % {case.modulus} AS INT) AS k "
            f"FROM range({case.right_start},{case.right_start+case.right_n},1,{case.right_partitions})) "
            f"SELECT top.id,top.k,rhs.bid FROM top {case.join} JOIN rhs USING(k) "
            "ORDER BY top.id ASC,rhs.bid ASC")
    q=ss.sql(statement)
    rows=[list(x) for x in q.collect()]
    elapsed=(perf_counter()-start)*1000
    result=diagnose(case,rows)
    return {"ms":round(elapsed,2),"rows":len(rows),
       "order_violation":result["order_violation"],
       "bag_violation":result["bag_violation"]}

def main():
    spark=(SparkSession.builder.master("local[2]")
        .appName("QueryProof-Guard-Exploratory-AQECost")
        .config("spark.ui.enabled","false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    out={"version":spark.version,"modes":MODES,"warmup":WARMUP,
        "repetitions":REPS,"cases":{},"note":
        "Exploratory single-runner median runtime. Not a production overhead estimate."}
    try:
      for name,c in CASES.items():
        result={mode:[] for mode in MODES}
        for mode in MODES:
          for _ in range(WARMUP):run_one(spark,c,mode)
        order=list(MODES)*REPS
        random.Random(20261016).shuffle(order)
        for mode in order:result[mode].append(run_one(spark,c,mode))
        score={}
        for mode,runs in result.items():
          score[mode]={"median_ms":round(median(x["ms"] for x in runs),2),
                       "failures":sum(x["order_violation"] or x["bag_violation"]
                                       for x in runs),
                       "row_counts":sorted(set(x["rows"] for x in runs))}
        out["cases"][name]={"input":asdict(c),"runs":result,"summary":score}
        print("QUERYPROOF_GUARD_PERFORMANCE",spark.version,name,
            json.dumps(score),flush=True)
    finally:
      spark.stop()
      Path("artifacts").mkdir(exist_ok=True)
      Path("artifacts/queryproof_guard_exploratory_cost.json").write_text(
        json.dumps(out,indent=2))
    return 2 if any(any(x["failures"] for x in c["summary"].values())
        for c in out["cases"].values()) else 0
if __name__=="__main__":raise SystemExit(main())
