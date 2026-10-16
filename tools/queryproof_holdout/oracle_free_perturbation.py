"""Oracle-free optimizer perturbation: compare results under candidate rule exclusions.

Discovery never reads expected answers, issue IDs or known culprit names.
This is a SQL-result disagreement detector, NOT a correctness prover.
Rows are compared as multisets; tests must have deterministic output values.
"""
import json,re,sys,tempfile
from collections import Counter
from pathlib import Path
from pyspark.sql import SparkSession
from run_holdout import MANIFEST,prepare,run_sql
from exhaustive_ablation import trace_candidate_rules,norm_plan

def canonical(rows):
    return Counter(json.dumps(r,sort_keys=True,default=str,separators=(',',':')) for r in rows)

def detect():
    obs=json.loads(Path("artifacts/queryproof_holdout_observations.json").read_text())
    rules_by_case=trace_candidate_rules("artifacts/queryproof-holdout-ast.log")
    spark=(SparkSession.builder.master("local[2]")
       .appName("QueryProof-Oracle-Free-Perturbation")
       .config("spark.ui.enabled","false")
       .config("spark.sql.adaptive.enabled","false")
       .config("spark.sql.shuffle.partitions","5").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    records={"version":1,"spark_version":spark.version,
             "method":"blind observed-rule SQL result disagreement",
             "important":"Disagreement does not establish which outcome is correct.",
             "discovery":{}}
    try:
      with tempfile.TemporaryDirectory() as td:
        for case in MANIFEST["cases"]:
          cid=case["id"];old=obs["observations"].get(cid,{})
          result={"baseline_rows":old.get("candidate",{}).get("rows"),
                  "attempts":[],"disagreeing_rules":[],"status":"NO_DISAGREEMENT"}
          records["discovery"][cid]=result
          if old.get("candidate_status") in ("ERROR","SETUP_ERROR") or not old.get("candidate"):
            result["status"]="BASELINE_UNAVAILABLE"
            continue
          before=old["candidate"]["rows"]
          before_plan=old["candidate"]["optimized_plan"]
          for idx,rule in enumerate(rules_by_case.get(cid,{})):
            ss=spark.newSession()
            for k,v in case.get("config",{}).items():ss.conf.set(k,v)
            ss.conf.set("spark.sql.optimizer.excludedRules",rule)
            attempt={"rule":rule,"status":"NOT_RUN"}
            try:
              prepare(ss,case["setup"],f"{td}/{cid}/{idx}")
              after=run_sql(ss,case["sql"])
              changed=(norm_plan(before_plan)!=norm_plan(after["optimized_plan"]))
              difference=canonical(before)!=canonical(after["rows"])
              attempt.update({"status":("DIFFERENT_RESULT" if difference and changed else
                                        "SAME_PLAN_RESULT_UNSTABLE" if difference else
                                        "SAME_RESULT"),
                              "plan_changed":changed,"changed_result":difference,
                              "rows":after["rows"]})
              if difference and changed:
                result["disagreeing_rules"].append(rule)
                result["status"]="RESULT_DISAGREEMENT_DETECTED"
            except Exception as ex:
              attempt.update({"status":"INCONCLUSIVE_ERROR","error":repr(ex)})
            result["attempts"].append(attempt)
          print("QUERYPROOF_ORACLE_FREE",cid,result["status"],
                [r.split('.')[-1] for r in result["disagreeing_rules"]],flush=True)
    finally:
      spark.stop()
    Path("artifacts/queryproof_oracle_free_discovery.json").write_text(json.dumps(records,indent=2))
    return records

def score_after_discovery(records):
    # Completely separate research scoring; this cannot affect candidate generation.
    out={"score_version":1,"cases":{}}
    for c in MANIFEST["cases"]:
      id=c["id"]
      actual=records["discovery"][id]
      verified_bug=(c["type"]=="bug" and actual["baseline_rows"] is not None and
                    canonical(actual["baseline_rows"]) != canonical(c["expected"]))
      expected_optimization=c["type"] in ("bug","negative_control","fixed_negative_control")
      rule=c.get("ground_truth_rule")
      out["cases"][id]={"case_type":c["type"],"issue":c["issue"],
        "verified_optimizer_bug":verified_bug,
        "disagreement":actual["status"]=="RESULT_DISAGREEMENT_DETECTED",
        "known_culprit_matched":bool(rule and rule in actual["disagreeing_rules"]),
        "flags":len(actual["disagreeing_rules"]),
        "attempts":len(actual["attempts"])}
    Path("artifacts/queryproof_oracle_free_score.json").write_text(json.dumps(out,indent=2))

if __name__=="__main__":
    report=detect()
    score_after_discovery(report)
