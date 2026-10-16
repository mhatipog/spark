"""Exhaustive *observed-rule* ablation baseline over frozen holdout.
No Jira issue IDs, expected rule names, or structural risk features select candidates.
Only rules actually changing the recorded query's logical AST are considered.
"""
import base64, json, re, sys, tempfile, time
from collections import OrderedDict
from pathlib import Path
from pyspark.sql import SparkSession
from run_holdout import MANIFEST, prepare, run_sql

def trace_candidate_rules(path):
    rules={}
    for line in Path(path).read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):continue
        parts=line.split("\t")
        if len(parts)!=8:raise ValueError("malformed AST event")
        case=base64.b64decode(parts[1],validate=True).decode("utf8")
        rule=base64.b64decode(parts[4],validate=True).decode("utf8")
        if ".optimizer." not in rule:continue
        if case not in rules:rules[case]=OrderedDict()
        rules[case][rule]=rules[case].get(rule,0)+1
    return rules

def norm_plan(text):
    return re.sub(r"#\d+L?", "#ID",text)

def main():
    observations=json.loads(Path("artifacts/queryproof_holdout_observations.json").read_text())
    by_case=trace_candidate_rules("artifacts/queryproof-holdout-ast.log")
    s=(SparkSession.builder.master("local[2]")
       .appName("QueryProof-Holdout-Observed-Rule-Ablation")
       .config("spark.ui.enabled","false")
       .config("spark.sql.adaptive.enabled","false")
       .config("spark.sql.shuffle.partitions","5").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    output={"version":1,"strategy":"all observed effective optimizer rules (without hints)",
           "version_spark":s.version,"cases":{}}
    try:
      with tempfile.TemporaryDirectory() as td:
        for case in MANIFEST["cases"]:
          cid=case["id"]
          base=observations["observations"].get(cid,{})
          if case["type"]!="bug" or base.get("candidate_status")!="DIFFERENCE" or not base.get("control_matches_expected"):
            output["cases"][cid]={"status":"NOT_CONFIRMED_OPTIMIZER_WRONG_ANSWER","attempts":[]}
            continue
          attempts=[]
          output["cases"][cid]={"status":"NO_RESTORING_INTERVENTION","attempts":attempts}
          for idx,(rule,count) in enumerate(by_case.get(cid,{}).items()):
            ss=s.newSession()
            for k,v in case.get("config",{}).items():ss.conf.set(k,v)
            ss.conf.set("spark.sql.optimizer.excludedRules",rule)
            dest=f"{td}/{cid}/{idx}"
            t=time.monotonic()
            result={"rule":rule,"observed_event_count":count}
            try:
              prepare(ss,case["setup"],dest)
              actual=run_sql(ss,case["sql"])
              changed=norm_plan(actual["optimized_plan"])!=norm_plan(base["candidate"]["optimized_plan"])
              correct=actual["rows"]==case["expected"]
              result.update({"rows":actual["rows"],"plan_changed":changed,"correct":correct,
                             "status":("RESTORES_EXPECTED" if changed and correct else
                                       "NO_PLAN_CHANGE" if not changed else
                                       "WRONG_RESULT_PERSISTS")})
              if result["status"]=="RESTORES_EXPECTED":
                output["cases"][cid]["status"]="RESTORED_BY_SINGLE_RULE_ABLATION"
            except Exception as e:
              result.update({"status":"INCONCLUSIVE_ERROR","error":repr(e)})
            result["duration_seconds"]=round(time.monotonic()-t,2)
            attempts.append(result)
            print("QUERYPROOF_EXHAUSTIVE",cid,idx,result["status"],rule,flush=True)
    finally:
      s.stop()
    p=Path("artifacts/queryproof_exhaustive_ablation.json")
    p.write_text(json.dumps(output,indent=2))
    return 0
if __name__=="__main__":
    sys.exit(main())
