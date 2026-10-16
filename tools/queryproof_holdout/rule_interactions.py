"""Causal interaction search using two-rule interventions on real Spark.

Only uses observed AST rule changes and result perturbations to select A and B.
Known Jira root cause and expected answers are never used in candidate discovery.
SINGLE disable vs DOUBLE disable can reveal masking of unsafe rewrites.
"""
import json,sys,tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from run_holdout import MANIFEST,prepare,run_sql
from oracle_free_perturbation import canonical
from exhaustive_ablation import trace_candidate_rules,norm_plan

def main():
    old=json.loads(Path("artifacts/queryproof_holdout_observations.json").read_text())
    single=json.loads(Path("artifacts/queryproof_oracle_free_discovery.json").read_text())["discovery"]
    observed=trace_candidate_rules("artifacts/queryproof-holdout-ast.log")
    s=(SparkSession.builder.master("local[2]").appName("QueryProof-SecondOrder-Causal-Search")
       .config("spark.ui.enabled","false")
       .config("spark.sql.adaptive.enabled","false")
       .config("spark.sql.shuffle.partitions","5").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    output={"version":1,"spark_version":s.version,"interaction_search":{},
            "disclaimer":"Contextual suppression of a wrong answer is evidence, not a proof of intrinsic rule unsoundness."}
    try:
      with tempfile.TemporaryDirectory() as td:
        for c in MANIFEST["cases"]:
          cid=c["id"]
          base=old["observations"].get(cid,{})
          if "candidate" not in base or cid not in single:
            continue
          base_rows=base["candidate"]["rows"]
          prev={a["rule"]:a for a in single[cid]["attempts"]}
          divergent=single[cid]["disagreeing_rules"]
          rec={"baseline_rows":base_rows,"divergent_single_rule_exclusions":divergent,
               "interventions":[],"suppressor_pairs":[]}
          output["interaction_search"][cid]=rec
          for i,a in enumerate(divergent):
            arows=prev[a]["rows"]
            for j,b in enumerate(observed.get(cid,{})):
              if a==b:continue
              run={"rule_a":a,"rule_b":b}
              ss=s.newSession()
              for k,v in c.get("config",{}).items():ss.conf.set(k,v)
              ss.conf.set("spark.sql.optimizer.excludedRules",a+","+b)
              try:
                prepare(ss,c["setup"],f"{td}/{cid}/{i}/{j}")
                both=run_sql(ss,c["sql"])
                br=both["rows"]
                b_single=prev.get(b,{}).get("rows")
                # Difference caused by B only in the context where A is excluded.
                interaction=(canonical(br)!=canonical(arows) and
                             b_single is not None and
                             canonical(b_single)==canonical(base_rows))
                suppress=(interaction and canonical(br)==canonical(base_rows))
                run.update({"status":"VALID","both_rows":br,
                  "b_alone_rows":b_single,"interaction_detected":interaction,
                  "restores_baseline":suppress,
                  "plan_changed_from_baseline":norm_plan(both["optimized_plan"])!=
                     norm_plan(base["candidate"]["optimized_plan"])})
                if suppress:
                  rec["suppressor_pairs"].append({"masking_rule":a,"suppressed_rule":b})
              except Exception as ex:
                run.update({"status":"INCONCLUSIVE_ERROR","error":repr(ex)})
              rec["interventions"].append(run)
              if run.get("interaction_detected"):
                print("QUERYPROOF_INTERACTION",cid,a.split(".")[-1],b.split(".")[-1],
                      "suppress",run["restores_baseline"],"both",run.get("both_rows"),flush=True)
    finally:
      s.stop()
    Path("artifacts/queryproof_rule_interactions.json").write_text(json.dumps(output,indent=2))
    print("QUERYPROOF_INTERACTION_SUMMARY",json.dumps({
      k:len(v["suppressor_pairs"]) for k,v in output["interaction_search"].items()}),flush=True)
    return 0
if __name__=="__main__":
    sys.exit(main())
