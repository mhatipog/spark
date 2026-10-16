"""No-answer-oracle ablation baseline on the preregistered external cases.

Uses only the original SQL, base Spark output, and real AST event rule IDs.
Reads no expected outputs, issue IDs, ground-truth rule, or control results
during detection. Disable only observed effective optimizer rules.
"""
import base64, collections, json, re, sys, tempfile, traceback
from pathlib import Path
from run import MANIFEST, fixture, run_once, configured_session, enc
from pyspark.sql import SparkSession

def rules_by_case(log_path):
    found=collections.defaultdict(dict)
    for line in Path(log_path).read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):continue
        parts=line.split("\t")
        if len(parts)!=8:raise ValueError("AST event missing fields")
        case,executor,batch,rule=[base64.b64decode(x,validate=True).decode("utf-8") for x in parts[1:5]]
        if ".optimizer." in rule:
            found[case][rule]=found[case].get(rule,0)+1
    return found

def canon(rows,mode):
    if mode=="sequence":return [json.dumps(x,sort_keys=True,default=str) for x in rows]
    return sorted(json.dumps(x,sort_keys=True,default=str) for x in rows)

def nplan(x):
    return re.sub(r"#\d+L?", "#COL", x)

def main():
    source=Path("artifacts/prospective_observations_4.2.0.json")
    data=json.loads(source.read_text())
    rules=rules_by_case("artifacts/prospective-spark.log")
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-Prospective-Rule-Perturbations")
        .config("spark.ui.enabled","false")
        .config("spark.sql.adaptive.enabled","false")
        .config("spark.sql.shuffle.partitions","5").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    output={"spark_version":spark.version,
        "manifest_sha256":data["manifest_sha256"],
        "method":"disable each observed effective optimizer rule, compare SQL outputs",
        "expected_answers_used_for_discovery":False,"cases":{}}
    try:
      with tempfile.TemporaryDirectory(prefix="queryproof_perturb_") as td:
        for c in MANIFEST["cases"]:
            cid=c["id"]
            original=data["cases"][cid]
            item={"status":"NO_DISAGREEMENT","observed_rules":len(rules.get(cid,{})),
                  "attempts":[],"disagreements":[]}
            output["cases"][cid]=item
            if original["status"]!="OBSERVED":
                item["status"]="BASELINE_UNAVAILABLE"
                continue
            before=original["observations"][0]["rows"]
            plan=original["observations"][0]["logical_plan"]
            for i,rule in enumerate(rules.get(cid,{})):
                s,settings=configured_session(spark,c)
                record={"rule":rule,"status":"INCONCLUSIVE"}
                item["attempts"].append(record)
                try:
                    fixture(s,c["fixture"],td+"/"+cid+"/"+str(i))
                    # Important: intervention after fixture, not before.
                    s.conf.set("spark.sql.optimizer.excludedRules",rule)
                    later=run_once(s,c["sql"],c.get("observed_columns"))
                    changed=nplan(later["logical_plan"])!=nplan(plan)
                    different=canon(before,c["comparison"])!=canon(later["rows"],c["comparison"])
                    status=("DIVERGENT_RESULT" if changed and different else
                            "UNSTABLE_RESULT_SAME_PLAN" if different else
                            "NO_PLAN_CHANGE" if not changed else "SAME_RESULT")
                    record.update(status=status,rows=later["rows"],plan_changed=changed,
                                  result_changed=different)
                    if status=="DIVERGENT_RESULT":
                        item["disagreements"].append(rule)
                        item["status"]="RESULT_DISAGREEMENT"
                except Exception as ex:
                    record.update(status="ERROR",error=repr(ex))
            print("QUERYPROOF_BLIND_ABLATION",cid,item["status"],len(item["attempts"]),
                  [r.split(".")[-1] for r in item["disagreements"]],flush=True)
    finally:
        spark.stop()
    Path("artifacts/prospective_blind_ablation_4.2.0.json").write_text(json.dumps(output,indent=2,sort_keys=True))
    return 0
if __name__=="__main__":
    raise SystemExit(main())
