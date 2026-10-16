"""Read-only audit of the physical ordering property experiment.

This computes benchmark error labels *after* metadata inspection, separately
from the ordering probe. The experiment has one known bug family; measured
sensitivity is NOT a population estimate and absent Spark guarantees are NOT
assertions that a result is wrong.
"""
import json,sys
from collections import Counter
from pathlib import Path

DECLARED="DECLARED_GLOBAL_ORDER_SATISFIED"
UNPROVEN="GLOBAL_ORDER_NOT_ESTABLISHED"
OUTSIDE="OUT_OF_SCOPE_NO_ROOT_GLOBAL_SORT"

def evaluate(data):
    t=Counter()
    rows=[]
    errors=[]
    for name,section in data["settings"].items():
        for trial in section["trials"]:
            after=trial.get("after",{})
            verdict=after.get("verdict", "ERROR")
            good=trial.get("matches_sql_oracle")
            if good not in (True,False):
                errors.append(f"{name}: oracle missing")
                continue
            t[("correct" if good else "wrong",verdict)]+=1
            if not good and verdict==DECLARED:
                errors.append(f"{name}: incorrect SQL result but ordering is declared satisfied")
            if verdict.startswith("UNKNOWN"):
                errors.append(f"{name}: metadata inspection unknown: {after.get('inspection_error')}")
            rows.append({"case":name,"correct":good,"verdict":verdict,
                "spark_final":after.get("is_final_aqe_plan"),
                "range_matches":after.get("range_partitioning_matches_order"),
                "ordering_satisfies":after.get("spark_ordering_satisfies"),
                "partitions":after.get("declared_num_partitions")})
    controls=data["controls"]
    known_global=["order_by_global","order_by_global_two_keys","order_by_explicit_null","top_k"]
    known_non_global=["sort_by_partition","no_sort"]
    for name in known_global:
        status=controls.get(name,{}).get("inspection",{}).get("verdict")
        if status!=DECLARED:errors.append(f"control {name}: expected {DECLARED}, got {status}")
    for name in known_non_global:
        status=controls.get(name,{}).get("inspection",{}).get("verdict")
        if status!=OUTSIDE:errors.append(f"control {name}: expected {OUTSIDE}, got {status}")
    expected=8*5
    if len(rows)!=expected:errors.append(f"expected {expected} benchmark trials, got {len(rows)}")
    return {
        "spark_version":data["spark_version"],"number_of_trials":len(rows),
        "incorrect_and_unproven":t[("wrong",UNPROVEN)],
        "incorrect_but_declared":t[("wrong",DECLARED)],
        "correct_and_declared":t[("correct",DECLARED)],
        "correct_but_unproven":t[("correct",UNPROVEN)],
        "not_a_proof":"Spark outputOrdering is a declared planner property, not a soundness theorem. UNPROVEN is not WRONG.",
        "control_verdicts":{k:v.get("inspection",{}).get("verdict","ERROR") for k,v in controls.items()},
        "failures":errors,
        "trials":rows
    }

def main(path):
    data=json.loads(Path(path).read_text())
    result=evaluate(data)
    out=Path("artifacts")/f"queryproof_physical_audit_{data['spark_version']}.json"
    out.write_text(json.dumps(result,indent=2,sort_keys=True))
    print("QUERYPROOF_PHYSICAL_AUDIT",data["spark_version"],
      "wrong_and_unproven",result["incorrect_and_unproven"],
      "wrong_but_declared",result["incorrect_but_declared"],
      "correct_and_declared",result["correct_and_declared"],
      "correct_but_unproven",result["correct_but_unproven"],
      "failures",len(result["failures"]),flush=True)
    for x in result["failures"]:print("QUERYPROOF_PHYSICAL_AUDIT_FAIL",x,flush=True)
    return 1 if result["failures"] else 0

if __name__=="__main__":
    raise SystemExit(main(sys.argv[1]))
