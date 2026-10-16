"""Frozen-feature holdout evaluator: run unmodified rank_ast.py on external AST events.
Separates query-result oracle, candidate generation, and issue-specific root cause.
"""
import json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"queryproof_instrumented"))
from rank_ast import analyze
root=Path(__file__).resolve().parent
manifest=json.loads((root/"holdout_manifest.json").read_text())
obs=json.loads(Path("artifacts/queryproof_holdout_observations.json").read_text())
ranking=analyze("artifacts/queryproof-holdout-ast.log")
summary={"manifest_sha256":obs["manifest_sha256"],"n_events":ranking["n_events"],
         "parser_errors":ranking["errors"],"cases":{}}
for case in manifest["cases"]:
    cid=case["id"]; found=ranking["ranked_candidates"].get(cid,[])
    target=case.get("ground_truth_rule")
    result=obs["observations"].get(cid,{})
    rows=result.get("candidate",{}).get("rows")
    candidate_status=result.get("candidate_status","MISSING")
    reproduces=candidate_status=="DIFFERENCE" and case["type"] in ("bug","out_of_scope_expression_bug")
    ranks=[i+1 for i,x in enumerate(found) if target and (x["rule"]==target or x["rule"].endswith("."+target.split(".")[-1]))]
    summary["cases"][cid]={
      "issue":case["issue"],"type":case["type"],
      "reproduces_expected_bug":reproduces,
      "candidate_status":candidate_status,
      "top_candidates":[{"rank":i+1,"rule":x["rule"],"score":x["score"],"features":x["features"]}
                        for i,x in enumerate(found[:5])],
      "expected_rule_rank":ranks[0] if ranks else None,
      "target_rule_appears_in_any_event":any(target and ev["rule"]==target for ev in []),
      "candidate_rows":rows,
      "control_matches_expected":result.get("control_matches_expected")
    }
Path("artifacts/queryproof_holdout_score.json").write_text(json.dumps(summary,indent=2))
print("QUERYPROOF_HOLDOUT_SCORE",summary["n_events"],"events",len(summary["parser_errors"]),"errors")
for cid,c in summary["cases"].items():
    print("QUERYPROOF_HOLDOUT_SCORE",cid,c["candidate_status"],"expected_rule_rank",c["expected_rule_rank"],
          "top1",c["top_candidates"][0]["rule"] if c["top_candidates"] else None,flush=True)
if summary["parser_errors"]:raise SystemExit(2)
