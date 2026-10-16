"""Post-discovery scorecard, intentionally isolated from blind_ablation.py."""
import json
from pathlib import Path
from run import MANIFEST
expected={c["id"]:c for c in MANIFEST["cases"]}
original=json.loads(Path("artifacts/prospective_score_4.2.0.json").read_text())
blind=json.loads(Path("artifacts/prospective_blind_ablation_4.2.0.json").read_text())
rank=json.loads(Path("artifacts/prospective_ast_ranking.json").read_text())
results={"spark":"4.2.0","cases":{}}
for cid,c in expected.items():
    base=original["cases"][cid]
    detection=blind["cases"][cid]
    risk=rank["ranked_candidates"].get(cid,[])
    results["cases"][cid]={
      "kind":c["kind"],"correctness_label":base["status"],
      "expected_answer":c["expected"],
      "reference_sql_or_config_detects_difference":base.get("config_or_sql_disagreement"),
      "unchanged_structural_heuristic_flag":bool(risk),
      "blind_rule_ablation_disagreement":bool(detection["disagreements"]),
      "blind_flagged_rule_names":[r.split(".")[-1] for r in detection["disagreements"]],
      "attempted_rule_exclusions":len(detection["attempts"]),
    }
reported=[v for v in results["cases"].values() if v["correctness_label"]=="BUG_REPRODUCED"]
negative=[v for v in results["cases"].values() if v["kind"]=="negative_control"]
results["summary"]={
  "confirmed_bug_cases":len(reported),
  "negative_controls":len(negative),
  "structural_flags_on_confirmed_bugs":sum(x["unchanged_structural_heuristic_flag"] for x in reported),
  "ablation_flags_on_confirmed_bugs":sum(x["blind_rule_ablation_disagreement"] for x in reported),
  "reference_differential_flags_on_confirmed_bugs":sum(x["reference_sql_or_config_detects_difference"] is True for x in reported),
  "structural_flags_on_negative_controls":sum(x["unchanged_structural_heuristic_flag"] for x in negative),
  "ablation_flags_on_negative_controls":sum(x["blind_rule_ablation_disagreement"] for x in negative),
}
p=Path("artifacts/prospective_blind_score_4.2.0.json");p.write_text(json.dumps(results,indent=2))
print("QUERYPROOF_POSTHOC_SCORE",results["summary"])
for cid,case in results["cases"].items():
    print("QUERYPROOF_POSTHOC_CASE",cid,case["correctness_label"],
          "structural",case["unchanged_structural_heuristic_flag"],
          "blind_ablation",case["blind_rule_ablation_disagreement"],
          "reference",case["reference_sql_or_config_detects_difference"])
