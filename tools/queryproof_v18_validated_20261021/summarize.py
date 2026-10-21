"""Score the frozen V18 hypotheses. Thresholds come from PROTOCOL.json.
Usage: python summarize.py KERNEL.json VALIDATION.json ORACLE.json OUT.json
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RULES = {"IFFC": "InferFiltersFromConstraints", "PDP": "PushDownPredicates"}


def main(kernel, validation, oracle, out):
    p = json.loads((HERE / "PROTOCOL.json").read_text())["hypotheses"]
    k = json.loads(Path(kernel).read_text())
    v = json.loads(Path(validation).read_text())
    o = json.loads(Path(oracle).read_text())
    per_rule = {}
    for short, rule in RULES.items():
        total = k["events_by_rule"].get(rule, 0)
        acc = k["accepted_by_rule"].get(rule, 0)
        per_rule[short] = {"events": total, "accepted": acc,
                           "rejected": k["rejected_printed_by_rule"].get(rule, 0),
                           "unknown": k["unsupported_by_rule"].get(rule, 0),
                           "coverage": acc / total if total else None}
    ev = v["accepted_events_by_differential"]
    evaluable = ev.get("NOT_SHOWN_WRONG", 0) + ev.get("VALIDATED_WRONG", 0)
    frac = evaluable / v["accepted_events"] if v["accepted_events"] else 0.0
    wrong = v["mutants_by_class"].get("VALIDATED_WRONG", 0)
    verdicts = {
        "H0a_no_accepted_event_differs": not v["accepted_events_that_differ"],
        "H0b_evaluable_fraction": frac >= p["H0b_evaluable_fraction"]["threshold"],
        "H1_no_invalid_mutant_accepted": not v["invalid_mutants_accepted"],
        "H1b_power": wrong >= p["H1b_power"]["threshold"],
        "H2_iffc_coverage": (per_rule["IFFC"]["coverage"] or 0) >= p["H2_iffc_coverage"]["threshold"],
        "H3_pdp_coverage": (per_rule["PDP"]["coverage"] or 0) >= p["H3_pdp_coverage"]["threshold"],
        "H5_oracle_agreement": not o["mismatched"],
    }
    result = {
        "per_rule": per_rule,
        "accepted_events_evaluable_fraction": frac,
        "validation": v["mutants_by_class"],
        "invalid_mutants": v["invalid_mutants"],
        "invalid_mutants_accepted": v["invalid_mutants_accepted"],
        "accepted_mutants_not_shown_wrong": len(v["accepted_mutants_not_shown_wrong"]),
        "reported_other_rule_accepted": {r: n for r, n in k["accepted_by_rule"].items()
                                         if r not in RULES.values()},
        "unsupported_by_rule_reason": k["unsupported_by_rule_reason"],
        "oracle": {"cases": o["cases"], "matched": o["matched"],
                   "mismatched": o["mismatched"], "spark_errors": len(o["spark_errors"]),
                   "sqlite_errors": len(o["sqlite_errors"])},
        "verdicts": verdicts,
    }
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_V18_RESULT", json.dumps({k2: result[k2] for k2 in (
        "per_rule", "accepted_events_evaluable_fraction", "validation", "invalid_mutants",
        "accepted_mutants_not_shown_wrong", "reported_other_rule_accepted", "verdicts")},
        sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:5])
