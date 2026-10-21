"""Score the frozen V17 hypotheses from the kernel report and the oracle report.
Usage: python summarize.py KERNEL_REPORT.json ORACLE_REPORT.json OUT.json
Definitions come from PROTOCOL.json; nothing here is tuned after the run.
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RULES = {"IFFC": "InferFiltersFromConstraints", "PDP": "PushDownPredicates"}


def main(kernel, oracle, out):
    protocol = json.loads((HERE / "PROTOCOL.json").read_text())
    k = json.loads(Path(kernel).read_text())
    o = json.loads(Path(oracle).read_text())
    per_rule = {}
    for short, rule in RULES.items():
        total = k["events_by_rule"].get(rule, 0)
        acc = k["accepted_by_rule"].get(rule, 0)
        per_rule[short] = {
            "events": total, "accepted": acc,
            "rejected": k["rejected_printed_by_rule"].get(rule, 0),
            "unknown": k["unsupported_by_rule"].get(rule, 0),
            "coverage": (acc / total) if total else None,
            "mutants": k["mutants_by_rule"].get(rule, 0),
        }
    other_accepted = {r: n for r, n in k["accepted_by_rule"].items() if r not in RULES.values()}
    t = protocol["hypotheses"]
    verdicts = {
        "H1_zero_mutants_accepted": len(k["mutants_accepted"]) == 0,
        "H2_iffc_coverage": (per_rule["IFFC"]["coverage"] or 0) >= t["H2_iffc_coverage"]["threshold"],
        "H3_pdp_coverage": (per_rule["PDP"]["coverage"] or 0) >= t["H3_pdp_coverage"]["threshold"],
        "H4_no_other_rule_accepted": not other_accepted,
        "H5_oracle_agreement": not o["mismatched"],
    }
    result = {"protocol_id": protocol["protocol_id"], "per_rule": per_rule,
              "other_rule_accepted": other_accepted,
              "mutants_total": k["mutants_total"],
              "mutants_accepted": k["mutants_accepted"],
              "mutants_rejected_by_kind": k["mutants_rejected_by_kind"],
              "mutants_unsupported": len(k["mutants_unsupported"]),
              "unsupported_by_rule_reason": k["unsupported_by_rule_reason"],
              "rejected_events": k["rejected_events"],
              "oracle": {"cases": o["cases"], "matched": o["matched"],
                         "mismatched": o["mismatched"],
                         "spark_errors": len(o["spark_errors"]),
                         "sqlite_errors": len(o["sqlite_errors"])},
              "verdicts": verdicts}
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_V17_RESULT", json.dumps({"per_rule": per_rule, "verdicts": verdicts,
                                               "mutants_total": k["mutants_total"],
                                               "mutants_accepted": len(k["mutants_accepted"])},
                                              sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:4])
