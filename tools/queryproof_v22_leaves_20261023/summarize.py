"""Score the frozen V22 hypotheses. Thresholds come from PROTOCOL.json.
Usage: python summarize.py TRACE.json ENDPOINTS.json EVENTS.json EVENT_VALIDATION.json ORACLE.json OUT.json
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main(trace, endpoints, events, ev_validation, oracle, out):
    p = json.loads((HERE / "PROTOCOL.json").read_text())["hypotheses"]
    t = json.loads(Path(trace).read_text())
    ep = json.loads(Path(endpoints).read_text())
    ev = json.loads(Path(events).read_text())
    v = json.loads(Path(ev_validation).read_text())
    o = json.loads(Path(oracle).read_text())
    agrees = ep["by_class"].get("AGREES", 0)
    frac_eval = agrees / ep["certified_endpoints"] if ep["certified_endpoints"] else 0.0
    denom = t["queries_with_suffix_events"]
    coverage = t["certified_nonempty_full_traces"] / denom if denom else 0.0
    verdicts = {
        "T0a_no_certified_trace_differs": not ep["differing"],
        "T0b_endpoint_evaluable_fraction": frac_eval >= p["T0b_endpoint_evaluable_fraction"]["threshold"],
        "T1_no_trace_mutant_accepted": not t["trace_mutants_accepted"],
        "T1b_trace_mutant_power": t["trace_mutants"] >= p["T1b_trace_mutant_power"]["threshold"],
        "E1_no_invalid_event_mutant_accepted": not v["invalid_mutants_accepted"],
        "E1b_event_mutant_power": v["mutants_by_class"].get("VALIDATED_WRONG", 0)
                                  >= p["E1b_event_mutant_power"]["threshold"],
        "T2_connected_observation": t["json_connectivity_breaks"] == 0 and t["kernel_link_failures"] == 0,
        "T3_contract_coverage": not t["unknown_tags"] and not t["ansi_off"] and t["unknown_tag_lines"] == 0,
        "T4_full_trace_coverage": coverage >= p["T4_full_trace_coverage"]["threshold"],
        "H5_oracle_agreement": not o["mismatched"],
        "T5_leaf_identity_observed": "LEAF_IDENTITY_UNOBSERVED" not in t["unprintable_by_reason"],
    }
    result = {"verdicts": verdicts, "full_trace_coverage": coverage,
              "certified_nonempty_full_traces": t["certified_nonempty_full_traces"],
              "queries_with_suffix_events": denom, "printed_traces": t["printed_traces"],
              "first_failure_by_rule": t["first_failure_by_rule"],
              "unprintable_by_reason": t["unprintable_by_reason"],
              "uncertified_events_by_rule": t["uncertified_events_by_rule"],
              "events_by_rule": t["events_by_rule"], "observed_tags": t["observed_tags"],
              "trace_mutants": t["trace_mutants"],
              "trace_mutants_rejected_by_kind": t["trace_mutants_rejected_by_kind"],
              "endpoints": ep["by_class"],
              "event_accepted_by_rule": ev["accepted_by_rule"],
              "event_rejected_by_rule": ev["rejected_printed_by_rule"],
              "event_validation": v["mutants_by_class"],
              "event_accepted_differential": v["accepted_events_by_differential"],
              "oracle": {"cases": o["cases"], "matched": o["matched"], "mismatched": o["mismatched"]}}
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_V22_RESULT", json.dumps({k: result[k] for k in (
        "verdicts", "full_trace_coverage", "certified_nonempty_full_traces",
        "queries_with_suffix_events", "trace_mutants", "endpoints", "event_validation",
        "first_failure_by_rule")}, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:7])
