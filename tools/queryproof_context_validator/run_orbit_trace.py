"""Apply unchanged equality-orbit verifier to actual RuleExecutor AST trace events.

Raw Catalyst JSON can include SQL literals. Read it locally and publish only
compact rule-level statuses/counters, never the AST or SQL text.
"""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_instrumented"))
from rank_ast import decode
from equality_orbits import certify_predicate_event

def classify_events(events):
    by_case=defaultdict(list)
    counts=Counter()
    unknowns=Counter()
    for event in events:
        report=certify_predicate_event(event)
        status=report["status"]
        counts[status]+=1
        case=event.get("case", "unlabelled")
        if status=="UNKNOWN":
            reason=report.get("reason","UNSPECIFIED")
            unknowns[reason]+=1
            entry={"rule": event.get("rule"), "status":status,
                   "reason":reason}
        else:
            root=report["root"]
            witness=root["minimal_witness"]
            entry={"rule":event.get("rule"),"status":status,
                   "operator":report["operator"],
                   "required_partition":report["required_partition"],
                   "context_path":report["context_path"],
                   "attributes":root["input_attributes"],
                   "orbits_checked":root["orbits_checked"],
                   "safe_observations":root["safe_partitions"],
                   "local_guard":report["local_guard"],
                   "semantic_context_masking":report["semantic_context_masking"],
                   "sqlite_verified":all(x["verified"]
                                         for x in report["independent_verification"]),
                   "witness_values":list(witness["bindings"].values()) if witness else None}
        by_case[case].append(entry)
    return {"summary":{"events":sum(counts.values()),"statuses":dict(counts),
                       "unknown_reasons":dict(unknowns),
                       "scope":"equality/nullness-only local Filter or INNER Join condition rewrites",
                       "warning":"Local counterexamples are not verified reachable source-row witnesses; historical calibration is not a holdout"},
            "cases":dict(by_case)}

def run(trace_path,output_path):
    events=[]
    errors=[]
    for line_number,line in enumerate(Path(trace_path).read_text(errors="replace").splitlines(),1):
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        try:
            events.append(decode(line))
        except Exception as exc:
            # Never publish the log line or its literal-bearing AST.
            errors.append({"line":line_number,"kind":type(exc).__name__})
    output=classify_events(events)
    output["summary"]["parse_errors"]=errors
    output["summary"]["trace_events"]=len(events)
    target=Path(output_path)
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(output,indent=2),encoding="utf8")
    print("QUERYPROOF_ORBIT_TRACE",json.dumps(output["summary"]),flush=True)
    return 0 if events and not errors else 1

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("trace")
    parser.add_argument("--output",default="artifacts/queryproof_orbit_trace.json")
    args=parser.parse_args()
    raise SystemExit(run(args.trace,args.output))
