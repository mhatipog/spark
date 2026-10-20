"""Retrospective H-cohort replay of real JVM input identity witnesses.

Uses a fresh disposable overlay; does not modify frozen H/K/L cases or promote
post-hoc diagnostics to prospective validity. An unmodelled leaf or missing
record quarantines the affected suffix. This is a necessary input condition,
not a sufficient rowbag-equivalence certificate.
"""
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_adversarial_20261020"))
from identity_trace_guard import audit_trace


def score_input_trace(path, scopes):
    report = audit_trace(Path(path).read_text(errors="replace").splitlines())
    records = report["records"]
    errors = list(report["errors"])
    scope_count = sum(r["kind"] == "SCOPE" for r in records)
    event_count = sum(r["kind"] == "EVENT" for r in records)
    if scope_count != scopes.get("scope_markers"):
        errors.append("SCOPE_DENOMINATOR_MISMATCH")
    if event_count != scopes.get("recorded_rule_events"):
        errors.append("EVENT_DENOMINATOR_MISMATCH")

    stack = []
    finished_cases = Counter()
    quarantined_cases = set()
    failure_counts = Counter()
    per_case_events = Counter()
    postfinish_events = Counter()
    postfinish_supported = Counter()
    for record in records:
        case = record["case"]
        if record["kind"] == "EVENT":
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("EVENT_OUTSIDE_BATCH")
                break
            parent = stack[-1]
            if (parent["case"] != case or parent["executor"] != record["context"][0] or
                    parent["batch"] != record["context"][1]):
                errors.append("EVENT_CONTEXT_MISMATCH")
                break
            per_case_events[case] += 1
            post = any(x["kind"] == "EXECUTOR" and x["finished"] for x in stack)
            if post:
                postfinish_events[case] += 1
                if record["status"] == "PRESERVED":
                    postfinish_supported[case] += 1
                else:
                    quarantined_cases.add(case)
                    failure_counts[record["reason"]] += 1
            continue

        kind, executor, batch, uid, phase = record["context"]
        if phase == "BEGIN":
            post = any(x["kind"] == "EXECUTOR" and x["finished"] for x in stack)
            if post and record["status"] != "SNAPSHOT_ONLY":
                quarantined_cases.add(case)
                failure_counts[record["reason"]] += 1
            stack.append({"kind": kind, "executor": executor, "batch": batch,
                          "uid": uid, "case": case, "start": record,
                          "finished": False, "post": post})
        elif phase == "END":
            if not stack:
                errors.append("ORPHAN_SCOPE_END")
                break
            scope = stack.pop()
            if any(scope[k] != v for k, v in
                   (("kind", kind), ("executor", executor), ("batch", batch),
                    ("uid", uid), ("case", case))):
                errors.append("SCOPE_CONTEXT_MISMATCH")
                break
            if scope["post"]:
                if scope["start"]["status"] != "SNAPSHOT_ONLY" or record["status"] != "SNAPSHOT_ONLY":
                    quarantined_cases.add(case)
                    failure_counts["INCOMPLETE_BOUNDARY_IDENTITY"] += 1
                elif scope["start"]["identities"][0] != record["identities"][0]:
                    quarantined_cases.add(case)
                    failure_counts["BOUNDARY_INPUT_REFERENCE_CHANGED"] += 1
            if kind == "BATCH" and batch == "Finish Analysis":
                if not stack or stack[-1]["kind"] != "EXECUTOR":
                    errors.append("FINISH_WITHOUT_EXECUTOR")
                    break
                stack[-1]["finished"] = True
                finished_cases[case] += 1
                if record["status"] != "SNAPSHOT_ONLY":
                    quarantined_cases.add(case)
                    failure_counts[record["reason"]] += 1
            if kind == "EXECUTOR" and scope["finished"]:
                if record["status"] != "SNAPSHOT_ONLY":
                    quarantined_cases.add(case)
                    failure_counts["INCOMPLETE_EXECUTOR_END_IDENTITY"] += 1
        else:
            errors.append("UNKNOWN_SCOPE_PHASE")
            break
    if stack:
        errors.append("UNCLOSED_SCOPES")
    if any(count != 1 for count in finished_cases.values()):
        errors.append("DUPLICATED_FINISH_ANALYSIS")
    suffixes = scopes.get("optimizer_suffix_case_counts", [])
    if len(finished_cases) != len(suffixes):
        errors.append("OPTIMIZER_CUT_DENOMINATOR_MISMATCH")
    existing = {s["case"] for s in suffixes if s["proved"] and s["changed"]}
    admitted = sorted(existing - quarantined_cases)
    if errors:
        admitted = []
    return {
        "status": "COMPLETE_INPUT_RETROSPECTIVE" if not errors else "INVALID_INPUT_RETROSPECTIVE",
        "errors": sorted(set(errors)),
        "source": "JVM_REFERENCE_ID_ALLOCATOR_V1",
        "scope_markers": scope_count,
        "rule_events": event_count,
        "postfinish_rule_events": sum(postfinish_events.values()),
        "postfinish_input_preserved_rule_events": sum(postfinish_supported.values()),
        "optimizer_suffixes_with_cut": len(finished_cases),
        "original_conditional_certificates": len(existing),
        "input_identity_eligible_conditional_certificates": len(admitted),
        "quarantined_original_conditional_certificates": len(existing & quarantined_cases),
        "failure_reasons": dict(failure_counts),
        "observed_marker_verdicts": report["counts"],
        "limits": [
            "Retrospective necessary condition only; cannot restore tag or metadata soundness",
            "Only LogicalRDD and OneRowRelation modelled; all other leaves rejected",
            "RDD reference sameness assumes a stable immutable abstract input during rewrite",
            "IdentityHashMap observer uses strong references in this CI prototype",
        ],
    }


def main():
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
    import run_frame
    previous = run_frame.score_frame

    def score(path, scopes):
        result = previous(path, scopes)
        result["v4_live_input_attestation"] = score_input_trace(path, scopes)
        return result

    run_frame.score_frame = score
    return run_frame.main("H")


if __name__ == "__main__":
    raise SystemExit(main())
