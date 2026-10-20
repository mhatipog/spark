"""Post-FinishAnalysis-only quarantine for observed Catalyst TreeNode tags.

A versioned refinement of the intentionally overconservative whole-trace
quarantine. The zero-tag requirement still does not certify metadata, arbitrary
runtime effects, or unobserved transient mutation.
"""
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
from frame_guard import parse_records


def tagged_optimizer_suffix_cases(records):
    stack = []
    tagged = set()
    finish = Counter()

    def active():
        return any(
            entry["kind"] == "EXECUTOR" and
            entry["executor"] == "Optimizer" and
            entry["finished"] for entry in stack)

    def has_tags(record):
        states = record.get("states")
        if not isinstance(states, list) or not states:
            raise ValueError("MISSING_TAG_COUNTS")
        for state in states:
            if (not isinstance(state, list) or len(state) != 4 or
                    any(type(x) is not int or x < 0 for x in state)):
                raise ValueError("MALFORMED_TAG_COUNTS")
        return any(state[0] > 0 for state in states)

    for record in records:
        case = record["core"][0]
        tagged_here = has_tags(record)
        if record["kind"] == "EVENT":
            if active() and tagged_here:
                tagged.add(case)
            continue

        case, kind, executor, batch, uid, phase = record["core"]
        if phase == "BEGIN":
            if active() and tagged_here:
                tagged.add(case)
            stack.append({
                "case": case, "kind": kind, "executor": executor,
                "batch": batch, "uid": uid, "finished": False,
            })
            continue

        if phase != "END" or not stack:
            raise ValueError("ORPHAN_SCOPE")
        top = stack[-1]
        if any(top[key] != value for key, value in (
                ("case", case), ("kind", kind), ("executor", executor),
                ("batch", batch), ("uid", uid))):
            raise ValueError("SCOPE_MISMATCH")
        if active() and tagged_here:
            tagged.add(case)
        stack.pop()
        if kind == "BATCH" and batch == "Finish Analysis":
            if not stack or stack[-1]["kind"] != "EXECUTOR" or (
                    stack[-1]["executor"] != "Optimizer"):
                raise ValueError("MISSING_OPTIMIZER_CUT")
            stack[-1]["finished"] = True
            finish[case] += 1
            # The cut's output is precisely the first suffix state.
            if tagged_here:
                tagged.add(case)
    if stack:
        raise ValueError("UNCLOSED_SCOPES")
    if any(n != 1 for n in finish.values()):
        raise ValueError("MULTIPLE_FINISH_CUTS")
    return tagged, finish


def suffix_tag_quarantine(frame_report, records, parse_errors):
    if parse_errors or frame_report.get("status") != "COMPLETE_FRAME_COMPARISON":
        return {"status": "INVALID_SUFFIX_TAG_AUDIT", "errors": sorted(
            set(parse_errors + frame_report.get("errors", []))),
            "strict_certified_suffix_cases": []}
    try:
        tagged, finish = tagged_optimizer_suffix_cases(records)
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        return {"status": "INVALID_SUFFIX_TAG_AUDIT",
                "errors": [str(exc)], "strict_certified_suffix_cases": []}
    originally = set(frame_report["newly_frame_preserved_suffixes"])
    if not originally <= set(finish):
        return {"status": "INVALID_SUFFIX_TAG_AUDIT",
                "errors": ["MISSING_CERTIFICATE_NORMALIZATION_CUT"],
                "strict_certified_suffix_cases": []}
    accepted = sorted(originally - tagged)
    return {
        "status": "COMPLETE_SUFFIX_TAG_AUDIT",
        "optimizer_suffixes_with_tagged_state": len(tagged),
        "optimizer_suffixes_with_normalization_cut": len(finish),
        "original_frame_preserved_suffixes": len(originally),
        "original_certificates_quarantined": len(originally & tagged),
        "strict_certified_changed_suffixes": len(accepted),
        "strict_certified_suffix_cases": accepted,
        "policy": "Any observed nonempty tag in a post-FinishAnalysis suffix makes that case UNKNOWN",
        "limits": [
            "Observation only at captured states, not continuous mutation monitoring",
            "Unmodified lossy frame JSON and other hidden-state limitations remain",
            "No whole-Spark or Analyzer/FinishAnalysis correctness assertion",
        ],
    }


def main():
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
    import run_frame
    old = run_frame.score_frame

    def score(path, scopes):
        report = old(path, scopes)
        records, errors = parse_records(
            Path(path).read_text(errors="replace").splitlines())
        report["v3_suffix_tag_quarantine"] = suffix_tag_quarantine(
            report, records, errors)
        return report

    run_frame.score_frame = score
    return run_frame.main("H")


if __name__ == "__main__":
    raise SystemExit(main())
