"""Strict successor framing: quarantine any observed nonempty TreeNode tag map.

This experiment deliberately sacrifices coverage rather than pretending a
lossy hash proves preservation of arbitrary JVM object state. No historical
H/K/L protocol or verifier file is changed.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))

from frame_guard import parse_records


def quarantine_tagged_cases(frame_report, records, parse_errors):
    if parse_errors or frame_report.get("status") != "COMPLETE_FRAME_COMPARISON":
        return {
            "status": "INVALID_STRICT_TAG_AUDIT",
            "errors": sorted(set(parse_errors + frame_report.get("errors", []))),
            "strict_certified_suffix_cases": [],
        }
    tagged = set()
    for record in records:
        for state in record["states"]:
            if len(state) != 4 or any(type(v) is not int or v < 0 for v in state):
                return {
                    "status": "INVALID_STRICT_TAG_AUDIT",
                    "errors": ["MALFORMED_TAG_COUNTS"],
                    "strict_certified_suffix_cases": [],
                }
            if state[0]:
                tagged.add(record["core"][0])
    initially_accepted = set(frame_report["newly_frame_preserved_suffixes"])
    accepted = sorted(initially_accepted - tagged)
    return {
        "status": "COMPLETE_STRICT_TAG_AUDIT",
        "original_frame_preserved_suffixes": len(initially_accepted),
        "observed_tagged_cases": len(tagged),
        "original_certificates_quarantined": len(initially_accepted & tagged),
        "strict_certified_suffix_cases": accepted,
        "strict_certified_changed_suffixes": len(accepted),
        "policy": "Any observed nonempty TreeNode tag map makes the entire case UNKNOWN",
        "limits": [
            "Conservative whole-trace quarantine, including pre-normalization tags",
            "A no-tag trace does not prove complete JVM-state equality",
            "Only changes this tag-collision exposure, not all soundness assumptions",
        ],
    }


def score_strict_frame(path, scopes, old_score_frame):
    report = old_score_frame(path, scopes)
    records, errors = parse_records(
        Path(path).read_text(errors="replace").splitlines())
    report["v2_strict_tag_quarantine"] = quarantine_tagged_cases(
        report, records, errors)
    return report


def main():
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
    import run_frame
    original = run_frame.score_frame

    def wrapped(path, scopes):
        return score_strict_frame(path, scopes, original)

    run_frame.score_frame = wrapped
    return run_frame.main("H")


if __name__ == "__main__":
    raise SystemExit(main())
