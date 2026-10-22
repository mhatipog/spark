"""Classify every kernel verdict with the independent SQLite differential.
Usage: python validate.py KERNEL_REPORT.json V15Kernel.v OUT.json

Events: each ACCEPTED event must never differ (H0).
Mutants: VALIDATED_WRONG (some database distinguishes OLD/NEW), ILL_SCOPED
(references an attribute its input does not output), NOT_SHOWN_WRONG
(no difference in any trial; excluded from H1), OUTSIDE (not evaluable).
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from differential import differs, ILL_SCOPED  # noqa: E402

LABEL = {True: "VALIDATED_WRONG", False: "NOT_SHOWN_WRONG", None: "OUTSIDE",
         ILL_SCOPED: "ILL_SCOPED"}


def main(kernel_report, kernel_file, out):
    report = json.loads(Path(kernel_report).read_text())
    text = Path(kernel_file).read_text()
    terms = dict(re.findall(r"Definition ((?:ev|mut)_[on]\d+) := (.*?)\.\n", text))
    verdicts = {(p, int(i)): v == "true" for p, i, v in
                re.findall(r"Example (ev|mut)_(\d+) : \w+ \w+ \w+ = (true|false)", text)}

    events = []
    for i, meta in enumerate(report["event_index"]):
        assert verdicts[("ev", i)] == meta["accepted"], "EVENT_INDEX_MISMATCH"
        if not meta["accepted"]:
            continue
        r = differs(terms[f"ev_o{i}"], terms[f"ev_n{i}"], seed=i)
        events.append({**meta, "differential": LABEL[r]})

    mutants = []
    for i, meta in enumerate(report["mutant_index"]):
        assert verdicts[("mut", i)] == meta["accepted"], "MUTANT_INDEX_MISMATCH"
        r = differs(terms[f"mut_o{i}"], terms[f"mut_n{i}"], seed=10_000 + i)
        mutants.append({**meta, "class": LABEL[r]})

    invalid = [m for m in mutants if m["class"] in ("VALIDATED_WRONG", "ILL_SCOPED")]
    result = {
        "accepted_events": len(events),
        "accepted_events_by_differential": dict(Counter(e["differential"] for e in events)),
        "accepted_events_that_differ": [e for e in events if e["differential"] == "VALIDATED_WRONG"],
        "mutants_by_class": dict(Counter(m["class"] for m in mutants)),
        "mutants_by_family_class": dict(Counter(f"{m['mutant']} | {m['class']}" for m in mutants)),
        "invalid_mutants": len(invalid),
        "invalid_mutants_accepted": [m for m in invalid if m["accepted"]],
        "accepted_mutants_not_shown_wrong": [m for m in mutants
                                             if m["accepted"] and m["class"] == "NOT_SHOWN_WRONG"],
    }
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_V19_VALIDATION", json.dumps({
        "accepted_events": len(events),
        "events": result["accepted_events_by_differential"],
        "mutants": result["mutants_by_class"],
        "invalid_mutants_accepted": len(result["invalid_mutants_accepted"])}, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:4])
