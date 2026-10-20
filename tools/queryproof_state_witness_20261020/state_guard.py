"""Fail-closed check of JVM state witnesses adjacent to every Catalyst marker.

Checks presence, exact provenance and finite nonnegative counters. This audit
does not equate identical JSON with identical physical Catalyst state; a
case is conservatively rejected if any inspected plan has tags, nonempty
metadata, or a collection type that toJSON could erase.
"""
import base64
from collections import Counter, defaultdict
from pathlib import Path


def _decode_fields(line, prefix):
    parts = line.rstrip("\n").split("\t")
    if parts[0] != prefix:
        raise ValueError("Unexpected trace kind")
    return [
        base64.b64decode(value, validate=True).decode("utf8")
        for value in parts[1:]
    ]


def _expected_marker(line):
    if line.startswith("QUERYPROOF_SCOPE\t"):
        fields = _decode_fields(line, "QUERYPROOF_SCOPE")
        if len(fields) != 7:
            raise ValueError("Malformed scope")
        case, kind, executor, batch, uid, phase, _plan = fields
        return [case, "SCOPE", kind, executor, batch, uid, phase]
    if line.startswith("QUERYPROOF_AST\t"):
        fields = _decode_fields(line, "QUERYPROOF_AST")
        if len(fields) != 7:
            raise ValueError("Malformed changed-rule event")
        case, executor, batch, rule, iteration, _before, _after = fields
        return [case, "EVENT", executor, batch, rule, iteration]
    return None


def audit_witness_stream(lines, scope_summary=None):
    pending = None
    errors = []
    markers = Counter()
    cases = defaultdict(lambda: Counter())
    for line in lines:
        if line.startswith((
                "QUERYPROOF_SCOPE_ERROR\t", "QUERYPROOF_AST_ERROR\t",
                "QUERYPROOF_STATE_ERROR\t")):
            errors.append("JVM_MARKER_ERROR")
            continue
        try:
            expected = _expected_marker(line)
            if expected is not None:
                if pending is not None:
                    errors.append("MISSING_WITNESS")
                pending = expected
                markers[expected[1]] += 1
                continue
            if line.startswith("QUERYPROOF_STATE\t"):
                state = _decode_fields(line, "QUERYPROOF_STATE")
                if pending is None:
                    errors.append("ORPHAN_WITNESS")
                    continue
                width = 4 if pending[1] == "SCOPE" else 8
                if len(state) != len(pending) + width or state[:len(pending)] != pending:
                    errors.append("WITNESS_IDENTITY_MISMATCH")
                    pending = None
                    continue
                nums = [int(n) for n in state[len(pending):]]
                if min(nums) < 0 or any(nums[i] < 1 for i in range(3, width, 4)):
                    errors.append("INVALID_JVM_COUNTER")
                    pending = None
                    continue
                case = cases[pending[0]]
                case["markers"] += 1
                for start in range(0, width, 4):
                    case["observed_plan_states"] += 1
                    for key, number in zip(("tags", "metadata", "opaque"), nums[start:start + 3]):
                        case[key + "_occurrences"] += number
                        if number:
                            case[key + "_dirty_states"] += 1
                pending = None
            elif pending is not None and line.startswith("QUERYPROOF_"):
                errors.append("NONADJACENT_WITNESS")
                pending = None
        except (ValueError, UnicodeDecodeError) as exc:
            errors.append("MALFORMED_WITNESS:" + exc.__class__.__name__)
            pending = None
    if pending is not None:
        errors.append("MISSING_FINAL_WITNESS")
    if scope_summary is not None:
        if markers["SCOPE"] != scope_summary.get("scope_markers"):
            errors.append("SCOPE_WITNESS_DENOMINATOR_MISMATCH")
        if markers["EVENT"] != scope_summary.get("recorded_rule_events"):
            errors.append("RULE_WITNESS_DENOMINATOR_MISMATCH")
    blocked = {
        case: [key for key in ("tags", "metadata", "opaque")
               if counts[key + "_dirty_states"] > 0]
        for case, counts in cases.items()
        if any(counts[key + "_dirty_states"] for key in ("tags", "metadata", "opaque"))
    }
    suffixes = (scope_summary or {}).get("optimizer_suffix_case_counts", [])
    accepted = [
        item["case"] for item in suffixes
        if item["proved"] and item["changed"] and item["case"] not in blocked
    ]
    all_cases = sorted(cases)
    return {
        "status": "COMPLETE_JVM_WITNESSES" if not errors else "INVALID_WITNESS",
        "errors": sorted(set(errors)),
        "scope_witnesses": markers["SCOPE"],
        "event_witnesses": markers["EVENT"],
        "observed_cases": len(all_cases),
        "observed_plan_states": sum(
            item["observed_plan_states"] for item in cases.values()),
        "tagged_plan_states": sum(
            item["tags_dirty_states"] for item in cases.values()),
        "metadata_plan_states": sum(
            item["metadata_dirty_states"] for item in cases.values()),
        "opaque_collection_plan_states": sum(
            item["opaque_dirty_states"] for item in cases.values()),
        "cases_blocked": blocked,
        "unblocked_cases": [case for case in all_cases if case not in blocked],
        "previously_certified_changed_suffixes": sum(
            item["proved"] and item["changed"] for item in suffixes),
        "state_guarded_changed_suffix_cases": sorted(accepted) if not errors else [],
        "state_guarded_changed_suffix_count": len(accepted) if not errors else 0,
        "important_limit": (
            "A zero-counter witness rules out only inspected categories; "
            "Catalyst JSON still cannot certify all unobserved JVM state, "
            "external runtime state, or general Spark equivalence."),
    }


def score_state_witness(path, scope_summary):
    return audit_witness_stream(
        Path(path).read_text(errors="replace").splitlines(), scope_summary)
