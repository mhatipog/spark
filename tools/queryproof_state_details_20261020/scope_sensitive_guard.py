"""Localize hidden Catalyst state to the actual post-FinishAnalysis suffix.

Independent of the frozen K verifier. A category is reported only by JVM
class name; metadata contents, SQL literals and ASTs are never returned.
"""
import base64
from collections import Counter, defaultdict
from pathlib import Path

from state_guard import audit_witness_stream, _decode_fields, _expected_marker


def audit_details(lines, scope_summary):
    old = audit_witness_stream(lines, scope_summary)
    pending = None
    state = None
    errors = []
    global_classes = Counter()
    suffix_classes = Counter()
    optimizer_cases = defaultdict(lambda: Counter())
    suffix_by_case = defaultdict(lambda: Counter())
    active_case = None
    after_finish = False
    marker_count = 0
    for line in lines:
        try:
            next_marker = _expected_marker(line)
            if next_marker is not None:
                if pending is not None:
                    errors.append("MISSING_CLASS_PROFILE")
                pending = next_marker
                state = None
                if (pending[1] == "SCOPE" and pending[2] == "EXECUTOR" and
                        pending[6] == "BEGIN"):
                    if active_case is not None:
                        errors.append("NESTED_OPTIMIZER_UNSUPPORTED")
                    active_case = pending[0]
                    after_finish = False
                continue
            if line.startswith("QUERYPROOF_STATE\t"):
                if pending is None:
                    errors.append("STATE_WITHOUT_MARKER")
                    continue
                parts = _decode_fields(line, "QUERYPROOF_STATE")
                if parts[:len(pending)] != pending:
                    errors.append("STATE_CONTEXT_MISMATCH")
                    continue
                width = 4 if pending[1] == "SCOPE" else 8
                if len(parts) != len(pending) + width:
                    errors.append("STATE_FIELD_COUNT")
                    continue
                state = [int(x) for x in parts[len(pending):]]
                continue
            if not line.startswith("QUERYPROOF_STATE_CLASS\t"):
                continue
            if pending is None or state is None:
                errors.append("ORPHAN_CLASS_PROFILE")
                continue
            fields = _decode_fields(line, "QUERYPROOF_STATE_CLASS")
            number = 1 if pending[1] == "SCOPE" else 2
            if fields[:len(pending)] != pending or len(fields) != len(pending) + number:
                errors.append("CLASS_PROFILE_IDENTITY_MISMATCH")
                pending = None
                continue
            profiles = fields[len(pending):]
            marker_count += 1
            is_suffix = active_case == pending[0] and after_finish
            for profile in profiles:
                if profile:
                    for item in profile.split(","):
                        key, raw_count = item.rsplit("=", 1)
                        amount = int(raw_count)
                        if amount < 0 or not key.startswith(("T:", "M:", "Q:")):
                            errors.append("BAD_CLASS_CATEGORY")
                            continue
                        global_classes[key] += amount
                        if is_suffix:
                            suffix_classes[key] += amount
            if is_suffix:
                target = suffix_by_case[pending[0]]
                target["observed_plan_states"] += number
                for start in range(0, len(state), 4):
                    for key, value in zip(("tags", "metadata", "opaque"),
                                          state[start:start + 3]):
                        if value:
                            target[key + "_dirty_states"] += 1
                optimizer_cases[pending[0]]["suffix_markers"] += 1
            if (pending[1] == "SCOPE" and pending[2] == "BATCH" and
                    pending[4] == "Finish Analysis" and pending[6] == "END" and
                    active_case == pending[0]):
                after_finish = True
                optimizer_cases[pending[0]]["finish_analysis_seen"] += 1
            if (pending[1] == "SCOPE" and pending[2] == "EXECUTOR" and
                    pending[6] == "END"):
                if active_case != pending[0]:
                    errors.append("EXECUTOR_BOUNDARY_MISMATCH")
                active_case = None
                after_finish = False
            pending = None
            state = None
        except (ValueError, UnicodeDecodeError, IndexError) as exc:
            errors.append("CLASS_PROFILE_PARSE_ERROR:" + exc.__class__.__name__)
            pending = None
            state = None
    if pending is not None:
        errors.append("MISSING_FINAL_CLASS_PROFILE")
    if active_case is not None:
        errors.append("UNFINISHED_EXECUTOR")
    seen_finishes = sum(x["finish_analysis_seen"] for x in optimizer_cases.values())
    expected_finishes = scope_summary.get("post_finish_analysis_optimizer_suffixes")
    if expected_finishes is None or seen_finishes != expected_finishes:
        errors.append("FINISH_ANALYSIS_DENOMINATOR_MISMATCH")
    if expected_finishes and not suffix_by_case:
        errors.append("NO_POST_NORMALIZATION_WITNESSES")
    if marker_count != old["scope_witnesses"] + old["event_witnesses"]:
        errors.append("PROFILE_DENOMINATOR_MISMATCH")
    baseline = [
        value for value in scope_summary.get("optimizer_suffix_case_counts", [])
        if value["proved"] and value["changed"]
    ]
    accepted = [
        item["case"] for item in baseline
        if not any(suffix_by_case[item["case"]][k + "_dirty_states"]
                   for k in ("tags", "metadata", "opaque"))
    ]
    complete = not errors and old["status"] == "COMPLETE_JVM_WITNESSES"
    return {
        "status": "COMPLETE_CLASSIFIED_JVM_TRACE" if complete else "INVALID_DETAIL_TRACE",
        "errors": sorted(set(errors) | set(old["errors"])),
        "cases_with_finish_analysis": len([
            x for x in optimizer_cases.values() if x["finish_analysis_seen"]
        ]),
        "observed_class_markers": marker_count,
        "suffix_states": sum(x["observed_plan_states"]
                             for x in suffix_by_case.values()),
        "suffix_tagged_states": sum(x["tags_dirty_states"]
                                    for x in suffix_by_case.values()),
        "suffix_metadata_states": sum(x["metadata_dirty_states"]
                                      for x in suffix_by_case.values()),
        "suffix_opaque_states": sum(x["opaque_dirty_states"]
                                    for x in suffix_by_case.values()),
        "top_global_hidden_classes": global_classes.most_common(18),
        "top_suffix_hidden_classes": suffix_classes.most_common(18),
        "suffix_guard_blockers_by_case": {
            case: [key for key in ("tags", "metadata", "opaque")
                   if counts[key + "_dirty_states"]]
            for case, counts in sorted(suffix_by_case.items())
            if any(counts[key + "_dirty_states"]
                   for key in ("tags", "metadata", "opaque"))
        },
        "previously_certified_suffix_count": len(baseline),
        "clean_suffix_cases": sorted(accepted) if complete else [],
        "clean_suffix_count": len(accepted) if complete else 0,
        "limit": (
            "Class-level diagnosis, not a full JVM state equality witness. "
            "Unknown state and unexamined serialized field kinds remain out of scope."),
    }


def score_details(path, scope_summary):
    return audit_details(
        Path(path).read_text(errors="replace").splitlines(), scope_summary)
