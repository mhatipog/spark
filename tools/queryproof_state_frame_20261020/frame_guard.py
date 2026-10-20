"""Retrospective hidden-state frame preservation across Catalyst optimizer steps.

A previously certified post-FinishAnalysis suffix can pass this additional
guard only if actual JVM tags and output metadata remain stable, shared
attribute metadata is unchanged, and there are no opaque collections.
This is a rowbag-scoped conditional result, not whole-Spark equivalence.
"""
import base64
from collections import Counter
from pathlib import Path


def _decode(line, marker):
    fields = line.rstrip("\n").split("\t")
    if fields[0] != marker:
        raise ValueError("Unexpected frame marker")
    return [base64.b64decode(f, validate=True).decode("utf8") for f in fields[1:]]


def _marker(line):
    if line.startswith("QUERYPROOF_SCOPE\t"):
        f = _decode(line, "QUERYPROOF_SCOPE")
        if len(f) != 7:
            raise ValueError("Malformed scope")
        return ["SCOPE", *f[:6]], f[6]
    if line.startswith("QUERYPROOF_AST\t"):
        f = _decode(line, "QUERYPROOF_AST")
        if len(f) != 7:
            raise ValueError("Malformed rule event")
        return ["EVENT", *f[:5]], None
    return None


def parse_records(lines):
    records = []
    errors = []
    pending = None
    state = None
    for line in lines:
        try:
            parsed = _marker(line)
            if parsed:
                if pending is not None:
                    errors.append("MISSING_FRAME")
                pending = parsed
                state = None
                continue
            if line.startswith(("QUERYPROOF_SCOPE_ERROR\t", "QUERYPROOF_AST_ERROR\t")):
                errors.append("CATALYST_MARKER_ERROR")
            if line.startswith("QUERYPROOF_STATE\t"):
                if pending is None:
                    errors.append("STATE_ORPHAN")
                    continue
                fields = _decode(line, "QUERYPROOF_STATE")
                typ, *core = pending[0]
                expected = [core[0], typ, *core[1:]]
                count = 4 if typ == "SCOPE" else 8
                if fields[:len(expected)] != expected or len(fields) != len(expected) + count:
                    errors.append("STATE_CONTEXT_DISAGREEMENT")
                    pending = None
                    continue
                state = [int(v) for v in fields[len(expected):]]
                continue
            if line.startswith("QUERYPROOF_FRAME\t"):
                if pending is None or state is None:
                    errors.append("FRAME_ORPHAN")
                    continue
                typ, *core = pending[0]
                expected = [core[0], typ, *core[1:]]
                count = 4 if typ == "SCOPE" else 8
                fields = _decode(line, "QUERYPROOF_FRAME")
                if fields[:len(expected)] != expected or len(fields) != len(expected) + count:
                    errors.append("FRAME_CONTEXT_DISAGREEMENT")
                    pending = None
                    continue
                frames = []
                for start in range(len(expected), len(fields), 4):
                    tag, attr, out, conflict = fields[start:start + 4]
                    if conflict not in ("true", "false"):
                        raise ValueError("Invalid metadata conflict bit")
                    attributes = {}
                    for item in attr.split(",") if attr else []:
                        key, value = item.split(":", 1)
                        if key in attributes and attributes[key] != value:
                            raise ValueError("Contradictory metadata frame")
                        attributes[key] = value
                    frames.append({"tags": tag, "attrs": attributes,
                                   "output": out, "conflict": conflict == "true"})
                records.append({
                    "kind": typ, "core": core, "plan": pending[1],
                    "states": [state[i:i + 4] for i in range(0, len(state), 4)],
                    "frames": frames,
                })
                pending = None
                state = None
        except (ValueError, UnicodeDecodeError, IndexError) as exc:
            errors.append("FRAME_PARSE:" + exc.__class__.__name__)
            pending = None
            state = None
    if pending is not None:
        errors.append("MISSING_LAST_FRAME")
    return records, errors


def frame_change(left, right, exact_identity=False):
    reasons = []
    if left["conflict"] or right["conflict"]:
        reasons.append("INCONSISTENT_ATTRIBUTE_METADATA")
    if left["tags"] != right["tags"]:
        reasons.append("TAG_FRAME_CHANGED")
    if left["output"] != right["output"]:
        reasons.append("OUTPUT_METADATA_CHANGED")
    shared = set(left["attrs"]) & set(right["attrs"])
    if any(left["attrs"][key] != right["attrs"][key] for key in shared):
        reasons.append("SURVIVING_ATTRIBUTE_METADATA_CHANGED")
    if exact_identity and left["attrs"] != right["attrs"]:
        reasons.append("IDENTITY_ATTRIBUTE_FRAME_CHANGED")
    return reasons


def audit_frame_records(records, parse_errors, scope_summary):
    errors = list(parse_errors)
    stack = []
    suffix_results = []
    total_batch_checks = 0
    failing_batches = Counter()
    event_checks = 0
    event_failures = Counter()
    opaque_suffix_states = 0
    for r in records:
        kind = r["kind"]
        core = r["core"]
        case = core[0]
        if kind == "EVENT":
            event_checks += 1
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("EVENT_WITHOUT_BATCH")
                break
            reasons = frame_change(r["frames"][0], r["frames"][1])
            if any(stats[2] for stats in r["states"]):
                reasons.append("OPAQUE_EVENT_STATE")
            if reasons:
                for reason in reasons:
                    event_failures[reason] += 1
            stack[-1]["events"].append(reasons)
            continue
        _case, scope_kind, executor, batch, uid, phase = core
        if phase == "BEGIN":
            stack.append({
                "kind": scope_kind, "case": case, "executor": executor,
                "batch": batch, "uid": uid, "start": r, "children": [],
                "events": [], "has_finish": False, "finish_frame": None,
            })
            continue
        if not stack:
            errors.append("ORPHAN_END")
            break
        current = stack.pop()
        if any(current[k] != value for k, value in (
                ("kind", scope_kind), ("case", case), ("executor", executor),
                ("batch", batch), ("uid", uid))):
            errors.append("END_MISMATCH")
            break
        if current["kind"] == "BATCH":
            reasons = frame_change(
                current["start"]["frames"][0], r["frames"][0],
                current["start"]["plan"] == r["plan"])
            if any(stats[2] for stats in (current["start"]["states"][0], r["states"][0])):
                reasons.append("OPAQUE_BATCH_STATE")
            for ev in current["events"]:
                reasons.extend(ev)
            current["end"] = r
            current["reasons"] = sorted(set(reasons))
            if not stack or stack[-1]["kind"] != "EXECUTOR":
                errors.append("BATCH_WITHOUT_EXECUTOR")
                break
            parent = stack[-1]
            if parent["has_finish"]:
                current["post_finish"] = True
                total_batch_checks += 1
                for reason in current["reasons"]:
                    failing_batches[reason] += 1
                opaque_suffix_states += sum(
                    stats[2] > 0 for stats in (
                        current["start"]["states"][0], r["states"][0]))
            else:
                current["post_finish"] = False
            parent["children"].append(current)
            if current["batch"] == "Finish Analysis":
                if parent["has_finish"]:
                    errors.append("DUPLICATED_FINISH_ANALYSIS")
                parent["has_finish"] = True
                parent["finish_frame"] = r["frames"][0]
            continue
        if current["has_finish"]:
            suffix_batches = [
                c for c in current["children"] if c.get("post_finish")]
            reasons = [
                reason for c in suffix_batches for reason in c["reasons"]
            ]
            if not suffix_batches:
                errors.append("MISSING_POST_FINISH_BATCHES")
            last_frame = current["finish_frame"]
            for child in suffix_batches:
                boundary_reasons = frame_change(
                    last_frame, child["start"]["frames"][0], exact_identity=True)
                reasons.extend(boundary_reasons)
                last_frame = child["end"]["frames"][0]
            reasons.extend(frame_change(
                last_frame, r["frames"][0], exact_identity=True))
            if current["finish_frame"]["conflict"] or r["frames"][0]["conflict"]:
                reasons.append("BOUNDARY_ATTRIBUTE_CONFLICT")
            suffix_results.append({
                "case": case, "reasons": sorted(set(reasons)),
                "batch_count": len(suffix_batches),
            })
    if stack:
        errors.append("UNCLOSED_SCOPES")
    expected_scope_markers = scope_summary.get("scope_markers")
    expected_events = scope_summary.get("recorded_rule_events")
    actual_scopes = sum(r["kind"] == "SCOPE" for r in records)
    actual_events = sum(r["kind"] == "EVENT" for r in records)
    if actual_scopes != expected_scope_markers or actual_events != expected_events:
        errors.append("FRAME_TRACE_DENOMINATOR")
    expected_suffix = scope_summary.get("post_finish_analysis_optimizer_suffixes")
    if len(suffix_results) != expected_suffix:
        errors.append("OPTIMIZER_SUFFIX_DENOMINATOR")
    eligible = {
        item["case"] for item in scope_summary.get(
            "optimizer_suffix_case_counts", []) if item["proved"] and item["changed"]
    }
    preserved = [
        item["case"] for item in suffix_results
        if item["case"] in eligible and not item["reasons"]
    ]
    ready = not errors
    return {
        "status": "COMPLETE_FRAME_COMPARISON" if ready else "INVALID_FRAME_COMPARISON",
        "errors": sorted(set(errors)),
        "scope_records": actual_scopes,
        "rule_records": actual_events,
        "optimizer_suffixes": len(suffix_results),
        "postfinish_batch_checks": total_batch_checks,
        "frame_violating_batch_reasons": dict(failing_batches),
        "frame_violating_rule_reasons": dict(event_failures),
        "opaque_suffix_boundary_states": opaque_suffix_states,
        "originally_certified_changed_suffixes": len(eligible),
        "newly_frame_preserved_suffixes": sorted(preserved) if ready else [],
        "newly_frame_preserved_count": len(preserved) if ready else 0,
        "suffix_failure_reasons": dict(Counter(
            reason for item in suffix_results for reason in item["reasons"])),
        "suffix_failure_case_count": sum(bool(item["reasons"]) for item in suffix_results),
        "limits": [
            "Retrospective evidence, not fresh prospective confirmation",
            "Tag values hashed from JVM runtime toString representations",
            "Attribute metadata hashed using Metadata.json and ExprId",
            "Rulebag and optimizer-window claims are narrow and conditional",
            "Crypto fingerprint stability is observational rather than a proof of noninterference",
        ],
    }


def score_frame(path, scope_summary):
    records, errors = parse_records(
        Path(path).read_text(errors="replace").splitlines())
    return audit_frame_records(records, errors, scope_summary)
