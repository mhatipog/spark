"""V7 strict chain ledger. Every recorded effective rule step must be certified.\nThis is retrospective structural accounting, not a semantic correctness proof.\n"""
"""Round V6 posthoc DEVELOPMENT boundary ledger (derived from frozen V5) (v5, join grouping + filters) for recorded Catalyst rule executions.

A batch is accepted only if its exact entry/exit states are connected by
individually certified, contiguous changed-rule steps, or identical by syntax.
An executor is accepted only through every directly enclosed batch boundary.
Raw ASTs never leave this module's private in-memory scoring pass.
"""
import base64
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys


def decode_scope(line):
    if not line.startswith("QUERYPROOF_SCOPE\t"):
        return None
    parts = line.rstrip("\n").split("\t")[1:]
    if len(parts) != 7:
        raise ValueError("Malformed scope marker")
    values = [base64.b64decode(part, validate=True).decode("utf-8")
              for part in parts]
    case, kind, executor, batch, uid, phase, encoded_plan = values
    plan = json.loads(encoded_plan)
    if kind not in ("BATCH", "EXECUTOR") or phase not in ("BEGIN", "END"):
        raise ValueError("Unknown scope kind or phase")
    if not isinstance(plan, list):
        raise ValueError("Scope AST must be a list")
    return {
        "type": "scope", "case": case, "kind": kind, "executor": executor,
        "batch": batch, "uid": int(uid), "phase": phase, "plan": plan,
    }


def _chain_proof(start, end, steps):
    if start == end and not steps:
        return True, "EXACT_NO_EVENT_IDENTITY"
    if not steps:
        return False, "UNRECORDED_DELTA"
    if not all(step["certified"] for step in steps):
        return False, "UNKNOWN_RULE"
    if steps[0]["old"] != start or steps[-1]["new"] != end:
        return False, "ENDPOINT_GAP"
    if any(a["new"] != b["old"] for a, b in zip(steps, steps[1:])):
        return False, "INTERMEDIATE_GAP"
    return True, "COMPLETE_CERTIFIED_CHAIN"


def summarize_scope_stream(stream):
    """Validate LIFO provenance, then certify batch and executor intervals."""
    stack = []
    completed = []
    errors = []
    event_count = 0
    for index, record in enumerate(stream):
        if record["type"] == "scope" and record["phase"] == "BEGIN":
            kind = record["kind"]
            parent = stack[-1]["kind"] if stack else None
            if ((kind == "EXECUTOR" and parent not in (None, "BATCH")) or
                    (kind == "BATCH" and parent != "EXECUTOR")):
                errors.append("BAD_SCOPE_NESTING")
                break
            if stack and record["case"] != stack[-1]["case"]:
                errors.append("INTERLEAVED_CASE")
                break
            entry = dict(record)
            entry["start"] = record["plan"]
            entry["direct_steps"] = []
            entry["children"] = []
            stack.append(entry)
            continue

        if record["type"] == "event":
            event_count += 1
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("EVENT_OUTSIDE_BATCH")
                break
            top = stack[-1]
            if (record["case"] != top["case"] or
                    record["executor"] != top["executor"] or
                    record["batch"] != top["batch"]):
                errors.append("EVENT_SCOPE_MISMATCH")
                break
            top["direct_steps"].append(record)
            continue

        if record["type"] != "scope" or not stack:
            errors.append("ORPHAN_SCOPE_END")
            break
        current = stack.pop()
        if any(current[key] != record[key]
               for key in ("case", "kind", "executor", "batch", "uid")):
            errors.append("SCOPE_END_MISMATCH")
            break
        if current["kind"] == "BATCH":
            proof, reason = _chain_proof(
                current["start"], record["plan"], current["direct_steps"])
        else:
            children = current["children"]
            if current["start"] == record["plan"] and not children:
                proof, reason = True, "EXACT_NO_CHILD_IDENTITY"
            elif not children:
                proof, reason = False, "EXECUTOR_MISSING_BATCH"
            elif not all(child["certified"] for child in children):
                proof, reason = False, "UNKNOWN_ENCLOSED_BATCH"
            else:
                steps = [{
                    "old": child["start"], "new": child["end"],
                    "certified": child["certified"],
                } for child in children]
                proof, reason = _chain_proof(
                    current["start"], record["plan"], steps)
        current["end"] = record["plan"]
        current["certified"] = proof
        current["reason"] = reason
        current["changed"] = current["start"] != current["end"]
        if stack:
            stack[-1]["children"].append(current)
        completed.append(current)

    if stack:
        errors.append("UNCLOSED_SCOPE")
    if errors:
        return {"status": "INVALID_TRACE", "errors": sorted(set(errors)),
                "recorded_rule_events": event_count}

    batch_scopes = [s for s in completed if s["kind"] == "BATCH"]
    executors = [s for s in completed if s["kind"] == "EXECUTOR"]
    batches = defaultdict(lambda: Counter())
    by_reason = Counter()
    unknown_rules = Counter()
    removed_shapes = Counter()
    added_shapes = Counter()
    by_case = defaultdict(lambda: Counter())
    for scope in completed:
        by_reason[(scope["kind"], scope["reason"])] += 1
        by_case[scope["case"]][scope["kind"] + "_changed"] += scope["changed"]
        if scope["certified"] and scope["changed"]:
            by_case[scope["case"]][scope["kind"] + "_nontrivial_proved"] += 1
        if scope["kind"] == "BATCH":
            for step in scope["direct_steps"]:
                if not step["certified"]:
                    key_rule = (scope["executor"], scope["batch"], step["rule"])
                    unknown_rules[key_rule] += 1
                    before = Counter(n.get("class", "").split(".")[-1]
                                     for n in step["old"] if isinstance(n, dict))
                    after = Counter(n.get("class", "").split(".")[-1]
                                    for n in step["new"] if isinstance(n, dict))
                    for name, count in (before - after).items():
                        removed_shapes[(scope["batch"], step["rule"], name)] += count
                    for name, count in (after - before).items():
                        added_shapes[(scope["batch"], step["rule"], name)] += count
            key = (scope["executor"], scope["batch"])
            batches[key]["total"] += 1
            batches[key]["changed"] += scope["changed"]
            batches[key]["proved_nontrivial"] += (
                scope["changed"] and scope["certified"])
            batches[key]["rule_events"] += len(scope["direct_steps"])
    suffixes = []
    for executor in executors:
        batches_in_executor = executor["children"]
        finish = [
            i for i, batch in enumerate(batches_in_executor)
            if batch["kind"] == "BATCH" and batch["batch"] == "Finish Analysis"
        ]
        if len(finish) != 1:
            continue
        baseline = batches_in_executor[finish[0]]["end"]
        later = batches_in_executor[finish[0] + 1:]
        steps = [{
            "old": batch["start"], "new": batch["end"],
            "certified": batch["certified"],
        } for batch in later]
        proof, reason = _chain_proof(baseline, executor["end"], steps)
        suffixes.append({
            "case": executor["case"],
            "changed": baseline != executor["end"],
            "proved": proof,
            "reason": reason,
            "changed_later_batches": sum(batch["changed"] for batch in later),
            "later_batch_diagnostics": [
                {"batch": batch["batch"], "changed": batch["changed"],
                 "certified": batch["certified"], "reason": batch["reason"]}
                for batch in later if batch["changed"] or not batch["certified"]
            ],
        })
    suffix_reasons = Counter(s["reason"] for s in suffixes)
    return {
        "status": "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER",
        "errors": [],
        "recorded_rule_events": event_count,
        "scope_markers": 2 * len(completed),
        "batch_scopes": len(batch_scopes),
        "executor_scopes": len(executors),
        "changed_batches": sum(s["changed"] for s in batch_scopes),
        "certified_changed_batches": sum(
            s["changed"] and s["certified"] for s in batch_scopes),
        "changed_executors": sum(s["changed"] for s in executors),
        "certified_changed_executors": sum(
            s["changed"] and s["certified"] for s in executors),
        "exact_identity_batches": sum(not s["changed"] for s in batch_scopes),
        "exact_identity_executors": sum(not s["changed"] for s in executors),
        "post_finish_analysis_optimizer_suffixes": len(suffixes),
        "changed_optimizer_suffixes": sum(s["changed"] for s in suffixes),
        "certified_changed_optimizer_suffixes": sum(
            s["changed"] and s["proved"] for s in suffixes),
        "optimizer_suffix_reason_counts": dict(sorted(suffix_reasons.items())),
        "optimizer_suffix_case_counts": [s for s in suffixes],
        "by_reason": {kind + ":" + reason: count
                      for (kind, reason), count in sorted(by_reason.items())},
        "unknown_rules": [
            {"executor": key[0], "batch": key[1], "rule": key[2], "count": count}
            for key, count in sorted(unknown_rules.items())],
        "unknown_rule_shape_changes": {
            direction: [
                {"batch": key[0], "rule": key[1], "node_class": key[2],
                 "count": count} for key, count in sorted(counter.items())]
            for direction, counter in (("removed", removed_shapes),
                                       ("added", added_shapes))},
        "by_batch": [{
            "executor": key[0], "batch": key[1], **dict(value)
        } for key, value in sorted(batches.items())],
        "case_counts": {
            case: dict(counts) for case, counts in sorted(by_case.items())
        },
        "limits": [
            "Experimental trace only, not a guarantee for all Spark programs",
            "Scope start/end are emitted by a disposable instrumented RuleExecutor",
            "Changed rule records are complete only if no cap or trace errors",
            "Equal AST endpoints are allowed only when zero effective events are recorded",
            "A zero-event record is trustworthy only when event-capture completeness is established",
            "Nested executor changes must be covered by the enclosing parent rule",
            "No Spark order sensitivity, side effects, or non-Catalyst stages proved",
            "Optimizer suffix begins AFTER FinishAnalysis semantic normalization",
        ],
    }


def score_scope_trace(private_trace_path):
    root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(root / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(root / "tools" / "queryproof_instrumented"))
    sys.path.insert(0, str(root / "tools" / "queryproof_holdout_20261018"))
    from rank_ast import decode
    from run_holdout import _rule_basename
    from grouping_repetition_certificate import certify_grouping_repetition_event
    from generalized_liveness_v2 import (
        certify_generalized_projection_liveness_event)
    from aggregate_identity_v2 import (
        certify_aggregate_framed_identity_event)
    from join_project_v6 import certify_join_pruning_event
    from join_grouping_v1 import certify_join_group_dedup_event
    from filter_steps_v1 import (certify_inferred_filter_event,
                                 certify_filter_project_push_event)

    def column_pruning_or_join(event):
        ordinary = certify_generalized_projection_liveness_event(event)
        if ordinary["status"] == "EQUIVALENT_IN_FRAGMENT":
            return ordinary
        return certify_join_pruning_event(event)

    def grouping_or_join(event):
        original = certify_grouping_repetition_event(event)
        if original["status"] == "EQUIVALENT_IN_FRAGMENT":
            return original
        return certify_join_group_dedup_event(event)

    checks = {
        "RemoveRepetitionFromGroupExpressions": grouping_or_join,
        "ColumnPruning": column_pruning_or_join,
        "CollapseProject": certify_aggregate_framed_identity_event,
        "InferFiltersFromConstraints": certify_inferred_filter_event,
        "PushDownPredicates": certify_filter_project_push_event,
    }
    stream = []
    marker_errors = []
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if line.startswith("QUERYPROOF_SCOPE_ERROR\t"):
            marker_errors.append("SCOPE_SERIALIZATION_FAILURE")
        elif line.startswith("QUERYPROOF_AST_ERROR\t"):
            marker_errors.append("RULE_SERIALIZATION_FAILURE")
        elif line.startswith("QUERYPROOF_SCOPE\t"):
            stream.append(decode_scope(line))
        elif line.startswith("QUERYPROOF_AST\t"):
            event = decode(line)
            rule = _rule_basename(event["rule"])
            result = checks[rule](event) if rule in checks else {"status": "UNKNOWN"}
            stream.append({
                "type": "event", "case": event["case"], "executor": event["executor"],
                "batch": event["batch"], "old": event["old"], "new": event["new"],
                "rule": rule, "certified": result["status"] == "EQUIVALENT_IN_FRAGMENT",
            })
    if marker_errors:
        return {"status": "INVALID_TRACE", "errors": sorted(set(marker_errors))}
    return summarize_scope_stream(stream)
