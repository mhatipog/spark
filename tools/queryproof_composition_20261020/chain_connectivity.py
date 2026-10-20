"""Fail-closed composition of exactly adjacent local Catalyst certificates.

A collection of individually certified events is not a whole optimizer proof.
Only an ordered sequence where every event is certified AND the new plan AST
of each event equals the old plan AST of the next event may be composed.
Raw plans are never included in returned metrics.
"""
from collections import Counter, defaultdict
from pathlib import Path
import sys


def _same_provenance_adjacent(left, right):
    return (left["new"] == right["old"] and
            left.get("executor") == right.get("executor") and
            left.get("batch") == right.get("batch"))


def summarize_event_chain(events):
    """Same executor+batch, consecutive plan snapshots; no raw AST output."""
    by_case = defaultdict(list)
    for event in events:
        by_case[event["case"]].append(event)
    segments, by_rule, case_summaries = [], Counter(), []
    linked_pairs, adjacent_all, certified_total = 0, 0, 0
    cross_scope = Counter()
    for case, sequence in by_case.items():
        for left, right in zip(sequence, sequence[1:]):
            if (left["certified"] and right["certified"] and
                    left["new"] == right["old"]):
                if left.get("executor") != right.get("executor"):
                    scope = "different_executor"
                elif left.get("batch") != right.get("batch"):
                    scope = "same_executor_different_batch"
                else:
                    scope = "same_executor_same_batch"
                cross_scope[(left["rule"], right["rule"], scope,
                             str(left.get("executor")),
                             str(left.get("batch")),
                             str(right.get("executor")),
                             str(right.get("batch")))] += 1
        current = []
        all_certified = all(item["certified"] for item in sequence)
        all_connected = all(
            _same_provenance_adjacent(left, right)
            for left, right in zip(sequence, sequence[1:]))
        for idx, item in enumerate(sequence):
            if item["certified"]:
                certified_total += 1
                by_rule[item["rule"]] += 1
            if idx and _same_provenance_adjacent(sequence[idx - 1], item):
                adjacent_all += 1
            if item["certified"]:
                if current and _same_provenance_adjacent(sequence[idx - 1], item):
                    linked_pairs += 1
                    current.append(item)
                else:
                    if current:
                        segments.append({
                            "case": case, "length": len(current),
                            "rules": [x["rule"] for x in current]})
                    current = [item]
            else:
                if current:
                    segments.append({
                        "case": case, "length": len(current),
                        "rules": [x["rule"] for x in current]})
                current = []
        if current:
            segments.append({
                "case": case, "length": len(current),
                "rules": [x["rule"] for x in current]})
        case_summaries.append({
            "case": case,
            "events": len(sequence),
            "locally_certified": sum(e["certified"] for e in sequence),
            "whole_recorded_trace_certified": bool(sequence) and
            all_certified and all_connected,
        })
    strong = [s for s in segments if s["length"] >= 2]
    return {
        "status": "RETROSPECTIVE_LOCAL_CHAIN_CONNECTIVITY_ONLY",
        "recorded_events": len(events),
        "cases": len(by_case),
        "locally_certified_events": certified_total,
        "locally_certified_by_rule": dict(sorted(by_rule.items())),
        "adjacent_exact_ast_pairs_all_rules": adjacent_all,
        "adjacent_certified_pairs": linked_pairs,
        "exact_snapshot_certified_boundary_types": [
            {"source_rule": key[0], "target_rule": key[1],
             "scope_class": key[2],
             "source_executor": key[3], "source_batch": key[4],
             "target_executor": key[5], "target_batch": key[6],
             "count": count}
            for key, count in sorted(cross_scope.items())],
        "exact_snapshot_certified_pairs_any_scope": sum(cross_scope.values()),
        "maximal_certified_segments_length_ge_2": strong,
        "multi_edge_segments": len(strong),
        "max_certified_segment_length": max(
            (s["length"] for s in segments), default=0),
        "cases_with_two_edge_segment": len(set(s["case"] for s in strong)),
        "fully_connected_certified_recorded_case_traces": sum(
            c["whole_recorded_trace_certified"] for c in case_summaries),
        "per_case": case_summaries,
        "limits": [
            "Consecutive exact ASTs must share Catalyst executor and optimization batch",
            "Every intervening transition must be certified; UNKNOWN breaks chain",
            "Only recorded local events, not unobserved optimizer/instrumentation steps",
            "Analyzer and Optimizer phase wrappers are currently unproved",
            "Sort equivalence concerns unordered row bags, not physical ordering",
        ],
    }


def score_chain_trace(private_trace_path):
    root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(root / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(root / "tools" / "queryproof_instrumented"))
    sys.path.insert(0, str(root / "tools" / "queryproof_holdout_20261018"))
    from rank_ast import decode
    from run_holdout import _rule_basename
    from grouping_repetition_certificate import certify_grouping_repetition_event
    from generalized_projection_liveness_certificate import (
        certify_generalized_projection_liveness_event)
    from aggregate_framed_identity_certificate import (
        certify_aggregate_framed_identity_event)

    checks = {
        "RemoveRepetitionFromGroupExpressions": certify_grouping_repetition_event,
        "ColumnPruning": certify_generalized_projection_liveness_event,
        "CollapseProject": certify_aggregate_framed_identity_event,
    }
    events = []
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        evt = decode(line)
        rule = _rule_basename(evt.get("rule"))
        result = checks[rule](evt) if rule in checks else {"status": "UNKNOWN"}
        events.append({
            "case": evt.get("case", "unlabelled"),
            "rule": rule,
            "executor": evt.get("executor"),
            "batch": evt.get("batch"),
            "iteration": evt.get("iteration"),
            "old": evt["old"],
            "new": evt["new"],
            "certified": result["status"] == "EQUIVALENT_IN_FRAGMENT",
        })
    return summarize_event_chain(events)
