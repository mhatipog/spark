"""Retrospective audit of every RuleExecutor invocation, not only changed AST events.

Observe fastEquals no-ops for hidden-state drift. This diagnoses trace
completeness; tags are NOT presumed semantically inert from stable hashes.
"""
import base64
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_adversarial_20261020"))
from check_silent_rule_audit import decode as decode_account
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
from scope_ledger import decode_scope


def audit_all_rules(path, scopes):
    stack = []
    errors = []
    counts = Counter()
    suffix_counts = Counter()
    suffix_silent_cases = set()
    matched_cuts = Counter()
    recorded = 0
    for line in Path(path).read_text(errors="replace").splitlines():
        if line.startswith("QUERYPROOF_RULE_AUDIT_ERROR\t"):
            errors.append("RULE_AUDIT_INSTRUMENTATION_FAILURE")
            continue
        if line.startswith("QUERYPROOF_RULE_AUDIT\t"):
            try:
                record = decode_account(line)
            except (ValueError, IndexError, UnicodeDecodeError) as exc:
                errors.append("MALFORMED_RULE_ACCOUNT:" + str(exc))
                continue
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("RULE_ACCOUNT_OUTSIDE_SCOPE")
                break
            active = stack[-1]
            if (active["case"] != record["case"] or
                    active["executor"] != record["executor"] or
                    active["batch"] != record["batch"]):
                errors.append("RULE_ACCOUNT_CONTEXT_DISAGREEMENT")
                break
            recorded += int(record["effective"])
            counts["all_rule_calls"] += 1
            counts["effective_calls"] += int(record["effective"])
            counts["fast_equals_noop_calls"] += int(not record["effective"])
            silent = not record["effective"] and record["state_changed"]
            counts["silent_observed_hidden_drift"] += int(silent)
            counts["silent_input_change"] += int(
                not record["effective"] and record["input_changed"])
            post = any(scope["kind"] == "EXECUTOR" and scope["finished"]
                       for scope in stack)
            if post:
                suffix_counts["calls"] += 1
                suffix_counts["effective"] += int(record["effective"])
                suffix_counts["noops"] += int(not record["effective"])
                suffix_counts["silent_drift"] += int(silent)
                if silent:
                    suffix_silent_cases.add(record["case"])
            continue
        if line.startswith("QUERYPROOF_SCOPE\t"):
            try:
                r = decode_scope(line)
            except (ValueError, IndexError, UnicodeDecodeError) as exc:
                errors.append("MALFORMED_SCOPE:" + str(exc))
                break
            if r["phase"] == "BEGIN":
                stack.append({key: r[key] for key in
                              ("case", "kind", "executor", "batch", "uid")})
                stack[-1]["finished"] = False
            elif r["phase"] == "END":
                if not stack:
                    errors.append("ORPHAN_SCOPE_END")
                    break
                prior = stack.pop()
                if any(prior[key] != r[key] for key in
                       ("case", "kind", "executor", "batch", "uid")):
                    errors.append("SCOPE_STACK_MISMATCH")
                    break
                if r["kind"] == "BATCH" and r["batch"] == "Finish Analysis":
                    if not stack or stack[-1]["kind"] != "EXECUTOR":
                        errors.append("FINISH_ANALYSIS_PARENT_MISSING")
                        break
                    stack[-1]["finished"] = True
                    matched_cuts[r["case"]] += 1
    if stack:
        errors.append("UNCLOSED_SCOPE")
    if recorded != scopes.get("recorded_rule_events"):
        errors.append("EFFECTIVE_EVENT_DENOMINATOR")
    if len(matched_cuts) != scopes.get("post_finish_analysis_optimizer_suffixes"):
        errors.append("SUFFIX_CUT_DENOMINATOR")
    if any(count != 1 for count in matched_cuts.values()):
        errors.append("DUPLICATE_NORMALIZATION_CUT")
    originals = {r["case"] for r in scopes.get("optimizer_suffix_case_counts", [])
                 if r["proved"] and r["changed"]}
    if errors:
        originals_without_silent = []
    else:
        originals_without_silent = sorted(originals - suffix_silent_cases)
    return {
        "status": "COMPLETE_ALL_RULE_AUDIT" if not errors else "INVALID_ALL_RULE_AUDIT",
        "errors": sorted(set(errors)),
        "all_rule_invocations": dict(counts),
        "postfinish_rule_invocations": dict(suffix_counts),
        "optimizer_suffixes": len(matched_cuts),
        "original_conditional_candidates": len(originals),
        "candidates_without_observed_silent_drift": len(originals_without_silent),
        "cases_with_postfinish_observed_silent_drift": len(suffix_silent_cases),
        "limitations": [
            "Post-hoc H evidence only, not a fresh prospective cohort",
            "State/frame hashing remains noninjective and cannot prove no mutation",
            "A rule can temporarily mutate and restore state between snapshots",
            "All-rule audit only applies to RuleExecutor and logical plan inputs",
            "Absence of observed hidden drift does not prove tag noninterference",
        ],
    }


def main():
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
    import run_frame
    previous = run_frame.score_frame

    def score(path, scopes):
        result = previous(path, scopes)
        result["v5_all_rule_accounting"] = audit_all_rules(path, scopes)
        return result

    run_frame.score_frame = score
    return run_frame.main("H")


if __name__ == "__main__":
    raise SystemExit(main())
