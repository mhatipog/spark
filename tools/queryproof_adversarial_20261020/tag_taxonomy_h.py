"""H cohort tag census with hashed names and type-only JVM evidence.

No tag values/SQL literals leave the disposable private trace. Known keys are
matched against a checked Spark v4.2.0 source allowlist. This inventory is
NOT proof that tags are inert or that tag mutation cannot affect optimization.
"""
import base64
from collections import Counter
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
KNOWN_NAMES = (
    "dataset_id", "plan_id", "is_metadata_col", "processedJoinCondition",
    "ROOT_REPARTITION", "FAILED_TO_EVALUATE", "grandTotalAggregate",
    "functionAliasName", "collationContext", "wholeStageCodegenId",
    "project_for_expression_id_deduplication", "single_pass_subtree_boundary",
    "single_pass_is_lca", "top_level_operator", "single_pass_analysis_marker",
    "dataTypeMismatchError", "by_ctas", "logical_plan",
    "logical_plan_inherited", "by_table_insertion", "user_specified_cast",
    "single_pass_sql_string_override",
    "single_pass_outer_aggregate_alias_name_override",
    "temp_logical_plan", "by_table_argument",
)
KEYS = {hashlib.sha256(name.encode()).hexdigest(): name for name in KNOWN_NAMES}


def decode(line):
    parts = line.split("\t")
    return [base64.b64decode(part, validate=True).decode()
            for part in parts[1:]]


def parse_taxonomy(values):
    summary = Counter()
    types = Counter()
    if not values:
        return summary, types
    for atom in values.split(";"):
        parts = atom.rsplit(":", 2)
        if len(parts) != 3:
            raise ValueError("MALFORMED_TAG_TYPE_DESCRIPTOR")
        key_hash, typ, raw_count = parts
        if len(key_hash) != 64 or any(c not in "0123456789abcdef" for c in key_hash):
            raise ValueError("INVALID_TAG_KEY_HASH")
        if not raw_count.isdecimal() or int(raw_count) < 1:
            raise ValueError("INVALID_TAG_COUNT")
        number = int(raw_count)
        key = KEYS.get(key_hash, "UNRECOGNIZED_KEY")
        summary[key] += number
        types[(key, typ)] += number
    return summary, types


def read_bound_records(path):
    records = []
    errors = []
    awaiting = None
    for line in Path(path).read_text(errors="replace").splitlines():
        try:
            if line.startswith("QUERYPROOF_INPUT\t"):
                if awaiting is not None:
                    errors.append("PREVIOUS_TAG_RECORD_MISSING")
                fields = decode(line)
                if len(fields) < 8 or fields[1] not in ("EVENT", "SCOPE"):
                    raise ValueError("INPUT_CONTEXT_INVALID")
                awaiting = fields[:- (2 if fields[1] == "EVENT" else 1)]
                continue
            if line.startswith("QUERYPROOF_TAG_TAXONOMY\t"):
                fields = decode(line)
                if awaiting is None:
                    errors.append("ORPHAN_TAG_TAXONOMY")
                    continue
                if fields[:len(awaiting)] != awaiting:
                    errors.append("TAG_CONTEXT_DISAGREEMENT")
                    awaiting = None
                    continue
                plan_count = 2 if fields[1] == "EVENT" else 1
                if len(fields) != len(awaiting) + plan_count:
                    errors.append("TAG_PLAN_CARDINALITY")
                    awaiting = None
                    continue
                tag_summaries = [parse_taxonomy(item) for item in fields[len(awaiting):]]
                records.append({
                    "case": fields[0], "kind": fields[1],
                    "context": fields[2:len(awaiting)],
                    "tags": tag_summaries,
                })
                awaiting = None
        except (ValueError, UnicodeDecodeError, IndexError, TypeError):
            errors.append("TAG_TAXONOMY_PARSE_FAILURE")
            awaiting = None
    if awaiting is not None:
        errors.append("UNPAIRED_FINAL_TAG_TAXONOMY")
    return records, sorted(set(errors))


def score_taxonomy(path, scope_summary):
    records, errors = read_bound_records(path)
    n_events = sum(r["kind"] == "EVENT" for r in records)
    n_scopes = sum(r["kind"] == "SCOPE" for r in records)
    if n_events != scope_summary.get("recorded_rule_events"):
        errors.append("RULE_RECORD_DENOMINATOR")
    if n_scopes != scope_summary.get("scope_markers"):
        errors.append("SCOPE_RECORD_DENOMINATOR")
    stack = []
    finished = Counter()
    tagged_suffix_cases = set()
    counts = Counter()
    typecounts = Counter()
    suffixcounts = Counter()
    suffixtypes = Counter()
    unknown_suffix = set()
    for r in records:
        case = r["case"]
        if r["kind"] == "SCOPE":
            kind, executor, batch, uid, phase = r["context"]
            if phase == "BEGIN":
                stack.append({"case": case, "kind": kind, "executor": executor,
                              "batch": batch, "uid": uid, "finished": False})
            elif phase == "END":
                if not stack:
                    errors.append("ORPHAN_SCOPE_END")
                    break
                entry = stack.pop()
                if any(entry[key] != value for key, value in (
                        ("case", case), ("kind", kind), ("executor", executor),
                        ("batch", batch), ("uid", uid))):
                    errors.append("SCOPE_CONTEXT_MISMATCH")
                    break
                if kind == "BATCH" and batch == "Finish Analysis":
                    if not stack or stack[-1]["kind"] != "EXECUTOR":
                        errors.append("MISSING_FINISH_PARENT")
                        break
                    stack[-1]["finished"] = True
                    finished[case] += 1
        else:
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("EVENT_OUTSIDE_BATCH")
                break
            if r["context"][:2] != [stack[-1]["executor"], stack[-1]["batch"]]:
                errors.append("EVENT_CONTEXT_MISMATCH")
                break
        after_finish = any(x["kind"] == "EXECUTOR" and x["finished"] for x in stack)
        for summary, types in r["tags"]:
            counts.update(summary)
            typecounts.update(types)
            if after_finish or (r["kind"] == "SCOPE" and
                                r["context"][2] == "Finish Analysis" and
                                r["context"][4] == "END"):
                suffixcounts.update(summary)
                suffixtypes.update(types)
                if summary:
                    tagged_suffix_cases.add(case)
                if summary.get("UNRECOGNIZED_KEY", 0):
                    unknown_suffix.add(case)
    if stack:
        errors.append("UNCLOSED_SCOPES")
    if len(finished) != scope_summary.get("post_finish_analysis_optimizer_suffixes"):
        errors.append("OPTIMIZER_CUT_DENOMINATOR")
    candidates = {c["case"] for c in
                  scope_summary.get("optimizer_suffix_case_counts", [])
                  if c["changed"] and c["proved"]}
    if errors:
        candidates = set()
    return {
        "status": "COMPLETE_HASHED_TAG_CENSUS" if not errors else "INVALID_TAG_CENSUS",
        "errors": sorted(set(errors)),
        "rule_records": n_events,
        "scope_records": n_scopes,
        "postfinish_case_count": len(finished),
        "tagged_suffix_cases": len(tagged_suffix_cases),
        "candidate_cases_with_observed_tags": len(candidates & tagged_suffix_cases),
        "candidate_cases_with_unrecognized_tag_names": len(candidates & unknown_suffix),
        "recognized_tag_occurrences": {k: v for k, v in sorted(suffixcounts.items())
                                       if k != "UNRECOGNIZED_KEY"},
        "unrecognized_tag_occurrences": suffixcounts["UNRECOGNIZED_KEY"],
        "recognized_tag_value_types": [
            {"name": k, "jvm_value_type": t, "occurrences": v}
            for (k, t), v in sorted(suffixtypes.items()) if k != "UNRECOGNIZED_KEY"],
        "overall_known_key_occurrences": {k: v for k, v in sorted(counts.items())
                                          if k != "UNRECOGNIZED_KEY"},
        "limits": [
            "Tags are counted at captured states; repeated snapshots are not independent tags",
            "Known-name catalog derived from Spark v4.2.0 static source search",
            "Unknown keys are deliberately not revealed or accepted",
            "This is a retrospective observational inventory, never a proof of noninterference",
        ],
    }


def main():
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))
    import run_frame
    original = run_frame.score_frame

    def audit(path, scopes):
        result = original(path, scopes)
        result["v6_tag_type_census"] = score_taxonomy(path, scopes)
        return result

    run_frame.score_frame = audit
    return run_frame.main("H")


if __name__ == "__main__":
    raise SystemExit(main())
