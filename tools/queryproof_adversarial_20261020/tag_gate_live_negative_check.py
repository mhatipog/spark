"""Fail-closed, end-to-end live Spark tag census tests.

Privacy rule: print only fixed status strings and aggregate counts. Never print
tag values, raw marker payloads, plans or unknown tag keys.
"""
from collections import Counter
from pathlib import Path
import sys

from tag_taxonomy_h import dataset_tag_eligible, read_bound_records

EXPECTED = {
    "tag-valid-long": (True, ("dataset_id", "scala.collection.mutable.HashSet",
                              "all-long-elements")),
    "tag-invalid-member": (False, ("dataset_id", "scala.collection.mutable.HashSet",
                                  "non-long-elements")),
    "tag-processed-join": (False, ("processedJoinCondition", "java.lang.String",
                                  "not-examined")),
    "tag-unknown-key": (False, ("UNRECOGNIZED_KEY", "java.lang.String",
                               "not-examined")),
}


def main(stderr_path, stdout_path):
    stdout = Path(stdout_path).read_text(errors="replace").splitlines()
    records, errors = read_bound_records(stderr_path)
    if errors:
        raise ValueError("LIVE_TAG_CONTEXT_INTEGRITY_FAILURE")
    result_counts = Counter()
    for record in records:
        case = record["case"]
        if case not in EXPECTED:
            continue
        if len(record["tags"]) != 1:
            raise ValueError("UNEXPECTED_LIVE_SCOPE_PLAN_COUNT")
        summary, forms = record["tags"][0]
        expected_allow, expected_form = EXPECTED[case]
        if set(forms) != {expected_form}:
            raise ValueError("INCORRECT_LIVE_JVM_TAG_CLASSIFICATION")
        if sum(forms.values()) != 1:
            raise ValueError("INCORRECT_TAG_INSTANCE_COUNT")
        if dataset_tag_eligible(forms) != expected_allow:
            raise ValueError("LIVE_TAG_ALLOWLIST_FALSE_POSITIVE_OR_NEGATIVE")
        result_counts[case] += 1
    for case in EXPECTED:
        if f"QUERYPROOF_LIVE_TAG_FIXTURE_EXECUTED={case}" not in stdout:
            raise ValueError("MISSING_ACTUAL_SPARK_FIXTURE")
        if result_counts[case] != 4:
            raise ValueError("INCOMPLETE_SCOPE_TAG_CENSUS")
    print("QUERYPROOF_JVM_TAG_GATE_LIVE_POSITIVE=PASS")
    print("QUERYPROOF_JVM_TAG_GATE_LIVE_NEGATIVE_CONTROLS=3_PASS")
    print("QUERYPROOF_JVM_TAG_SCOPE_CONTEXT=16_COMPLETE")
    print("QUERYPROOF_JVM_TAG_VALUES_REDACTED=YES")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: checker stderr-file stdout-file")
    main(sys.argv[1], sys.argv[2])
