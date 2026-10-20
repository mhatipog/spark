"""Score actual all-rule JVM metadata snapshots without exporting tag values."""
import base64
from pathlib import Path
import sys


def decode(line):
    parts = line.split("\t")
    if parts[0] != "QUERYPROOF_RULE_AUDIT" or len(parts) != 10:
        raise ValueError("RULE_AUDIT_FIELD_COUNT")
    values = [base64.b64decode(v, validate=True).decode() for v in parts[1:]]
    if any(x not in ("true", "false") for x in values[5:]):
        raise ValueError("INVALID_BOOLEAN_FLAGS")
    return {"case": values[0], "executor": values[1],
            "batch": values[2], "rule": values[3], "iteration": values[4],
            "effective": values[5] == "true",
            "state_changed": values[6] == "true",
            "frame_changed": values[7] == "true",
            "input_changed": values[8] == "true"}


def check(path):
    lines = Path(path).read_text(errors="replace").splitlines()
    if any(x.startswith(("QUERYPROOF_RULE_AUDIT_ERROR\t",
                         "QUERYPROOF_AST_ERROR\t",
                         "QUERYPROOF_SCOPE_ERROR\t")) for x in lines):
        raise AssertionError("OBSERVER_ERROR")
    accounts = [decode(line) for line in lines
                if line.startswith("QUERYPROOF_RULE_AUDIT\t")]
    by_case = {item["case"]: item for item in accounts}
    if len(accounts) != 2 or len(by_case) != 2:
        raise AssertionError("INCOMPLETE_REAL_RULE_INVOCATIONS")
    mutant = by_case["silent-mutant"]
    benign = by_case["silent-benign"]
    if mutant["effective"] or not mutant["state_changed"]:
        raise AssertionError("SILENT_MUTATION_UNDETECTED")
    if not mutant["frame_changed"] or mutant["input_changed"]:
        raise AssertionError("WRONG_HIDDEN_STATE_CAUSE")
    if benign["effective"] or benign["state_changed"] or benign["input_changed"]:
        raise AssertionError("BENIGN_RULE_FALSE_ALARM")
    ast = [line for line in lines if line.startswith("QUERYPROOF_AST\t")]
    if ast:
        raise AssertionError("CONTROL_MUST_BE_INVISIBLE_TO_ORIGINAL_AST_LOG")
    return accounts


if __name__ == "__main__":
    records = check(sys.argv[1])
    print("QUERYPROOF_UNOBSERVED_FASTEQUALS_TAG_MUTATION=CONFIRMED")
    print("QUERYPROOF_ALL_RULE_OBSERVER_DETECTS_SILENT_MUTATION=CONFIRMED")
    print("QUERYPROOF_BENIGN_SILENT_RULE_HAS_NO_FALSE_ALARM=CONFIRMED")
    print("QUERYPROOF_RULE_AUDIT_COUNTS=" + str(len(records)))
