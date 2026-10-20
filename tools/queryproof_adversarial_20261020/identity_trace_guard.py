"""Fail-closed validation of INPUT witnesses bound to real Catalyst RuleExecutor logs.

No credentials or plan literals are exported. This checks trace completeness and
backing input identity only; an unchanged backing input does not itself prove
that a logical rewrite is equivalent.
"""
import base64
import json
from collections import Counter
from pathlib import Path
import sys

SUPPORTED_RDD = "org.apache.spark.sql.execution.LogicalRDD"
SUPPORTED_ONE_ROW = "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation"
KINDS = ("QUERYPROOF_AST", "QUERYPROOF_SCOPE")
PARTS = ("QUERYPROOF_STATE", "QUERYPROOF_FRAME", "QUERYPROOF_INPUT")


def decode(line, kind):
    parts = line.split("\t")
    if parts[0] != kind:
        raise ValueError("Unexpected marker")
    return [base64.b64decode(item, validate=True).decode("utf-8")
            for item in parts[1:]]


def plan_leaves(plan):
    if not isinstance(plan, list):
        raise ValueError("Malformed Catalyst plan")
    return [node for node in plan if isinstance(node, dict) and
            node.get("num-children") == 0]


def interpret_witness(plan, string):
    leaves = plan_leaves(plan)
    entries = [] if not string else string.split("~")
    if len(entries) != len(leaves):
        return "INCOMPLETE_LEAF_BINDING", ()
    witnesses = []
    for leaf, entry in zip(leaves, entries):
        if entry.count("|") != 1:
            return "MALFORMED_LEAF_BINDING", ()
        cls, token = entry.split("|", 1)
        if cls != leaf.get("class"):
            return "LEAF_CLASS_MISMATCH", ()
        if cls == SUPPORTED_RDD:
            if not token.startswith("rdd#") or not token[4:].isdigit() or int(token[4:]) <= 0:
                return "INVALID_RDD_REFERENCE", ()
        elif cls == SUPPORTED_ONE_ROW:
            if token != "intrinsic#one-row":
                return "INVALID_INTRINSIC_LEAF", ()
        else:
            return "UNKNOWN_RELATION_SEMANTICS", ()
        witnesses.append((cls, token))
    return "COMPLETE", tuple(witnesses)


def finalize(pending, input_fields):
    kind = pending["kind"]
    core = pending["core"]
    expect = [core[0], "EVENT" if kind == "QUERYPROOF_AST" else "SCOPE", *core[1:]]
    if input_fields[:len(expect)] != expect:
        raise ValueError("INPUT_CONTEXT_MISMATCH")
    bindings = input_fields[len(expect):]
    if len(bindings) != len(pending["plans"]):
        raise ValueError("INPUT_PLAN_CARDINALITY")
    identities = [interpret_witness(plan, witness)
                  for plan, witness in zip(pending["plans"], bindings)]
    failures = [reason for reason, _ in identities if reason != "COMPLETE"]
    if failures:
        verdict = "UNKNOWN"
        reason = failures[0]
    elif len(identities) == 2:
        verdict = "PRESERVED" if identities[0][1] == identities[1][1] else "UNKNOWN"
        reason = "SAME_INPUT_SEQUENCE" if verdict == "PRESERVED" else "INPUT_REFERENCE_CHANGED"
    else:
        verdict = "SNAPSHOT_ONLY"
        reason = "COMPLETE_SNAPSHOT"
    return {"kind": "EVENT" if kind == "QUERYPROOF_AST" else "SCOPE",
            "case": core[0], "context": core[1:], "status": verdict,
            "reason": reason, "identities": [identity for _, identity in identities]}


def audit_trace(lines):
    records = []
    errors = []
    pending = None
    phase = 0
    for line in lines:
        if not line.startswith("QUERYPROOF_"):
            continue
        line = line.rstrip("\n")
        try:
            name = line.split("\t", 1)[0]
            if name.endswith("_ERROR"):
                errors.append("OBSERVER_ERROR")
                continue
            if name in KINDS:
                if pending is not None:
                    errors.append("MISSING_INPUT_MARKER")
                decoded = decode(line, name)
                length = 7
                if len(decoded) != length:
                    raise ValueError("BASE_CONTEXT_CARDINALITY")
                core = decoded[:5] if name == "QUERYPROOF_AST" else decoded[:6]
                strings = decoded[5:] if name == "QUERYPROOF_AST" else decoded[6:]
                pending = {"kind": name, "core": core,
                           "plans": [json.loads(p) for p in strings]}
                phase = 0
            elif name in PARTS:
                if pending is None or phase >= len(PARTS) or name != PARTS[phase]:
                    raise ValueError("MISSING_OR_REORDERED_EVIDENCE")
                decoded = decode(line, name)
                expected = [pending["core"][0],
                            "EVENT" if pending["kind"] == "QUERYPROOF_AST" else "SCOPE",
                            *pending["core"][1:]]
                if decoded[:len(expected)] != expected:
                    raise ValueError("WITNESS_CONTEXT_MISMATCH")
                if name == "QUERYPROOF_INPUT":
                    records.append(finalize(pending, decoded))
                    pending = None
                phase += 1
        except (ValueError, UnicodeDecodeError, IndexError, KeyError, TypeError) as exc:
            errors.append(str(exc))
            pending = None
            phase = 0
    if pending is not None:
        errors.append("MISSING_FINAL_INPUT_MARKER")
    counters = Counter((r["kind"], r["status"], r["reason"]) for r in records)
    return {"status": "COMPLETE" if not errors else "INVALID",
            "errors": sorted(set(errors)), "records": records,
            "counts": {"|".join(keys): count for keys, count in counters.items()}}


def check_control(result):
    if result["status"] != "COMPLETE":
        return False, "INVALID_PROTOCOL"
    events = [r for r in result["records"] if r["kind"] == "EVENT"]
    negative = [r for r in events if r["case"] == "swap-negative"]
    positive = [r for r in events if r["case"] == "same-rdd-positive"]
    if len(negative) != 1 or len(positive) != 1:
        return False, "INCOMPLETE_CONTROL_EVENT_SET"
    if len(result["records"]) != 10:
        return False, "INCOMPLETE_CONTROL_SCOPE_SET"
    if negative[0]["reason"] != "INPUT_REFERENCE_CHANGED":
        return False, "SWAP_NOT_REJECTED"
    if positive[0]["reason"] != "SAME_INPUT_SEQUENCE":
        return False, "POSITIVE_IDENTITY_NOT_PRESERVED"
    snapshots = [r for r in result["records"] if r["kind"] == "SCOPE"]
    if any(r["reason"] != "COMPLETE_SNAPSHOT" for r in snapshots):
        return False, "INCOMPLETE_SCOPE_BINDINGS"
    return True, "CONTROL_PASS"


def main():
    path = Path(sys.argv[1])
    result = audit_trace(path.read_text(errors="replace").splitlines())
    okay, reason = check_control(result)
    print("QUERYPROOF_ACTUAL_RULE_INPUT_ATTESTATION=" + reason)
    print("QUERYPROOF_REAL_RULE_NEGATIVE_REJECTED=" +
          ("CONFIRMED" if okay else "UNCONFIRMED"))
    print("QUERYPROOF_REAL_RULE_POSITIVE_INPUT_PRESERVED=" +
          ("CONFIRMED" if okay else "UNCONFIRMED"))
    print("QUERYPROOF_INPUT_MARKER_COUNTS=" + json.dumps(result["counts"], sort_keys=True))
    if not okay:
        print("QUERYPROOF_INPUT_TRACE_ERRORS=" + repr(result["errors"]))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
