"""Independent QueryProof V12 stream verifier and adversarial test battery.

Verifies internal integrity of the V11 source-hook channel ONLY. SHA-256 here
is unkeyed: it cannot authenticate Spark, the emitter, semantic correctness,
or a malicious rewriter capable of recomputing a new seal.
"""
import base64
import binascii
import hashlib
import re
import sys
from pathlib import Path

PREFIX = "QUERYPROOF_V12_STREAM_B64="
BEGIN = "QUERYPROOF_RULE_SCOPE_V11_BEGIN"
END = "QUERYPROOF_RULE_SCOPE_V11_END"
MUT = "QUERYPROOF_TAG_MUTATION_V11"
SEAL = "QUERYPROOF_V12_SEAL"
QUERY = "V12_NATIVE_NARROW_SEALED_STREAM"


class Rejected(ValueError):
    pass


def encode(parts):
    return "\t".join(
        [parts[0]] + [base64.b64encode(x.encode("utf-8")).decode("ascii")
                      for x in parts[1:]]
    )


def decode(line):
    columns = line.split("\t")
    if len(columns) <= 1:
        raise Rejected("unframed or unencoded line")
    values = []
    for encoded in columns[1:]:
        try:
            raw = base64.b64decode(encoded, validate=True)
            if base64.b64encode(raw).decode("ascii") != encoded:
                raise Rejected("noncanonical Base64")
            values.append(raw.decode("utf-8", errors="strict"))
        except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
            raise Rejected("invalid Base64 field") from exc
    return columns[0], values


def number(s):
    if not re.fullmatch(r"(0|[1-9][0-9]*)", s):
        raise Rejected("noncanonical integer")
    return int(s)


def verify(raw):
    if not isinstance(raw, bytes) or not raw.endswith(b"\n"):
        raise Rejected("missing final newline")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise Rejected("invalid UTF-8") from exc
    if "\r" in text or "\x00" in text:
        raise Rejected("noncanonical line separator")
    lines = text[:-1].split("\n")
    if len(lines) < 2:
        raise Rejected("missing events/seal")
    parsed = [decode(line) for line in lines]
    if parsed[-1][0] != SEAL or len(parsed[-1][1]) != 7:
        raise Rejected("one final seal required")
    if any(marker == SEAL for marker, _ in parsed[:-1]):
        raise Rejected("multiple/early seal")

    seal = parsed[-1][1]
    if seal[0] != QUERY:
        raise Rejected("unexpected query identity")
    declared_count, declared_mut, declared_scope, unknown, depth = [
        number(x) for x in seal[1:6]
    ]
    declared_sha = seal[6]
    if not re.fullmatch(r"[0-9a-f]{64}", declared_sha):
        raise Rejected("invalid digest")
    if declared_count != len(lines) - 1:
        raise Rejected("event count mismatch")
    if unknown != 0 or depth != 0:
        raise Rejected("UNKNOWN or unclosed scope in seal")
    original_bytes = ("\n".join(lines[:-1]) + "\n").encode("utf-8")
    if hashlib.sha256(original_bytes).hexdigest() != declared_sha:
        raise Rejected("digest mismatch")

    stack = []
    scopes = {}
    starts = []
    mutation_serial = 0
    writes = []
    for marker, fields in parsed[:-1]:
        if marker in (BEGIN, END):
            if len(fields) != 6 or fields[0] != QUERY:
                raise Rejected("wrong scope record")
            sid = number(fields[1])
            number(fields[5])
            if marker == BEGIN:
                if sid != len(starts) + 1 or sid in scopes:
                    raise Rejected("scope serial not contiguous")
                scopes[sid] = tuple(fields)
                starts.append(fields)
                stack.append(sid)
            else:
                if not stack or stack[-1] != sid:
                    raise Rejected("scope not in LIFO order")
                if tuple(fields) != scopes[sid]:
                    raise Rejected("scope end fields changed")
                stack.pop()
        elif marker == MUT:
            if len(fields) != 9 or fields[0] != QUERY:
                raise Rejected("wrong mutation record")
            seq = number(fields[1])
            node = number(fields[2])
            if seq != mutation_serial + 1 or node <= 0:
                raise Rejected("mutation serial/identity invalid")
            mutation_serial = seq
            sid = number(fields[4])
            if not stack or stack[-1] != sid:
                raise Rejected("mutation not attributed to active top scope")
            op, tag, old, new = fields[5:]
            if (tag != "FAILED_TO_EVALUATE" or
                op not in ("SET", "UNSET") or
                old not in ("true", "false") or
                new not in ("true", "false") or
                new != ("true" if op == "SET" else "false")):
                raise Rejected("unsupported mutation semantics")
            writes.append((sid, node, op, old, new))
        else:
            raise Rejected("unknown/unaccounted event: " + marker)
    if stack or mutation_serial != declared_mut or len(starts) != declared_scope:
        raise Rejected("unclosed scopes or nonmatching sequence counter")

    # Prospective V12 test contract: one production ConstantFolding execution,
    # followed by an OuterRule containing an InnerRule on one Cast node.
    if len(starts) != 3 or len(writes) != 5 or len(lines) != 12:
        raise Rejected("V12 witness test shape changed")
    rule_names = [x[4] for x in starts]
    if ("ConstantFolding" not in rule_names[0] or
        "OuterRule" not in rule_names[1] or
        "InnerRule" not in rule_names[2]):
        raise Rejected("unexpected native rule identity")
    if [x[0] for x in writes] != [1, 2, 3, 3, 2]:
        raise Rejected("write-to-scope attribution changed")
    if [x[2] for x in writes] != ["SET", "SET", "UNSET", "SET", "UNSET"]:
        raise Rejected("transient write sequence changed")
    if len(set(x[1] for x in writes[1:])) != 1:
        raise Rejected("nested Cast identity changed")
    if writes[0][3:] != ("false", "true"):
        raise Rejected("production ConstantFolding transition changed")
    return {"events": declared_count, "scopes": len(starts),
            "mutations": len(writes), "sha256": declared_sha}


def extract_native_stdout(path):
    matches = [line[len(PREFIX):] for line in Path(path).read_text().splitlines()
               if line.startswith(PREFIX)]
    if len(matches) != 1:
        raise Rejected("expected exactly one native stream payload")
    try:
        return base64.b64decode(matches[0], validate=True)
    except binascii.Error as exc:
        raise Rejected("bad native payload Base64") from exc


def mutation(raw, kind):
    lines = raw.decode("utf-8").splitlines()
    events, seal = lines[:-1], lines[-1]
    if kind == "drop":
        events.pop(1)
    elif kind == "duplicate":
        events.insert(1, events[1])
    elif kind == "reorder":
        events[0], events[1] = events[1], events[0]
    elif kind == "wrong_operation":
        idx = next(i for i, line in enumerate(events) if line.startswith(MUT + "\t"))
        marker, fields = decode(events[idx])
        fields[5] = "UNSET"
        events[idx] = encode([marker] + fields)
    elif kind == "wrong_scope":
        idx = next(i for i, line in enumerate(events) if line.startswith(MUT + "\t"))
        marker, fields = decode(events[idx])
        fields[4] = "999"
        events[idx] = encode([marker] + fields)
    elif kind == "unknown":
        events.insert(1, encode(["QUERYPROOF_TAG_MUTATION_V11_UNKNOWN", QUERY, "bogus"]))
    elif kind == "noseal":
        return ("\n".join(events) + "\n").encode()
    elif kind == "append":
        return ("\n".join(lines + ["QUERYPROOF_V12_UNKNOWN_AFTER_SEAL"]) + "\n").encode()
    elif kind == "digest":
        marker, fields = decode(seal)
        fields[6] = "0" * 64
        seal = encode([marker] + fields)
    elif kind == "base64":
        events[0] = events[0] + "\t!"
    elif kind == "truncate":
        return raw[:-6]
    elif kind == "unclosed":
        idx = next(i for i, line in enumerate(events) if line.startswith(END + "\t"))
        events[idx] = events[idx].replace(END, BEGIN, 1)
    else:
        raise ValueError(kind)
    return ("\n".join(events + [seal]) + "\n").encode()


def forged_reseal(raw):
    """Show exactly why an unkeyed checksum is not authenticity."""
    lines = raw.decode().splitlines()
    idx = next(i for i, s in enumerate(lines) if s.startswith(MUT + "\t"))
    marker, fields = decode(lines[idx])
    fields[2] = str(number(fields[2]) + 100000)
    lines[idx] = encode([marker] + fields)
    marker, seal = decode(lines[-1])
    seal[6] = hashlib.sha256(
        ("\n".join(lines[:-1]) + "\n").encode("utf-8")
    ).hexdigest()
    lines[-1] = encode([marker] + seal)
    return ("\n".join(lines) + "\n").encode("utf-8")


def main(path):
    actual = extract_native_stdout(path)
    result = verify(actual)
    print("QUERYPROOF_V12_INDEPENDENT_TRACE_INTEGRITY_VERIFIED", result)
    controls = [
        "drop", "duplicate", "reorder", "wrong_operation", "wrong_scope",
        "unknown", "noseal", "append", "digest", "base64", "truncate", "unclosed"
    ]
    for kind in controls:
        try:
            verify(mutation(actual, kind))
        except Rejected as exc:
            print("QUERYPROOF_V12_ADVERSARY_REJECTED", kind, str(exc))
        else:
            raise AssertionError("critical adulteration was accepted: " + kind)
    assert len(controls) == 12
    print("QUERYPROOF_V12_TWELVE_ADVERSARIAL_CONTROLS_REJECTED")
    forged = forged_reseal(actual)
    assert forged != actual
    forged_result = verify(forged)
    assert forged_result["events"] == result["events"]
    print("QUERYPROOF_V12_RESEALED_FORGERY_ACCEPTED_BY_UNKEYED_VERIFIER")
    print("QUERYPROOF_V12_NOT_AUTHENTICATION_NOT_SQL_PROOF")
    print("QUERYPROOF_V12_TEST_COMPLETE")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: verify_v12_stream.py NATIVE_STDOUT_LOG")
    try:
        main(sys.argv[1])
    except (Rejected, AssertionError) as exc:
        sys.exit("QUERYPROOF_V12_FAIL_CLOSED: " + str(exc))
