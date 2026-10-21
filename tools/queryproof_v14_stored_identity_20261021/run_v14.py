"""V14 retrospective W32 diagnostic: why does V12 strict identity fail?

Wraps the unchanged V12 scorer and adds, for the same four V10/V11/V12
InferFiltersFromConstraints witness events, the V14 stored/derived identity
counts. Predictions are frozen in PROTOCOL.json BEFORE this runs. W32 is
historical development data; nothing here is prospective or an acceptance.
"""
import base64
import json
from collections import defaultdict
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "tools" / "queryproof_v12_native_20261021"))
import run_v12  # noqa: E402  (also imports run_v10/run_v11/run_external)
from run_v12 import run_external, run_v10  # noqa: E402

PROTOCOL = json.loads((HERE / "PROTOCOL.json").read_text())


def decode(line, marker, arity):
    parts = line.rstrip("\n").split("\t")[1:]
    if not line.startswith(marker + "\t") or len(parts) != arity:
        raise ValueError("MALFORMED_" + marker)
    return [base64.b64decode(p, validate=True).decode("utf-8") for p in parts]


def counts(csv):
    values = [int(x) for x in csv.split(",")]
    if len(values) != 4 or min(values) < 0:
        raise ValueError("MALFORMED_V14_COUNTS")
    return dict(zip(("missing", "changed", "fresh", "fresh_unseen"), values))


def parse_v14(path):
    events = defaultdict(list)
    unknown = defaultdict(int)
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("QUERYPROOF_NATIVE_V14\t"):
            f = decode(line, "QUERYPROOF_NATIVE_V14", 9)
            self_stored, self_derived = f[6].split(";")
            events[tuple(f[:5])].append({
                "effective": f[5] == "true",
                "self_stored": counts(self_stored),
                "self_derived": counts(self_derived),
                "stored": counts(f[7]),
                "derived": counts(f[8]),
            })
        elif line.startswith("QUERYPROOF_NATIVE_V14_UNKNOWN\t"):
            unknown[decode(line, "QUERYPROOF_NATIVE_V14_UNKNOWN", 7)[6]] += 1
    return events, dict(unknown)


def witness_key(case, ast):
    """Same unique-candidate selection as run_v12.score_v12."""
    matching = [k for k, series in ast.items()
                if k[0] == case and k[3].endswith("InferFiltersFromConstraints")
                for x in series
                if run_v10.parse_witness(dict(
                    case=case, rule=k[3], old=json.loads(x["raw_before"]),
                    new=json.loads(x["raw_after"]))) is not None]
    return matching[0] if len(matching) == 1 else None


def score_v14(path):
    report = run_v12.score_v12(path)
    v12 = report["v12_native_metadata_binding"]
    v12_diag = {x["case"]: x.get("native_identity_diagnostic")
                for x in v12["unattested_cases"]}
    ast, _, _ = run_v12.parse_events(path)
    v14, unknown = parse_v14(path)
    rows = []
    for witness in report["v10_coq_guard_witnesses"]["witnesses"]:
        case = witness["case"]
        key = witness_key(case, ast)
        group = v14.get(key, []) if key else []
        if len(group) != 1:
            rows.append({"case": case, "status": "UNKNOWN_V14_WITNESS_MISSING_OR_DUPLICATE"})
            continue
        e = group[0]
        diag = v12_diag.get(case)
        v12_missing = diag["missing_prior_objects"] if diag else 0
        explained = (e["self_stored"]["missing"] == 0 and
                     e["self_derived"]["missing"] == v12_missing and
                     e["stored"]["missing"] == 0 and e["stored"]["changed"] == 0)
        stored_clean = (e["stored"]["missing"] == 0 and e["stored"]["changed"] == 0
                        and e["stored"]["fresh_unseen"] == 0)
        rows.append({"case": case, "effective": e["effective"],
                     "v12_strict_attested": diag is None,
                     "v12_missing_prior_objects": v12_missing,
                     "self_stored": e["self_stored"], "self_derived": e["self_derived"],
                     "stored": e["stored"], "derived": e["derived"],
                     "h1_v12_failure_explained_by_derived_reallocation": explained,
                     "h2_stored_identity_preserved": stored_clean})
    report["v14_stored_identity"] = {
        "status": "RETROSPECTIVE_DIAGNOSTIC_NOT_AN_ACCEPTANCE",
        "protocol_id": PROTOCOL["protocol_id"],
        "witnesses": rows,
        "h1_count": sum(bool(r.get("h1_v12_failure_explained_by_derived_reallocation")) for r in rows),
        "h2_count": sum(bool(r.get("h2_stored_identity_preserved")) for r in rows),
        "native_v14_unknown_markers": unknown,
        "limits": PROTOCOL["limits"],
    }
    return report


run_external.union_coverage = score_v14
run_external.OUTPUT = ROOT / "artifacts/queryproof_v14_stored_identity.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
