"""Prospective K validation on frozen scope-ledger and local certificate blobs."""
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
PINS = json.loads((HERE / "VERIFIER_PIN.json").read_text())

# Fail before Spark starts if any method under prospective audit has drifted.
pinned = {
    PINS["verifier_module"]: PINS["frozen_verifier_git_blob_sha"],
    PINS["scorer_module"]: PINS["frozen_scorer_git_blob_sha"],
    **PINS["scope_and_support_module_blob_sha"],
}
for path, expected in pinned.items():
    observed = subprocess.run(
        ["git", "hash-object", path], cwd=ROOT, check=True,
        capture_output=True, text=True).stdout.strip()
    if observed != expected:
        raise SystemExit("PROSPECTIVE_VERIFIER_DRIFT: " + path)

sys.path.insert(0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
import run_external
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
from scope_ledger import score_scope_trace

original = run_external.union_coverage


def union_with_locked_scopes(private_trace_path):
    result = original(private_trace_path)
    result["hierarchical_scope_ledger"] = score_scope_trace(private_trace_path)
    result["scope_verifier_pin_match"] = True
    return result


run_external.union_coverage = union_with_locked_scopes
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_scope_prospective_k.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
