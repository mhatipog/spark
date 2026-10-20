"""Prospective L first-run evaluation under frozen JVM-frame and local certificates.

Every proof-critical module and the L corpus/protocol are checked against exact
Git blob IDs before launching Spark. No automatic verifier repair is allowed.
"""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent.parent
HERE = Path(__file__).resolve().parent
pins = json.loads((HERE / "VERIFIER_PIN.json").read_text())
if pins["benchmark_id"] != "queryproof-preserved-jvm-frame-prospective-l-20261020":
    raise SystemExit("L_BENCHMARK_PIN_DRIFT")
for path, expected in pins["pinned_git_blob_sha"].items():
    current = subprocess.run(
        ["git", "hash-object", path],
        cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()
    if current != expected:
        raise SystemExit("L_VERIFIER_OR_CASES_DRIFT:" + path)
print("QUERYPROOF_L_FROZEN_METHODS_AND_CORPUS_VERIFIED")

sys.path.insert(
    0, str(ROOT / "tools" / "queryproof_external_postgresql_join_20261019"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_scopes_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_witness_20261020"))
sys.path.insert(0, str(ROOT / "tools" / "queryproof_state_frame_20261020"))

import run_external
from scope_ledger import score_scope_trace
from state_guard import score_state_witness
from frame_guard import score_frame

old = run_external.union_coverage


def prospective_audit(path):
    result = old(path)
    scopes = score_scope_trace(path)
    result["hierarchical_scope_ledger"] = scopes
    result["jvm_state_witness"] = score_state_witness(path, scopes)
    result["prospectively_frozen_frame_preservation"] = score_frame(path, scopes)
    result["frame_proof_version_matches_pins"] = True
    return result


run_external.union_coverage = prospective_audit
run_external.MANIFEST = HERE / "CASES.json"
run_external.PROTOCOL = HERE / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_prospective_l_frame.json"
run_external.DIAGNOSTIC_GENERALIZED = True
run_external.DIAGNOSTIC_GROUPING_REPETITION = True

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
