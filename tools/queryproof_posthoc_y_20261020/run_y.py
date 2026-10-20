"""Round Y phase-local state gate and counterfactual trace mutations.

Development POSTHOC replay of W cases; never changes the frozen W report.
Synthetic single-counter flips are made in private disposable traces only.
"""
import base64
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
for name in ("queryproof_posthoc_y_20261020",
             "queryproof_posthoc_x_20261020",
             "queryproof_prospective_w_20261020",
             "queryproof_join_filters_u_dev_20261020",
             "queryproof_join_suffix_s_dev_20261020",
             "queryproof_join_pruning_q_dev_20261020",
             "queryproof_round_n_dev_20261020",
             "queryproof_external_postgresql_join_20261019",
             "queryproof_context_validator", "queryproof_instrumented",
             "queryproof_holdout_20261018", "queryproof_scopes_20261020",
             "queryproof_state_witness_20261020",
             "queryproof_state_frame_20261020",
             "queryproof_adversarial_20261020"):
    sys.path.insert(0, str(ROOT / "tools" / name))

import run_external
import run_x
from scope_ledger_v5 import score_scope_trace
from phase_guard import score_phase_local


def _opaque_counter_mutant(original_path, scope_summary, scope_phase):
    lines = original_path.read_text(errors="replace").splitlines()
    matched = False
    for i, line in enumerate(lines):
        if not line.startswith("QUERYPROOF_STATE\t"):
            continue
        parts = line.split("\t")
        fields = [base64.b64decode(item, validate=True).decode("utf8")
                  for item in parts[1:]]
        if fields[0] != "W01":
            continue
        if scope_phase == "pre" and not (
                fields[1] == "SCOPE" and fields[4] == "Resolution"):
            continue
        if scope_phase == "post" and not (
                fields[1] == "EVENT" and fields[3] == "Infer Filters"):
            continue
        context_width = 7 if fields[1] == "SCOPE" else 6
        assert len(fields) == context_width + (4 if scope_phase == "pre" else 8)
        offset = context_width + 2  # third integer of first state = opaque counter
        assert int(fields[offset]) >= 0
        fields[offset] = str(int(fields[offset]) + 1)
        lines[i] = "QUERYPROOF_STATE\t" + "\t".join(
            base64.b64encode(v.encode("utf8")).decode("ascii") for v in fields)
        matched = True
        break
    if not matched:
        return {"status": "NOT_APPLICABLE", "reason": "SYNTHETIC_TARGET_MARKER_NOT_FOUND"}
    with tempfile.TemporaryDirectory(prefix="qp-y-opaque-test-") as tmp:
        mutated = Path(tmp) / "mutated-private-trace.txt"
        mutated.write_text("\n".join(lines) + "\n")
        result = score_phase_local(mutated, scope_summary)
    return {
        "status": result["status"],
        "target_mutation": scope_phase,
        "phase_guard_survivors": result["phase_local_eligible_count"],
        "target_W01_survives": "W01" in result["phase_local_eligible_cases"],
        "errors": result["errors"],
    }


def score_y(path):
    original = run_x.score_x(path)
    scopes = score_scope_trace(path)
    result = score_phase_local(path, scopes)
    old_global = original["round_x_posthoc"]["state_guard_count"]
    before = _opaque_counter_mutant(path, scopes, "pre")
    after = _opaque_counter_mutant(path, scopes, "post")
    original["round_y_posthoc"] = {
        "status": "POSTHOC_DIFFERENTIAL_STATE_CUTPOINT_DEVELOPMENT",
        "actual_phase_guard": result,
        "global_presence_guard_count": old_global,
        "negative_control_postcut_opaque_mutant": after,
        "positive_control_precut_opaque_mutant": before,
        "comparison": (
            "Only snapshot-reachable state at and after FinishAnalysis cut "
            "is relevant to conditional optimizer suffix; tag/metadata "
            "fingerprint comparison remains delegated to frame_guard"),
        "limitations": [
            "A separate posthoc W replay, not first-run prospective W proof",
            "Uses frame_guard, not an independent hidden-state measurement",
            "All frame digests are observational, collision-prone and not semantic proofs",
            "Does not certify pre-Finish Analyzer or any uninstrumented state",
        ],
    }
    return original


run_external.union_coverage = score_y
run_external.MANIFEST = ROOT / "tools" / "queryproof_prospective_w_20261020" / "CASES.json"
run_external.PROTOCOL = ROOT / "tools" / "queryproof_prospective_w_20261020" / "PROTOCOL.json"
run_external.OUTPUT = ROOT / "artifacts" / "queryproof_round_y_phase_local_posthoc.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
