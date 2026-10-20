"""Retrospective classification of an immutable prospective-L CI artifact.

No new Spark execution, verifier change, or access to raw private ASTs.
"""
from collections import Counter, defaultdict
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
REPORT = ROOT / "artifacts" / "queryproof_prospective_l_frame.json"
MANIFEST = (
    ROOT / "tools" / "queryproof_prospective_l_20261020" / "CASES.json")


def main():
    report = json.loads(REPORT.read_text())
    manifest = json.loads(MANIFEST.read_text())
    family_by_case = {c["id"]: c["family"] for c in manifest["cases"]}
    ledger = report["union_coverage"]["hierarchical_scope_ledger"]
    frames = report["union_coverage"]["prospectively_frozen_frame_preservation"]
    counts = defaultdict(Counter)
    for case in manifest["cases"]:
        counts[case["family"]]["total"] += 1
    for item in ledger["optimizer_suffix_case_counts"]:
        family = family_by_case[item["case"]]
        counts[family]["suffix_observed"] += 1
        counts[family]["suffix_changed"] += int(item["changed"])
        counts[family]["certified_changed"] += int(
            item["changed"] and item["proved"])
        counts[family]["unknown"] += int(not item["proved"])
    old_certified = sorted(
        item["case"] for item in ledger["optimizer_suffix_case_counts"]
        if item["changed"] and item["proved"])
    post_frame = frames["newly_frame_preserved_suffixes"]
    if old_certified != sorted(post_frame):
        raise SystemExit("Frame preservation and local-rule approvals diverge")
    if report["executed_cases"] != len(manifest["cases"]):
        raise SystemExit("First-run artifact incomplete")
    if report["row_oracle_mismatch_cases"] or report["error_cases"]:
        raise SystemExit("First-run finite rowbag errors")
    if not report["union_coverage"]["frame_proof_version_matches_pins"]:
        raise SystemExit("Prospective frame pin not verified")
    print("QUERYPROOF_L_FIRST_RUN_SHA", report["manifest_sha256"])
    print("QUERYPROOF_L_PROOF_BY_SQL_FAMILY", json.dumps({
        k: dict(v) for k, v in sorted(counts.items())}, sort_keys=True))
    print("QUERYPROOF_L_PROVED_CASES", json.dumps(old_certified))
    print("QUERYPROOF_L_UNKNOWN_RULES", json.dumps(
        ledger["unknown_rules"], sort_keys=True))
    print("QUERYPROOF_L_UNKNOWN_SHAPES", json.dumps(
        ledger["unknown_rule_shape_changes"], sort_keys=True))
    print("QUERYPROOF_L_CHANGED_BATCHES", json.dumps([
        b for b in ledger["by_batch"] if b["changed"]],
        sort_keys=True))
    print("QUERYPROOF_L_HIDDEN_STATE_FRAME_BOUNDARIES", json.dumps({
        "preserved": frames["newly_frame_preserved_count"],
        "postfinish_batches": frames["postfinish_batch_checks"],
        "frame_violating_batch_reasons": frames["frame_violating_batch_reasons"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
