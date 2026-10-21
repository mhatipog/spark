"""V10 developmental bridge: actual Spark 4.2 Catalyst local-rule events ->
narrow signed-nullable guard certificate -> Coq CHECKER (in CI).

The Coq checker proves the universal KEY-ONLY bag lemma. The AST decoder,
SQLConf semantics, native metadata and hidden-state effects remain unproved.
Never present this as a full Spark proof, prospective coverage, or W32 soundness.
"""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
for name in (
    "queryproof_v9_inequality_20261020",
    "queryproof_v7_strict_20261020",
    "queryproof_posthoc_v6_20261020",
    "queryproof_instrumented",
    "queryproof_holdout_20261018",
    "queryproof_join_filters_u_dev_20261020",
):
    sys.path.insert(0, str(ROOT / "tools" / name))

import run_v9
import run_external
from rank_ast import decode
from run_holdout import _rule_basename
from filter_steps_v9 import _frames_to_join, certify_inferred_filter_event

TARGETS = frozenset({"W17", "W18", "W19", "W20"})
COMPARISONS = {
    "org.apache.spark.sql.catalyst.expressions.EqualTo": "StrictEq",
    "org.apache.spark.sql.catalyst.expressions.LessThan": "StrictLt",
}
JOINS = {"inner": "Inner", "left": "LeftOuter"}
COQ_TARGET = ROOT / "research/queryproof/proofs/QueryProofV10LiveInstances.v"


def parse_witness(event):
    """Use V9's strict direct-input AST checker to produce candidate facts.

    This translator remains in the TCB. In particular the Catalyst JSON path
    is NOT injective: metadata and mutable JVM state can be erased.
    """
    if _rule_basename(event["rule"]) != "InferFiltersFromConstraints":
        return None
    proof = certify_inferred_filter_event(event)
    if proof["status"] != "EQUIVALENT_IN_FRAGMENT":
        return None
    header, _old_children, _new_children, kind = _frames_to_join(event)
    condition = header.get("condition")
    if not (isinstance(condition, list) and len(condition) == 3):
        raise ValueError("V10_UNEXPECTED_CONDITION_SHAPE")
    op = condition[0]
    if not isinstance(op, dict) or op.get("class") not in COMPARISONS:
        raise ValueError("V10_UNSUPPORTED_COMPARATOR")
    if kind not in JOINS or proof["join_kind"] != kind:
        raise ValueError("V10_JOIN_KIND_DISAGREEMENT")
    sides = proof["filter_sides"]
    if (not isinstance(sides, list) or not sides or
            any(type(i) is not int or i not in (0, 1) for i in sides) or
            len(set(sides)) != len(sides)):
        raise ValueError("V10_UNEXPECTED_FILTER_SIDES")
    if kind == "left" and 0 in sides:
        raise ValueError("V10_LEFT_PRESERVED_SIDE_NOT_ADMISSIBLE")
    encoding = json.dumps(
        {"old": event["old"], "new": event["new"]},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return {
        "case": event["case"],
        "comparator": COMPARISONS[op["class"]],
        "join_kind": JOINS[kind],
        "guard_left": 0 in sides,
        "guard_right": 1 in sides,
        "serialized_plan_pair_sha256": hashlib.sha256(encoding).hexdigest(),
    }


def emit_kernel_instances(witnesses):
    """No SQL literals or full serialized Catalyst ASTs are exported."""
    text = [
        "(** Generated FROM live W32 optimizer-rule events in this CI job.",
        "    Coq proves the nullable key-bag law CONDITIONALLY on the",
        "    unverified Python/Catalyst interpretation of each witness.",
        "    Do not commit this disposable file or claim Spark soundness. *)",
        "Require Import QueryProofGuardCertificate.",
        "",
    ]
    for i, w in enumerate(witnesses):
        name = "live_%03d" % i
        text += [
            "(* case %s; encoded-plan-pair SHA-256 %s *)" % (
                w["case"], w["serialized_plan_pair_sha256"]),
            "Definition %s : GuardCertificate :=" % name,
            "  {| comparison := %s; join_kind := %s;" % (
                w["comparator"], w["join_kind"]),
            "     filter_left := %s; filter_right := %s |}." % (
                str(w["guard_left"]).lower(), str(w["guard_right"]).lower()),
            "Example %s_admitted_by_kernel : check %s = true." % (name, name),
            "Proof. vm_compute. reflexivity. Qed.",
            "Theorem %s_key_bags_preserved :" % name,
            "  forall l r, execute %s (guarded_left %s l)" % (name, name),
            "                      (guarded_right %s r) = execute %s l r." % (
                name, name),
            "Proof. intros; apply checked_certificate_sound.",
            "       vm_compute. reflexivity. Qed.",
            "",
        ]
    text += [
        "(* Counterexample controls must never be admitted by the checker. *)",
        "Example reject_nullsafe :",
        "  check {| comparison := NullSafeEq; join_kind := Inner;",
        "           filter_left := true; filter_right := true |} = false.",
        "Proof. vm_compute. reflexivity. Qed.",
        "Example reject_outer_left :",
        "  check {| comparison := StrictLt; join_kind := LeftOuter;",
        "           filter_left := true; filter_right := true |} = false.",
        "Proof. vm_compute. reflexivity. Qed.",
    ]
    COQ_TARGET.write_text("\n".join(text) + "\n")


def score_v10(path):
    report = run_v9.score_v9(path)
    witnesses = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if event["case"] in TARGETS and event["old"] != event["new"]:
            witness = parse_witness(event)
            if witness is not None:
                witnesses.append(witness)
    observed_cases = {w["case"] for w in witnesses}
    if observed_cases != TARGETS:
        raise RuntimeError("V10_MISSING_LIVE_JOIN_GUARD_WITNESS: " +
                           repr(sorted(TARGETS - observed_cases)))
    if len(witnesses) < 4:
        raise RuntimeError("V10_TOO_FEW_LIVE_GUARD_WITNESSES")
    emit_kernel_instances(witnesses)
    report["v10_coq_guard_witnesses"] = {
        "status": "LIVE_W32_BRIDGE_OBLIGATIONS_PENDING_COQ",
        "case_ids": sorted(observed_cases),
        "guard_certificate_instances": len(witnesses),
        "witnesses": witnesses,
        "coq_file": "research/queryproof/proofs/QueryProofV10LiveInstances.v",
        "limits": [
            "No prospective or unseen test: W32 is retrospective",
            "Python AST to Coq certificate reflection is unverified",
            "Coq theorem models nullable signed join KEYS, not full Catalyst row schema",
            "TreeNode.toJSON metadata erasure and JVM hidden effects remain unsolved",
            "No error, nondeterminism, physical execution, or SQLConf proof",
        ],
    }
    return report


run_external.union_coverage = score_v10
run_external.OUTPUT = ROOT / "artifacts/queryproof_v10_w32_coq_guard_witnesses.json"

if __name__ == "__main__":
    raise SystemExit(run_external.parent())
