"""Reflect paired abstract BEFORE/AFTER terms from V11-checked Spark events.

Inputs are the CI replay result, not raw SQL. Coq checks abstract plan-pair
structure (same join/comparator, old has no guard, legal new guards) and a
universal nullable signed-key bag theorem. Catalyst JSON -> typed pair stays
a separately tracked, unverified interpretation boundary.
"""
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
SRC=ROOT/"artifacts/queryproof_v12_native_source_metadata_observations.json"
DEST=ROOT/"research/queryproof/proofs/QueryProofV13LivePairs.v"
KINDS={"Inner","LeftOuter"}
COMPS={"StrictEq","StrictLt"}
TARGETS={"W17","W18","W19","W20"}

def emit():
    r=json.loads(SRC.read_text())
    assert r["executed_cases"]==32 and r["worker_exit_code"]==0
    assert not r["error_cases"] and not r["row_oracle_mismatch_cases"]
    results=r["union_coverage"]
    witnesses=results["v10_coq_guard_witnesses"]["witnesses"]
    v11=results["v11_independent_ast_binding"]
    assert v11["independent_witness_count"]==len(witnesses)==4
    assert {w["case"] for w in witnesses}==TARGETS
    native=results["v12_native_metadata_binding"]
    assert native["all_live_coq_witnesses"]==4
    source_attested=set(native["source_metadata_attested_cases"])
    value_fields_attested=set(native["native_exprid_valuefields_attested_cases"])
    assert value_fields_attested==TARGETS, "NATIVE_JVM_EXPRID_FIELDS_NOT_ATTESTED"
    lines=[
       "(** Generated from live W32 replay, with independent V11 AST binder.",
       "    Kernel proves the REDUCED-PLAN pair law, NOT verified Spark semantics.",
       "    Source-native attestation remains independently reported in V12. *)",
       "Require Import QueryProofGuardCertificate QueryProofGuardPair",
       "  QueryProofGuardPayload QueryProofGuardPairPayload.",
       "",
    ]
    for i,w in enumerate(witnesses):
        assert w["comparator"] in COMPS and w["join_kind"] in KINDS
        assert isinstance(w["guard_left"],bool) and isinstance(w["guard_right"],bool)
        assert w["case"] in TARGETS
        assert w["join_kind"]!="LeftOuter" or not w["guard_left"]
        assert isinstance(w["serialized_plan_pair_sha256"],str) and len(w["serialized_plan_pair_sha256"])==64
        fmt=lambda value:str(value).lower()
        suffix=f"{i:03d}"
        old,new=f"old_live_{suffix}",f"new_live_{suffix}"
        lines.extend([
           f"(* {w['case']}; strict native-object attested: {str(w['case'] in source_attested).lower()};",
           f"   native ExprId value-fields attested: {str(w['case'] in value_fields_attested).lower()};",
           f"   serialized snapshot pair SHA256 {w['serialized_plan_pair_sha256']} *)",
           f"Definition {old} : GuardCertificate :=",
           f"  {{| comparison := {w['comparator']}; join_kind := {w['join_kind']};",
           "     filter_left := false; filter_right := false |}.",
           f"Definition {new} : GuardCertificate :=",
           f"  {{| comparison := {w['comparator']}; join_kind := {w['join_kind']};",
           f"     filter_left := {fmt(w['guard_left'])};",
           f"     filter_right := {fmt(w['guard_right'])} |}}.",
           f"Example live_pair_{suffix}_accepted : transition_ok {old} {new} = true.",
           "Proof. vm_compute. reflexivity. Qed.",
           f"Theorem live_pair_{suffix}_bag_semantics :",
           "  forall left right,",
           f"    execute {old} (guarded_left {old} left)",
           f"                  (guarded_right {old} right) =",
           f"    execute {new} (guarded_left {new} left)",
           f"                  (guarded_right {new} right).",
           "Proof. intros. apply checked_plan_pair_preserves_bags.",
           "       vm_compute. reflexivity. Qed.",
           f"Theorem live_pair_{suffix}_full_row_payload_bags :",
           "  forall (L R : Type) (lk : L -> Nullable) (rk : R -> Nullable)",
           "         (left : list L) (right : list R),",
           f"    @run_payload L R lk rk {old}",
           f"      (@guarded_left_rows L lk {old} left)",
           f"      (@guarded_right_rows R rk {old} right) =",
           f"    @run_payload L R lk rk {new}",
           f"      (@guarded_left_rows L lk {new} left)",
           f"      (@guarded_right_rows R rk {new} right).",
           "Proof. intros; apply checked_pair_preserves_entire_row_bags.",
           "       vm_compute. reflexivity. Qed.",
           "",
        ])
    lines+= [
        "(* Incorrect before/after relation must fail even if after-alone check passes. *)",
        "Example mutant_changes_comparator :",
        "  transition_ok",
        "    {| comparison := StrictEq; join_kind := Inner;",
        "       filter_left := false; filter_right := false |}",
        "    {| comparison := StrictLt; join_kind := Inner;",
        "       filter_left := false; filter_right := true |} = false.",
        "Proof. vm_compute. reflexivity. Qed.",
        "Example mutant_changes_join_kind :",
        "  transition_ok",
        "    {| comparison := StrictLt; join_kind := Inner;",
        "       filter_left := false; filter_right := false |}",
        "    {| comparison := StrictLt; join_kind := LeftOuter;",
        "       filter_left := false; filter_right := true |} = false.",
        "Proof. vm_compute. reflexivity. Qed.",
    ]
    DEST.write_text("\n".join(lines)+"\n")
    print("QUERYPROOF_V13_GENERATED_LIVE_PAIRS",len(witnesses),
          "STRICT_NATIVE_OBJECT_ATTESTATIONS",len(source_attested),
          "NATIVE_EXPRID_VALUE_ATTESTATIONS",len(value_fields_attested))

if __name__=="__main__":emit()
