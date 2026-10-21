(** V14B: machine-checked proof of OLD/NEW abstract plan-pair
    preservation for complete SQL row payload bags, with SQL NULL and
    duplicates. Formalized predicates are strict integral Eq/Lt only.
    The actual Catalyst -> typed term reflection, source effects, and
    Spark evaluator equivalence remain independently unproved.
*)
From Coq Require Import Bool.Bool Lists.List.
Require Import QueryProofGuardCertificate QueryProofGuardPair QueryProofGuardPayload.

Section FullRowsPlanPairs.
  Context {L R : Type}.
  Variable left_key : L -> Nullable.
  Variable right_key : R -> Nullable.

  Theorem checked_pair_preserves_entire_row_bags :
    forall (old new : GuardCertificate) (left : list L) (right : list R),
      transition_ok old new = true ->
      @run_payload L R left_key right_key old
        (@guarded_left_rows L left_key old left)
        (@guarded_right_rows R right_key old right) =
      @run_payload L R left_key right_key new
        (@guarded_left_rows L left_key new left)
        (@guarded_right_rows R right_key new right).
  Proof.
    intros [ko jo lo ro] [kn jn ln rn] left right H.
    unfold transition_ok in H; simpl in H.
    apply andb_true_iff in H as [Hpre Hnew].
    apply andb_true_iff in Hpre as [Hpre Hro].
    apply andb_true_iff in Hpre as [Hpre Hlo].
    apply andb_true_iff in Hpre as [Hcmp Hkind].
    apply comparator_equal_sound in Hcmp.
    apply kind_equal_sound in Hkind.
    apply negb_true_iff in Hlo.
    apply negb_true_iff in Hro.
    subst kn jn lo ro.
    cbn [guarded_left_rows guarded_right_rows].
    replace
      (@run_payload L R left_key right_key
        {| comparison := ko; join_kind := jo;
           filter_left := false; filter_right := false |}
        left right)
      with
      (@run_payload L R left_key right_key
        {| comparison := ko; join_kind := jo;
           filter_left := ln; filter_right := rn |}
        left right) by reflexivity.
    pose proof (@checked_certificate_preserves_entire_payload_bags
      L R left_key right_key
      {| comparison := ko; join_kind := jo;
         filter_left := ln; filter_right := rn |}
      left right Hnew) as Hlaw.
    exact (eq_sym Hlaw).
  Qed.
End FullRowsPlanPairs.

(** Two negative obligations are kept out of the trusted proof. *)
Example no_nullsafe_pair :
  transition_ok
    {| comparison := StrictEq; join_kind := Inner;
       filter_left := false; filter_right := false |}
    {| comparison := NullSafeEq; join_kind := Inner;
       filter_left := true; filter_right := true |} = false.
Proof. reflexivity. Qed.

Example no_preserved_outer_filter_pair :
  transition_ok
    {| comparison := StrictLt; join_kind := LeftOuter;
       filter_left := false; filter_right := false |}
    {| comparison := StrictLt; join_kind := LeftOuter;
       filter_left := true; filter_right := false |} = false.
Proof. reflexivity. Qed.
