(** V14: preserve COMPLETE payload rows and bag multiplicity, not merely keys.
    This is a universally quantified finite SQL-style relation theorem.
    All payloads L and R are arbitrary Types; key extractors may yield NULL.
    Actual Catalyst AST reflection, tagged state, metadata and SQLConf remain
    external and explicitly unverified.
*)
From Coq Require Import Lists.List Bool.Bool.
Require Import QueryProofGuardCertificate.
Import ListNotations.

Section ArbitraryRowPayload.
  Context {L R : Type}.
  Variable left_key : L -> Nullable.
  Variable right_key : R -> Nullable.

  Definition true_match (k : Comparator) (l : L) (r : R) : bool :=
    cmp_true k (left_key l) (right_key r).

  Definition inner_payload (k : Comparator) (left : list L) (right : list R)
      : list (L * option R) :=
    flat_map (fun l =>
      map (fun r => (l, Some r)) (filter (true_match k l) right)) left.

  Definition outer_step (k : Comparator) (l : L) (right : list R)
      : list (L * option R) :=
    match filter (true_match k l) right with
    | [] => [(l,None)]
    | matching => map (fun r => (l,Some r)) matching
    end.

  Definition outer_payload (k : Comparator) (left : list L) (right : list R)
      : list (L * option R) :=
    flat_map (fun l => outer_step k l right) left.

  Lemma right_guard_preserves_matches :
    forall k l right, strict k = true ->
      filter (true_match k l) (filter (fun r => present (right_key r)) right) =
      filter (true_match k l) right.
  Proof.
    intros k l right Hstrict.
    apply filter_implied_guard.
    intros r Hmatch.
    pose proof (strict_match_requires_present
      k (left_key l) (right_key r) Hstrict Hmatch) as [_ Hright].
    exact Hright.
  Qed.

  Lemma no_matches_when_left_key_missing :
    forall k l right, strict k = true ->
      present (left_key l) = false ->
      filter (true_match k l) right = [].
  Proof.
    intros k l right Hstrict Hnull.
    induction right as [|r rs IH].
    - reflexivity.
    - simpl. destruct (true_match k l r) eqn:Hmatch.
      + pose proof (strict_match_requires_present
          k (left_key l) (right_key r) Hstrict Hmatch) as [Hleft _].
        rewrite Hnull in Hleft. discriminate.
      + exact IH.
  Qed.

  Lemma inner_payload_right_guard :
    forall k left right, strict k = true ->
      inner_payload k left (filter (fun r => present (right_key r)) right) =
      inner_payload k left right.
  Proof.
    intros k left right Hstrict.
    unfold inner_payload.
    induction left as [|l ls IH].
    - reflexivity.
    - simpl.
      rewrite (right_guard_preserves_matches k l right Hstrict).
      now rewrite IH.
  Qed.

  Lemma inner_payload_left_guard :
    forall k left right, strict k = true ->
      inner_payload k (filter (fun l => present (left_key l)) left) right =
      inner_payload k left right.
  Proof.
    intros k left right Hstrict.
    unfold inner_payload.
    induction left as [|l ls IH].
    - reflexivity.
    - simpl.
      destruct (present (left_key l)) eqn:Hpresent.
      + simpl. now rewrite IH.
      + simpl.
        rewrite (no_matches_when_left_key_missing
          k l right Hstrict Hpresent).
        simpl. exact IH.
  Qed.

  Lemma outer_payload_right_guard :
    forall k left right, strict k = true ->
      outer_payload k left (filter (fun r => present (right_key r)) right) =
      outer_payload k left right.
  Proof.
    intros k left right Hstrict.
    unfold outer_payload.
    induction left as [|l ls IH].
    - reflexivity.
    - simpl.
      assert (Hstep : outer_step k l
        (filter (fun r => present (right_key r)) right) =
         outer_step k l right).
      { unfold outer_step.
        now rewrite (right_guard_preserves_matches k l right Hstrict). }
      now rewrite Hstep, IH.
  Qed.

  Definition guarded_left_rows (c : GuardCertificate) (left : list L) :=
    if filter_left c then filter (fun l => present (left_key l)) left
    else left.
  Definition guarded_right_rows (c : GuardCertificate) (right : list R) :=
    if filter_right c then filter (fun r => present (right_key r)) right
    else right.

  Definition run_payload (c : GuardCertificate) (left : list L)
      (right : list R) : list (L * option R) :=
    match join_kind c with
    | Inner => inner_payload (comparison c) left right
    | LeftOuter => outer_payload (comparison c) left right
    end.

  Lemma inner_all_flags :
    forall k (gl gr : bool) left right, strict k = true ->
      inner_payload k
        (if gl then filter (fun l => present (left_key l)) left else left)
        (if gr then filter (fun r => present (right_key r)) right else right) =
      inner_payload k left right.
  Proof.
    intros k [|] [|] left right Hstrict; simpl.
    - rewrite (inner_payload_left_guard k left
        (filter (fun r => present (right_key r)) right) Hstrict).
      apply inner_payload_right_guard. exact Hstrict.
    - now apply inner_payload_left_guard.
    - now apply inner_payload_right_guard.
    - reflexivity.
  Qed.

  Lemma outer_right_flag :
    forall k (gr : bool) left right, strict k = true ->
      outer_payload k left
        (if gr then filter (fun r => present (right_key r)) right else right) =
      outer_payload k left right.
  Proof.
    intros k [|] left right Hstrict; simpl.
    - now apply outer_payload_right_guard.
    - reflexivity.
  Qed.

  Theorem checked_certificate_preserves_entire_payload_bags :
    forall c left right,
      check c = true ->
      run_payload c (guarded_left_rows c left)
                    (guarded_right_rows c right) =
      run_payload c left right.
  Proof.
    intros [k j gl gr] left right Hchecked.
    unfold check in Hchecked.
    unfold run_payload, guarded_left_rows, guarded_right_rows.
    simpl in *.
    destruct j.
    - rewrite andb_true_r in Hchecked.
      now apply inner_all_flags.
    - destruct gl; simpl in *.
      + rewrite andb_false_r in Hchecked. discriminate.
      + rewrite andb_true_r in Hchecked.
        now apply outer_right_flag.
  Qed.
End ArbitraryRowPayload.
