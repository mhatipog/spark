(** V9 developmental SQL bag lemma for nullable SIGNED integers.
    A precise companion to observed Spark 4.2.0 strict-LessThan join transformations.
    No Catalyst AST-to-semantics bridge or JVM-state noninterference proof.
    List output equality is stronger than bag equality, and duplicates stay present.
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool.
Import ListNotations.

Definition nullable_int := option Z.
Definition present (x : nullable_int) : bool :=
  match x with Some _ => true | None => false end.

Definition sql_lt_true (x y : nullable_int) : bool :=
  match x, y with
  | Some a, Some b => Z.ltb a b
  | _, _ => false
  end.

Lemma filter_guard_implied :
  forall (A : Type) (predicate guard : A -> bool) rows,
    (forall x, predicate x = true -> guard x = true) ->
    filter predicate (filter guard rows) = filter predicate rows.
Proof.
  intros A predicate guard rows H.
  induction rows as [| x xs IH].
  - reflexivity.
  - simpl. destruct (guard x) eqn:Hg; simpl.
    + destruct (predicate x); simpl; now rewrite IH.
    + destruct (predicate x) eqn:Hp.
      * specialize (H x Hp). rewrite H in Hg. discriminate.
      * exact IH.
Qed.

Lemma strict_lt_requires_right_present :
  forall x y, sql_lt_true x y = true -> present y = true.
Proof.
  intros [a|] [b|] H; simpl in *; try discriminate; reflexivity.
Qed.

Lemma strict_lt_requires_left_present :
  forall x y, sql_lt_true x y = true -> present x = true.
Proof.
  intros [a|] [b|] H; simpl in *; try discriminate; reflexivity.
Qed.

Lemma right_guard_preserves_lt_matches :
  forall x ys,
    filter (sql_lt_true x) (filter present ys) =
    filter (sql_lt_true x) ys.
Proof.
  intros x ys.
  apply filter_guard_implied.
  intros y Hy.
  now apply strict_lt_requires_right_present with (x := x).
Qed.

Lemma left_guard_preserves_lt_matches :
  forall y xs,
    filter (fun x => sql_lt_true x y) (filter present xs) =
    filter (fun x => sql_lt_true x y) xs.
Proof.
  intros y xs.
  apply filter_guard_implied.
  intros x Hx.
  now apply strict_lt_requires_left_present with (y := y).
Qed.

Definition inner_lt (left right : list nullable_int) :
    list (nullable_int * nullable_int) :=
  flat_map
    (fun x => map (fun y => (x, y)) (filter (sql_lt_true x) right))
    left.

Theorem inner_lt_right_guard_preserves_all_rows :
  forall left right,
    inner_lt left (filter present right) = inner_lt left right.
Proof.
  intros left right.
  unfold inner_lt.
  induction left as [| x xs IH].
  - reflexivity.
  - simpl. rewrite right_guard_preserves_lt_matches.
    now rewrite IH.
Qed.

Definition left_outer_lt_step (x : nullable_int) (right : list nullable_int) :
    list (nullable_int * option nullable_int) :=
  match filter (sql_lt_true x) right with
  | [] => [(x, None)]
  | matched => map (fun y => (x, Some y)) matched
  end.

Definition left_outer_lt (left right : list nullable_int) :
    list (nullable_int * option nullable_int) :=
  flat_map (fun x => left_outer_lt_step x right) left.

Theorem left_outer_lt_right_guard_preserves_padded_rows :
  forall left right,
    left_outer_lt left (filter present right) = left_outer_lt left right.
Proof.
  intros left right.
  unfold left_outer_lt.
  induction left as [| x xs IH].
  - reflexivity.
  - simpl.
    assert (Hstep : left_outer_lt_step x (filter present right) =
                    left_outer_lt_step x right).
    { unfold left_outer_lt_step.
      now rewrite right_guard_preserves_lt_matches. }
    now rewrite Hstep, IH.
Qed.

Definition nullsafe_eq (x y : nullable_int) : bool :=
  match x, y with
  | None, None => true
  | Some a, Some b => Z.eqb a b
  | _, _ => false
  end.

Example nullsafe_comparison_does_not_support_the_guard :
  filter (nullsafe_eq None) [None] <>
  filter (nullsafe_eq None) (filter present [None]).
Proof. discriminate. Qed.

Example left_outer_preserved_side_cannot_be_filtered :
  left_outer_lt [None] [Some 0%Z] <>
  left_outer_lt (filter present [None]) [Some 0%Z].
Proof. discriminate. Qed.

Example signed_duplicate_multiplicities_are_preserved :
  inner_lt [Some (-1)%Z; Some (-1)%Z] [Some 0%Z; Some 0%Z] =
  [(Some (-1)%Z, Some 0%Z); (Some (-1)%Z, Some 0%Z);
   (Some (-1)%Z, Some 0%Z); (Some (-1)%Z, Some 0%Z)].
Proof. reflexivity. Qed.
