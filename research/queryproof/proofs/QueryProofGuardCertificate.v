(** V10: executable certificate checker for a narrow signed nullable SQL bag fragment.
    UNIVERSAL over finite input bags; NOT a proof of Catalyst parsing,
    Spark interpreter fidelity, metadata/tags, errors or hidden JVM state.
    Observational JSON is not assumed injective.
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool.
Import ListNotations.

Definition Nullable := option Z.
Definition present (x : Nullable) : bool :=
  match x with Some _ => true | None => false end.

Inductive Comparator := StrictEq | StrictLt | NullSafeEq.
Inductive JoinKind := Inner | LeftOuter.
Definition strict (k : Comparator) : bool :=
  match k with StrictEq | StrictLt => true | NullSafeEq => false end.

Definition cmp_true (k : Comparator) (a b : Nullable) : bool :=
  match k with
  | StrictEq => match a,b with Some x, Some y => Z.eqb x y | _,_ => false end
  | StrictLt => match a,b with Some x, Some y => Z.ltb x y | _,_ => false end
  | NullSafeEq => match a,b with
                  | None,None => true
                  | Some x,Some y => Z.eqb x y
                  | _,_ => false end
  end.

Lemma strict_match_requires_present :
  forall k a b, strict k = true -> cmp_true k a b = true ->
  present a = true /\ present b = true.
Proof.
  intros k [x|] [y|] Hstrict Hmatch;
    destruct k; simpl in *; try discriminate;
    split; reflexivity.
Qed.

Lemma filter_implied_guard :
  forall (A : Type) (p g : A -> bool) xs,
    (forall x, p x = true -> g x = true) ->
    filter p (filter g xs) = filter p xs.
Proof.
  intros A p g xs Himp.
  induction xs as [|x rest IH].
  - reflexivity.
  - simpl. destruct (g x) eqn:Hg; simpl.
    + destruct (p x); simpl; now rewrite IH.
    + destruct (p x) eqn:Hp.
      * specialize (Himp x Hp). rewrite Himp in Hg. discriminate.
      * exact IH.
Qed.

Lemma right_match_filter :
  forall k a ys, strict k = true ->
    filter (cmp_true k a) (filter present ys) =
    filter (cmp_true k a) ys.
Proof.
  intros k a ys Hstrict.
  apply filter_implied_guard.
  intros b Hb. now apply (proj2 (strict_match_requires_present k a b Hstrict Hb)).
Qed.

Lemma no_matches_on_null_left :
  forall k ys, strict k = true -> filter (cmp_true k None) ys = [].
Proof.
  intros k ys Hstrict.
  induction ys as [|b rest IH].
  - reflexivity.
  - simpl.
    assert (Hfalse : cmp_true k None b = false).
    { destruct (cmp_true k None b) eqn:Hc; try reflexivity.
      pose proof (strict_match_requires_present k None b Hstrict Hc) as H.
      destruct H as [H _]. simpl in H. discriminate. }
    now rewrite Hfalse, IH.
Qed.

Definition PairBag := list (Nullable * option Nullable).
Definition inner (k : Comparator) (left right : list Nullable) : PairBag :=
  flat_map (fun a => map (fun b => (a, Some b))
                       (filter (cmp_true k a) right)) left.
Definition left_step (k : Comparator) (a : Nullable)
                     (right : list Nullable) : PairBag :=
  match filter (cmp_true k a) right with
  | [] => [(a,None)]
  | matches => map (fun b => (a,Some b)) matches
  end.
Definition left_outer (k : Comparator) (left right : list Nullable) : PairBag :=
  flat_map (fun a => left_step k a right) left.

Lemma inner_right_guard :
  forall k left right, strict k = true ->
  inner k left (filter present right) = inner k left right.
Proof.
  intros k left right Hstrict.
  unfold inner.
  induction left as [|a rest IH].
  - reflexivity.
  - simpl. rewrite (right_match_filter k a right Hstrict).
    now rewrite IH.
Qed.

Lemma inner_left_guard :
  forall k left right, strict k = true ->
  inner k (filter present left) right = inner k left right.
Proof.
  intros k left right Hstrict.
  unfold inner.
  induction left as [|a rest IH].
  - reflexivity.
  - destruct a as [z|].
    + simpl. now rewrite IH.
    + simpl. rewrite (no_matches_on_null_left k right Hstrict).
      simpl. exact IH.
Qed.

Lemma outer_right_guard :
  forall k left right, strict k = true ->
  left_outer k left (filter present right) = left_outer k left right.
Proof.
  intros k left right Hstrict.
  induction left as [|a rest IH].
  - reflexivity.
  - change (left_step k a (filter present right) ++
            left_outer k rest (filter present right) =
            left_step k a right ++ left_outer k rest right).
    rewrite IH.
    unfold left_step.
    rewrite (right_match_filter k a right Hstrict).
    reflexivity.
Qed.

Record GuardCertificate := {
  comparison : Comparator;
  join_kind : JoinKind;
  filter_left : bool;
  filter_right : bool
}.
Definition check (c : GuardCertificate) : bool :=
  strict (comparison c) &&
  match join_kind c with
  | Inner => true
  | LeftOuter => negb (filter_left c)
  end.
Definition guarded_left (c : GuardCertificate) (xs : list Nullable) :=
  if filter_left c then filter present xs else xs.
Definition guarded_right (c : GuardCertificate) (ys : list Nullable) :=
  if filter_right c then filter present ys else ys.
Definition execute (c : GuardCertificate) (xs ys : list Nullable) : PairBag :=
  match join_kind c with
  | Inner => inner (comparison c) xs ys
  | LeftOuter => left_outer (comparison c) xs ys
  end.

Lemma inner_guard_flags :
  forall k (gl gr : bool) left right, strict k = true ->
    inner k (if gl then filter present left else left)
            (if gr then filter present right else right) =
    inner k left right.
Proof.
  intros k [|] [|] left right Hstrict; simpl.
  - rewrite (inner_left_guard k left (filter present right) Hstrict).
    apply inner_right_guard; assumption.
  - now apply inner_left_guard.
  - now apply inner_right_guard.
  - reflexivity.
Qed.

Lemma outer_guard_flag :
  forall k (gr : bool) left right, strict k = true ->
    left_outer k left (if gr then filter present right else right) =
    left_outer k left right.
Proof.
  intros k [|] left right Hstrict; simpl.
  - now apply outer_right_guard.
  - reflexivity.
Qed.

Theorem checked_certificate_sound :
  forall c xs ys,
    check c = true ->
    execute c (guarded_left c xs) (guarded_right c ys) =
    execute c xs ys.
Proof.
  intros [k j gl gr] xs ys Hchecked.
  unfold check in Hchecked.
  unfold execute, guarded_left, guarded_right.
  simpl in *.
  destruct j.
  - rewrite andb_true_r in Hchecked.
    apply inner_guard_flags. exact Hchecked.
  - destruct gl; simpl in *.
    + rewrite andb_false_r in Hchecked. discriminate.
    + rewrite andb_true_r in Hchecked.
      now apply outer_guard_flag.
Qed.

Example true_inner_inequality_certificate :
  check {| comparison := StrictLt; join_kind := Inner;
           filter_left := true; filter_right := true |} = true.
Proof. reflexivity. Qed.
Example reject_outer_preserved_side_filter :
  check {| comparison := StrictLt; join_kind := LeftOuter;
           filter_left := true; filter_right := true |} = false.
Proof. reflexivity. Qed.
Example reject_null_safe_equijoin_guards :
  check {| comparison := NullSafeEq; join_kind := Inner;
           filter_left := true; filter_right := true |} = false.
Proof. reflexivity. Qed.
