(*
 QueryProof: narrow, standalone Coq semantics lemmas.
 Development branch only. Not a verification of Spark's Java/Scala code,
 the Catalyst-to-Coq translation, physical execution, or the W32 certificates.

 Lists represent finite bags with multiplicities via repeated occurrences.
 Equality of these lists is stronger than row-bag equality; output order is
 only a device for the proof. Keys are nullable naturals, and SQL comparison
 returns TRUE exactly for equal non-NULL keys. Non-TRUE rows are rejected.
*)
From Coq Require Import Lists.List Arith.PeanoNat Bool.Bool.
Import ListNotations.

Definition key := option nat.

Definition nonnull (x : key) : bool :=
  match x with
  | Some _ => true
  | None => false
  end.

Definition sql_eq_true (x y : key) : bool :=
  match x, y with
  | Some a, Some b => Nat.eqb a b
  | _, _ => false
  end.

Lemma sql_equality_requires_right_nonnull :
  forall x y, sql_eq_true x y = true -> nonnull y = true.
Proof.
  intros [a|] [b|] H; simpl in *; try discriminate; reflexivity.
Qed.

Lemma sql_equality_requires_left_nonnull :
  forall x y, sql_eq_true x y = true -> nonnull x = true.
Proof.
  intros [a|] [b|] H; simpl in *; try discriminate; reflexivity.
Qed.

Lemma right_null_guard_preserves_matches :
  forall x right,
    filter (sql_eq_true x) (filter nonnull right) =
    filter (sql_eq_true x) right.
Proof.
  intros x right; induction right as [|y ys IH].
  - reflexivity.
  - destruct x as [a|], y as [b|]; simpl; rewrite IH; reflexivity.
Qed.

Lemma left_null_guard_preserves_matches :
  forall y left,
    filter (fun x => sql_eq_true x y) (filter nonnull left) =
    filter (fun x => sql_eq_true x y) left.
Proof.
  intros y left; induction left as [|x xs IH].
  - reflexivity.
  - destruct x as [a|], y as [b|]; simpl; rewrite IH; reflexivity.
Qed.

Definition inner_join (left right : list key) : list (key * key) :=
  flat_map
    (fun x => map (fun y => (x, y)) (filter (sql_eq_true x) right))
    left.

Theorem inner_join_right_nonnull :
  forall left right,
    inner_join left (filter nonnull right) = inner_join left right.
Proof.
  intros left right; unfold inner_join.
  induction left as [|x xs IH].
  - reflexivity.
  - simpl. rewrite right_null_guard_preserves_matches.
    rewrite IH. reflexivity.
Qed.

(* Left outer rows are padded with None : option key, distinct from
   Some None, the representation of a hypothetical matched NULL right key. *)
Definition left_outer_step (x : key) (right : list key) :
    list (key * option key) :=
  match filter (sql_eq_true x) right with
  | [] => [(x, None)]
  | ys => map (fun y => (x, Some y)) ys
  end.

Definition left_outer_join (left right : list key) :
    list (key * option key) :=
  flat_map (fun x => left_outer_step x right) left.

Theorem left_outer_right_nonnull :
  forall left right,
    left_outer_join left (filter nonnull right) =
    left_outer_join left right.
Proof.
  intros left right; induction left as [|x xs IH].
  - reflexivity.
  - change (left_outer_step x (filter nonnull right) ++
            left_outer_join xs (filter nonnull right) =
            left_outer_step x right ++ left_outer_join xs right).
    rewrite IH.
    unfold left_outer_step.
    rewrite right_null_guard_preserves_matches.
    reflexivity.
Qed.

(* A minimal NULL counterexample: filtering the preserved LEFT input is NOT safe. *)
Example left_outer_preserved_side_guard_is_wrong :
  left_outer_join [None] [Some 0] <>
  left_outer_join (filter nonnull [None]) [Some 0].
Proof. discriminate. Qed.

Definition nullsafe_eq (x y : key) : bool :=
  match x, y with
  | None, None => true
  | Some a, Some b => Nat.eqb a b
  | _, _ => false
  end.

(* The null-intolerance lemma does NOT extend to null-safe equality. *)
Example nullsafe_join_guard_is_wrong :
  filter (nullsafe_eq None) [None] <>
  filter (nullsafe_eq None) (filter nonnull [None]).
Proof. discriminate. Qed.

(* Duplicates are not collapsed by join-filter rewrites. *)
Example exact_duplicate_multiplicity_control :
  inner_join [Some 1; Some 1] [Some 1; Some 1] =
  [(Some 1, Some 1); (Some 1, Some 1);
   (Some 1, Some 1); (Some 1, Some 1)].
Proof. reflexivity. Qed.

(* The same filter/projection naturality holds for any pure, total f.
   Catalyst admissibility still needs a checked binding/totality side condition. *)
Theorem filter_map_commutes :
  forall (A B : Type) (f : A -> B) (p : B -> bool) rows,
    filter p (map f rows) =
    map f (filter (fun x => p (f x)) rows).
Proof.
  intros A B f p rows; induction rows as [|x xs IH].
  - reflexivity.
  - simpl. destruct (p (f x)); simpl; rewrite IH; reflexivity.
Qed.

(* Grouping NULLs have reflexive equality, unlike WHERE's three-valued
   SQL predicate. Pure, total key expressions induce partitions by equality.
   This proves duplicate-key idempotence at the equivalence-relation level;
   COUNT/SUM evaluator correctness is a separate unproved bridge. *)
Section DuplicateGroupingKey.
  Variable row : Type.
  Variables f g : row -> key.

  Definition same_group_fg (x y : row) : Prop :=
    f x = f y /\\ g x = g y.

  Definition same_group_ffg (x y : row) : Prop :=
    f x = f y /\\ f x = f y /\\ g x = g y.

  Theorem duplicate_total_key_partition :
    forall x y, same_group_ffg x y <-> same_group_fg x y.
  Proof.
    intros x y; unfold same_group_ffg, same_group_fg; tauto.
  Qed.
End DuplicateGroupingKey.
