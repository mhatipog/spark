(*
 QueryProof predicate-parametric join-frame theorem (development).
 Independent of Catalyst parsing, provenance, hidden JVM state and runtime
 effects. Rows may repeat; lists preserve full bag multiplicity. A predicate
 is an arbitrary *pure and total* Boolean function of the retained columns.

 The theorem says that an input identity/dependency projection can commute
 with INNER and LEFT OUTER joins when the entire original join predicate
 factors through the retained attributes. SQL NULL/OR/< cases are included
 only if their real predicates satisfy that factorization side condition.
*)
From Coq Require Import Lists.List.
Import ListNotations.

Lemma filter_map_factor :
  forall (A B : Type) (f : A -> B) (p : B -> bool) rows,
    filter p (map f rows) =
    map f (filter (fun x => p (f x)) rows).
Proof.
  intros A B f p rows; induction rows as [|x xs IH].
  - reflexivity.
  - simpl. destruct (p (f x)); simpl; rewrite IH; reflexivity.
Qed.

Definition generic_inner {L R : Type}
    (p : L -> R -> bool) (left : list L) (right : list R) :
    list (L * R) :=
  flat_map (fun l => map (fun r => (l, r)) (filter (p l) right)) left.

Lemma inner_step_frame :
  forall (L R A B : Type) (pl : L -> A) (pr : R -> B)
         (predicate : A -> B -> bool) l right,
    map (fun pair => (pl (fst pair), pr (snd pair)))
        (map (fun r => (l, r))
             (filter (fun r => predicate (pl l) (pr r)) right)) =
    map (fun b => (pl l, b))
        (filter (predicate (pl l)) (map pr right)).
Proof.
  intros L R A B pl pr predicate l right.
  rewrite filter_map_factor.
  rewrite !map_map.
  reflexivity.
Qed.

Theorem inner_projection_frame :
  forall (L R A B : Type) (pl : L -> A) (pr : R -> B)
         (predicate : A -> B -> bool) left right,
    map (fun pair => (pl (fst pair), pr (snd pair)))
        (generic_inner (fun l r => predicate (pl l) (pr r)) left right) =
    generic_inner predicate (map pl left) (map pr right).
Proof.
  intros L R A B pl pr predicate left right.
  unfold generic_inner in *.
  induction left as [|l ls IH].
  - reflexivity.
  - simpl. rewrite map_app.
    rewrite inner_step_frame.
    rewrite IH.
    reflexivity.
Qed.

Definition generic_left_step {L R : Type}
    (p : L -> R -> bool) (l : L) (right : list R) :
    list (L * option R) :=
  match filter (p l) right with
  | [] => [(l, None)]
  | matched => map (fun r => (l, Some r)) matched
  end.

Definition generic_left {L R : Type}
    (p : L -> R -> bool) (left : list L) (right : list R) :
    list (L * option R) :=
  flat_map (fun l => generic_left_step p l right) left.

Lemma left_step_frame :
  forall (L R A B : Type) (pl : L -> A) (pr : R -> B)
         (predicate : A -> B -> bool) l right,
    map (fun pair => (pl (fst pair), option_map pr (snd pair)))
        (generic_left_step (fun x y => predicate (pl x) (pr y)) l right) =
    generic_left_step predicate (pl l) (map pr right).
Proof.
  intros L R A B pl pr predicate l right.
  unfold generic_left_step.
  rewrite filter_map_factor.
  remember (filter (fun r => predicate (pl l) (pr r)) right)
    as matches eqn:HM.
  destruct matches as [|r more].
  - reflexivity.
  - simpl. rewrite !map_map. reflexivity.
Qed.

Theorem left_outer_projection_frame :
  forall (L R A B : Type) (pl : L -> A) (pr : R -> B)
         (predicate : A -> B -> bool) left right,
    map (fun pair => (pl (fst pair), option_map pr (snd pair)))
        (generic_left (fun l r => predicate (pl l) (pr r)) left right) =
    generic_left predicate (map pl left) (map pr right).
Proof.
  intros L R A B pl pr predicate left right.
  unfold generic_left in *.
  induction left as [|l ls IH].
  - reflexivity.
  - simpl. rewrite map_app.
    rewrite left_step_frame.
    rewrite IH.
    reflexivity.
Qed.
