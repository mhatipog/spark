(** QueryProof: rule memoization is a control-flow effect, not necessarily
    a semantic effect. Coq 8.18, kernel-checked, no unsafe axioms.

    Motivated by a native Spark 4.2.0 reproduction of TreeNode's mutable
    _ineffectiveRules cache (run 39452186486). This is an abstract calculus,
    NOT a mechanically checked model of Spark's Scala implementation.
*)
From Coq Require Import Bool.Bool.

Section PureMemoization.
  Variables (Plan Answer : Type).
  Variable meaning : Plan -> Answer.
  Variable rewrite : Plan -> Plan.

  Definition cached_apply (skip : bool) (p : Plan) : Plan :=
    if skip then p else rewrite p.

  (** A cached skip is observationally equivalent to actually running the
      rule only when the cache fact "rule is ineffective on p" is valid.
      This is stronger than preservation of SQL meaning. *)
  Theorem valid_cache_replays_exact_plan :
    forall (skip : bool) (p : Plan),
      (skip = true -> rewrite p = p) ->
      cached_apply skip p = rewrite p.
  Proof.
    intros skip p Hvalid.
    destruct skip.
    - simpl. symmetry. apply Hvalid. reflexivity.
    - reflexivity.
  Qed.

  (** For SQL meaning preservation, a weaker condition suffices:
      every execution of the rule is semantics-preserving. Then ANY
      skip decision preserves meaning even if the optimization does not
      produce the same syntactic plan as an uncached execution. *)
  Theorem semantically_sound_rewrite_makes_any_cache_safe :
    (forall p, meaning (rewrite p) = meaning p) ->
    forall (skip : bool) (p : Plan),
      meaning (cached_apply skip p) = meaning p.
  Proof.
    intros Hsound skip p.
    destruct skip.
    - reflexivity.
    - simpl. apply Hsound.
  Qed.

  (** Under the same premise, both execution policies are equivalent
      with respect to denotation although their ASTs might differ. *)
  Theorem cache_policy_is_semantically_irrelevant :
    (forall p, meaning (rewrite p) = meaning p) ->
    forall (a b : bool) (p : Plan),
      meaning (cached_apply a p) = meaning (cached_apply b p).
  Proof.
    intros Hsound a b p.
    transitivity (meaning p).
    - now apply semantically_sound_rewrite_makes_any_cache_safe.
    - symmetry. now apply semantically_sound_rewrite_makes_any_cache_safe.
  Qed.
End PureMemoization.

(** Exact identity cache validity can fail when the rewrite reads changing
    external state. This is why Spark's RuleIdCollection requires pure,
    externally stable rules. No conclusion about production-rule defects
    follows from this countermodel. *)
Definition varying_rule (h p : bool) : bool :=
  if h then negb p else p.

Example externally_varying_rule_breaks_cached_exact_reexecution :
  varying_rule false false = false /\
  varying_rule true false <> false.
Proof.
  simpl. split.
  - reflexivity.
  - discriminate.
Qed.
