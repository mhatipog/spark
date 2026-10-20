(** QueryProof: effect-indexed, proof-carrying optimizer trace calculus.
    Development calculus, not a verified interpretation of Catalyst ASTs.
    No axioms, admits, or proof-engine shortcuts.

    Key point: a post-Finish-Analysis proof must check TWO independent
    obligations for every step: a semantics-preserving AST rewrite AT THE
    STARTING HIDDEN STATE, and semantic noninterference of any hidden-state
    change AT THE RESULTING PLAN. A lossy observer is sufficient only if its
    equality is a proven semantic congruence for the relevant plan fragment.
*)
From Coq Require Import Bool.Bool.

Record Machine (Plan Hidden : Type) : Type := {
  mPlan : Plan;
  mHidden : Hidden
}.
Arguments mPlan {Plan Hidden} _.
Arguments mHidden {Plan Hidden} _.

Section ProofCarryingTrace.
  Context (Plan Hidden Answer : Type).
  Variable sem : Hidden -> Plan -> Answer.

  Definition local_rewrite_preserves (s t : Machine Plan Hidden) : Prop :=
    sem (mHidden s) (mPlan s) = sem (mHidden s) (mPlan t).

  Definition effect_noninterference (s t : Machine Plan Hidden) : Prop :=
    sem (mHidden s) (mPlan t) = sem (mHidden t) (mPlan t).

  Definition state_meaning (s : Machine Plan Hidden) : Answer :=
    sem (mHidden s) (mPlan s).

  Inductive certified_suffix : Machine Plan Hidden ->
                               Machine Plan Hidden -> Prop :=
    | suffix_done : forall s, certified_suffix s s
    | suffix_step : forall s middle finish,
        local_rewrite_preserves s middle ->
        effect_noninterference s middle ->
        certified_suffix middle finish ->
        certified_suffix s finish.

  Theorem certified_suffix_sound :
    forall s finish,
      certified_suffix s finish ->
      state_meaning s = state_meaning finish.
  Proof.
    intros s finish Hcertificate.
    induction Hcertificate as
      [s | s middle finish Hrewrite Heffect Hrest IH].
    - reflexivity.
    - unfold state_meaning in *.
      unfold local_rewrite_preserves in Hrewrite.
      unfold effect_noninterference in Heffect.
      rewrite Hrewrite.
      rewrite Heffect.
      exact IH.
  Qed.

  (** The proof never refers to a state BEFORE the certified suffix. *)
  Corollary finish_analysis_cut_is_prefix_independent :
    forall (arbitrary_analyzer_state cut end_state : Machine Plan Hidden),
      certified_suffix cut end_state ->
      state_meaning cut = state_meaning end_state.
  Proof.
    intros analyzer cut last H.
    now apply certified_suffix_sound.
  Qed.

  (** Identity of all hidden bytes is unnecessarily strong: for an
      individual result plan, a proven semantic congruence of the observer
      suffices. But observed equality ALONE is not such a proof. *)
  Theorem sound_observation_congruence :
    forall (Obs : Type) (observe : Hidden -> Obs)
           (s t : Machine Plan Hidden),
      local_rewrite_preserves s t ->
      (forall h1 h2 p, observe h1 = observe h2 ->
         sem h1 p = sem h2 p) ->
      observe (mHidden s) = observe (mHidden t) ->
      state_meaning s = state_meaning t.
  Proof.
    intros Obs observe s t Hrewrite Hcongruence Hobs.
    unfold state_meaning, local_rewrite_preserves in *.
    rewrite Hrewrite.
    now apply Hcongruence.
  Qed.
End ProofCarryingTrace.

(** Executable Boolean countermodel for snapshot/hash equivalence.
    The observer conflates different hidden states; the plans and observed
    digests can be IDENTICAL even though their meanings are different. *)
Definition false_observer (_ : bool) : bool := false.
Definition hidden_sensitive_meaning (h p : bool) : bool := xorb h p.

Example equal_observer_and_plan_do_not_imply_semantic_equality :
  exists (s t : Machine bool bool),
    mPlan s = mPlan t /\
    false_observer (mHidden s) = false_observer (mHidden t) /\
    hidden_sensitive_meaning (mHidden s) (mPlan s) <>
    hidden_sensitive_meaning (mHidden t) (mPlan t).
Proof.
  exists {| mPlan := false; mHidden := false |}.
  exists {| mPlan := false; mHidden := true |}.
  simpl.
  repeat split; try reflexivity.
  discriminate.
Qed.

(** A fragment whose semantics does NOT depend on hidden state needs no
    injective hidden-state observer at all. This is the positive path for
    future source-proven tag/metadata noninterference of selected rules. *)
Definition hidden_insensitive_meaning (_ : bool) (p : bool) := p.

Example tag_independent_fragment_satisfies_any_observation_congruence :
  forall (ob : bool -> bool) (h1 h2 p : bool),
    ob h1 = ob h2 ->
    hidden_insensitive_meaning h1 p =
    hidden_insensitive_meaning h2 p.
Proof.
  intros. reflexivity.
Qed.
