(** QueryProof: a deliberately narrow formalization of Spark's
    ConstantFolding.FAILED_TO_EVALUATE control state.

    Production source: Apache Spark 4.2.0 expressions.scala, Git blob
    929ffdef2b52ff1aabdc6f38501dc131e7938fd2.
    Native causal counterfactual: Actions run 39457131853.

    This models only a pure foldable expression within a conditional branch:
    untagged -> try static evaluation; failure -> mark and keep original;
    tagged -> skip future attempted static evaluation.

    SqlVal is option nat: None means SQL NULL, Some n is an integer.
    StaticEval is option SqlVal: None means an evaluation failure, while
    Some None is a successful fold to SQL NULL. The separation matters.
    Runtime exceptions, nondeterminism, 3VL, Spark's AST-to-Coq bridge,
    and generic optimizer-rule correctness are NOT established here.

    All proofs use Coq's kernel and explicit stated assumptions only.
*)
From Coq Require Import Bool.Bool.
From Coq Require Import Arith.PeanoNat.

Definition SqlVal := option nat.
Definition StaticEval := option SqlVal.
Definition Row := nat.

Record Fragment := {
  runtime_eval : Row -> SqlVal;
  compile_eval : StaticEval
}.

Inductive FoldView :=
  | OriginalExpr
  | FoldedLiteral (v : SqlVal).

Record FoldState := {
  visible_view : FoldView;
  failed_tag : bool;
  attempt_count : nat
}.

Definition initial : FoldState :=
  {| visible_view := OriginalExpr;
     failed_tag := false;
     attempt_count := 0 |}.

Definition meaning (e : Fragment) (s : FoldState) (r : Row) : SqlVal :=
  match visible_view s with
  | OriginalExpr => runtime_eval e r
  | FoldedLiteral v => v
  end.

Definition fold_step (e : Fragment) (s : FoldState) : FoldState :=
  if failed_tag s then s
  else match visible_view s with
       | FoldedLiteral _ => s
       | OriginalExpr =>
           match compile_eval e with
           | Some v =>
               {| visible_view := FoldedLiteral v;
                  failed_tag := false;
                  attempt_count := S (attempt_count s) |}
           | None =>
               {| visible_view := OriginalExpr;
                  failed_tag := true;
                  attempt_count := S (attempt_count s) |}
           end
       end.

Definition clear_failure_tag (s : FoldState) : FoldState :=
  {| visible_view := visible_view s;
     failed_tag := false;
     attempt_count := attempt_count s |}.

Definition tag_override (s : FoldState) (new_tag : bool) : FoldState :=
  {| visible_view := visible_view s;
     failed_tag := new_tag;
     attempt_count := attempt_count s |}.

(** This is the essential *unproven-for-arbitrary-Spark-expressions*
    compiler correctness obligation: successful compile-time evaluation
    must agree with all possible run-time row evaluations. *)
Definition statically_sound (e : Fragment) : Prop :=
  forall v, compile_eval e = Some v ->
    forall r, runtime_eval e r = v.

Theorem failure_tag_write_does_not_change_runtime_denotation :
  forall e s b r,
    meaning e (tag_override s b) r = meaning e s r.
Proof.
  intros e [v t n] b r.
  destruct v; reflexivity.
Qed.

Theorem folding_step_preserves_denotation :
  forall e, statically_sound e ->
  forall s r, meaning e (fold_step e s) r = meaning e s r.
Proof.
  intros e Hsound [view tag attempts] r.
  unfold fold_step.
  destruct tag; simpl; [reflexivity|].
  destruct view; simpl; [|reflexivity].
  destruct (compile_eval e) as [v|] eqn:Hcompile; simpl.
  - unfold meaning; simpl.
    apply (Hsound v Hcompile r).
  - reflexivity.
Qed.

Theorem two_consecutive_optimizations_preserve_denotation :
  forall e, statically_sound e ->
  forall s r, meaning e (fold_step e (fold_step e s)) r = meaning e s r.
Proof.
  intros e Hsound s r.
  rewrite (folding_step_preserves_denotation e Hsound (fold_step e s) r).
  apply folding_step_preserves_denotation; exact Hsound.
Qed.

(** The visible view remains ORIGINAL in the deliberately failing case,
    but the hidden tag has a repeatable, experimentally observed control
    effect. Clearing ONLY the tag resurrects the evaluation attempt. *)
Theorem failed_fold_changes_next_decision_not_denotation :
  forall e, compile_eval e = None ->
  let first := fold_step e initial in
  visible_view first = OriginalExpr /\
  failed_tag first = true /\
  attempt_count first = 1 /\
  fold_step e first = first /\
  attempt_count (fold_step e (clear_failure_tag first)) = 2.
Proof.
  intros e Hfailure.
  unfold fold_step, initial, clear_failure_tag.
  simpl.
  rewrite Hfailure.
  simpl.
  repeat split; reflexivity.
Qed.

(** Without static-evaluation soundness, a literal substitution can
    indeed be wrong even though the compiler merely "folded a constant".
    Counterexample intentionally NOT a legal foldable Spark expression. *)
Definition unsound_static_fragment : Fragment :=
  {| runtime_eval := fun r => if Nat.eqb r 0 then None else Some 7;
     compile_eval := Some None |}.

Example unsound_static_evaluator_breaks_folding_at_row_one :
  meaning unsound_static_fragment (fold_step unsound_static_fragment initial) 1
  <> meaning unsound_static_fragment initial 1.
Proof.
  simpl. discriminate.
Qed.

(** Equally, the tag-inertness theorem is contingent on runtime
    evaluation ignoring the hidden tag. The following deliberately bad
    evaluator reads it; then even an IDENTICAL AST changes meaning. *)
Definition hidden_sensitive_runtime (s : FoldState) : SqlVal :=
  if failed_tag s then Some 0 else Some 1.

Example identical_visible_view_does_not_save_hidden_sensitive_evaluation :
  visible_view initial = visible_view (tag_override initial true) /\
  hidden_sensitive_runtime initial <>
  hidden_sensitive_runtime (tag_override initial true).
Proof.
  simpl. split; [reflexivity|discriminate].
Qed.
