(** QueryProof source-observation lower bound: lossy encodings cannot
    establish complete structural equality or reconstruct every meaning.

    Concrete motivating execution: actual Spark v4.2.0 (run 39461415673)
    has two unequal AttributeReference and LocalRelation trees whose
    TreeNode.toJSON values are exactly equal because parseToJson replaces
    Metadata with Metadata.empty.jsonValue. Official source blob:
    e82e6a30b9bba41e637b27cbab43f26d9d23ff9b.

    This Coq model is an ABSTRACT impossibility result. The optional
    semantic-decoder theorem only implies SQL-query unsoundness IF metadata
    can change denotation in the relevant plan; that separate premise has
    NOT been established for W32 or general Spark.

    No Axioms, Admitted steps, or unsafe proof shortcuts.
*)
From Coq Require Import Bool.Bool.
From Coq Require Import Arith.PeanoNat.

Section EncodingLowerBounds.
  Variables Source Encoded Answer : Type.
  Variable encode : Source -> Encoded.
  Variable denotation : Source -> Answer.

  Theorem collision_precludes_total_structural_decoder :
    forall x y : Source,
      x <> y -> encode x = encode y ->
      ~ (exists decode : Encoded -> Source,
           forall source, decode (encode source) = source).
  Proof.
    intros x y Hdistinct Hcollision [decode Hdecode].
    apply Hdistinct.
    pose proof (Hdecode x) as Hx.
    pose proof (Hdecode y) as Hy.
    rewrite <- Hx.
    rewrite <- Hy.
    now rewrite Hcollision.
  Qed.

  Theorem collision_with_distinct_meanings_precludes_semantic_decoder :
    forall x y : Source,
      encode x = encode y ->
      denotation x <> denotation y ->
      ~ (exists decode_meaning : Encoded -> Answer,
           forall source, decode_meaning (encode source) = denotation source).
  Proof.
    intros x y Hcollision Hdistinct [decode Hdecode].
    apply Hdistinct.
    pose proof (Hdecode x) as Hx.
    pose proof (Hdecode y) as Hy.
    rewrite <- Hx.
    rewrite <- Hy.
    now rewrite Hcollision.
  Qed.
End EncodingLowerBounds.

Record TinyAttribute := {
  numerical_expr_id : nat;
  nonempty_metadata : bool
}.

Definition metadata_erasing_json (a : TinyAttribute) : nat :=
  numerical_expr_id a.

Definition native_analogue_a : TinyAttribute :=
  {| numerical_expr_id := 7331; nonempty_metadata := false |}.
Definition native_analogue_b : TinyAttribute :=
  {| numerical_expr_id := 7331; nonempty_metadata := true |}.

Example two_unequal_attributes_have_same_erased_encoding :
  native_analogue_a <> native_analogue_b /\
  metadata_erasing_json native_analogue_a =
  metadata_erasing_json native_analogue_b.
Proof.
  split; [discriminate|reflexivity].
Qed.

Theorem no_json_only_reconstruction_on_full_attribute_domain :
  ~ (exists decode : nat -> TinyAttribute,
       forall a, decode (metadata_erasing_json a) = a).
Proof.
  intro Hdecode.
  pose proof (proj1 two_unequal_attributes_have_same_erased_encoding) as Hneq.
  apply Hneq.
  destruct Hdecode as [decode H].
  pose proof (H native_analogue_a) as Ha.
  pose proof (H native_analogue_b) as Hb.
  cbn in Ha, Hb.
  rewrite <- Ha.
  exact Hb.
Qed.

(** The following is a hypothetical semantic countermodel, NOT a claim
    that this Spark metadata key changes row values. *)
Definition hypothetical_metadata_sensitive_semantics (a : TinyAttribute) : bool :=
  nonempty_metadata a.

Theorem erased_json_cannot_decide_hypothetical_metadata_sensitive_meaning :
  ~ (exists interpreter : nat -> bool,
       forall a, interpreter (metadata_erasing_json a) =
                 hypothetical_metadata_sensitive_semantics a).
Proof.
  intros [interpreter H].
  pose proof (H native_analogue_a) as Ha.
  pose proof (H native_analogue_b) as Hb.
  cbn in Ha, Hb.
  rewrite <- Ha in Hb.
  discriminate.
Qed.

(** A strictly restricted source domain CAN admit a lossless decoder
    for these two fields, but observing only the serialized number does
    not prove the original source met the metadata-empty precondition.
    A separately trustworthy native source-side witness is necessary. *)
Theorem known_empty_metadata_restores_injectivity :
  forall x y,
    nonempty_metadata x = false ->
    nonempty_metadata y = false ->
    metadata_erasing_json x = metadata_erasing_json y ->
    x = y.
Proof.
  intros [ix mx] [iy my] Hx Hy Hid.
  simpl in *.
  subst iy.
  rewrite Hx, Hy.
  reflexivity.
Qed.

Example json_sameness_does_not_prove_metadata_absence :
  metadata_erasing_json native_analogue_a =
  metadata_erasing_json native_analogue_b /\
  nonempty_metadata native_analogue_a <> nonempty_metadata native_analogue_b.
Proof.
  simpl. split; [reflexivity|discriminate].
Qed.
