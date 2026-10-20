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
  rewrite Ha in Hb.
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


(** Constructive, very narrow repair: source-side, path-indexed frames
    record BOTH identity and metadata. The native fragment-specific
    extractor tested in run 39462504637 performs this for two outputs
    of an empty LocalRelation. The following small Coq model verifies
    that the additional information suffices for its declared domain.
    It does NOT verify the Scala extractor or unknown hidden Spark state. *)
Definition source_full_field_frame (a : TinyAttribute) : nat * bool :=
  (numerical_expr_id a, nonempty_metadata a).

Theorem full_source_attribute_frame_is_injective :
  forall x y,
    source_full_field_frame x = source_full_field_frame y ->
    x = y.
Proof.
  intros [ix mx] [iy my] H.
  unfold source_full_field_frame in H.
  simpl in H.
  injection H as Hi Hm.
  subst; reflexivity.
Qed.

Record TwoOutputFrame := {
  first_output : TinyAttribute;
  second_output : TinyAttribute
}.

Definition path_indexed_frame (p : TwoOutputFrame) :=
  (source_full_field_frame (first_output p),
   source_full_field_frame (second_output p)).

Theorem path_indexed_source_frame_is_injective :
  forall x y,
    path_indexed_frame x = path_indexed_frame y ->
    x = y.
Proof.
  intros [x0 x1] [y0 y1] H.
  pose proof (f_equal fst H) as H0.
  pose proof (f_equal snd H) as H1.
  unfold path_indexed_frame in H0, H1.
  simpl in H0, H1.
  apply full_source_attribute_frame_is_injective in H0.
  apply full_source_attribute_frame_is_injective in H1.
  subst; reflexivity.
Qed.

Definition swap_a : TwoOutputFrame :=
  {| first_output :=
       {| numerical_expr_id := 11; nonempty_metadata := false |};
     second_output :=
       {| numerical_expr_id := 12; nonempty_metadata := true |} |}.

Definition swap_b : TwoOutputFrame :=
  {| first_output :=
       {| numerical_expr_id := 11; nonempty_metadata := true |};
     second_output :=
       {| numerical_expr_id := 12; nonempty_metadata := false |} |}.

Definition erased_two_output_json (p : TwoOutputFrame) :=
  (numerical_expr_id (first_output p),
   numerical_expr_id (second_output p)).

Example unequal_native_style_swaps_have_equal_erased_json :
  swap_a <> swap_b /\
  erased_two_output_json swap_a = erased_two_output_json swap_b.
Proof.
  split; [discriminate|reflexivity].
Qed.

Example path_indexed_source_frame_separates_native_style_swaps :
  path_indexed_frame swap_a <> path_indexed_frame swap_b.
Proof.
  simpl. discriminate.
Qed.
