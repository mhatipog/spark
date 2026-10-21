(** V13: an executable *old-plan/new-plan* guard rewrite checker.
    Strengthens V10, which checked only claimed guard flags. The kernel now
    verifies BOTH abstract plan terms preserve join kind and comparator, and
    that the old term has no guards. Still not a verified Catalyst parser.
*)
From Coq Require Import Bool.Bool.
Require Import QueryProofGuardCertificate.

Definition comparator_equal (a b : Comparator) : bool :=
  match a,b with
  | StrictEq, StrictEq | StrictLt, StrictLt
  | NullSafeEq, NullSafeEq => true
  | _,_ => false
  end.

Definition kind_equal (a b : JoinKind) : bool :=
  match a,b with
  | Inner,Inner | LeftOuter,LeftOuter => true
  | _,_ => false
  end.

Lemma comparator_equal_sound : forall x y,
    comparator_equal x y = true -> x = y.
Proof. intros [] []; simpl; intros H; try discriminate; reflexivity. Qed.
Lemma kind_equal_sound : forall x y,
    kind_equal x y = true -> x = y.
Proof. intros [] []; simpl; intros H; try discriminate; reflexivity. Qed.

(** A miniature typed logical join plan: comparator, join kind and native
    guard placement; payload schemas, aliases and metadata are external.
    Every bit of this abstract OLD/NEW relation is checked by the kernel.
*)
Definition transition_ok (old new : GuardCertificate) : bool :=
  comparator_equal (comparison old) (comparison new) &&
  kind_equal (join_kind old) (join_kind new) &&
  negb (filter_left old) &&
  negb (filter_right old) &&
  check new.

Theorem checked_plan_pair_preserves_bags :
  forall old new left right,
    transition_ok old new = true ->
    execute old (guarded_left old left) (guarded_right old right) =
    execute new (guarded_left new left) (guarded_right new right).
Proof.
  intros [ko jo lo ro] [kn jn ln rn] left right H.
  unfold transition_ok in H; simpl in H.
  apply andb_true_iff in H as [Hpre Hnew].
  apply andb_true_iff in Hpre as [Hpre Hro].
  apply andb_true_iff in Hpre as [Hpre Hlo].
  apply andb_true_iff in Hpre as [Hcmp Hkind].
  apply comparator_equal_sound in Hcmp.
  apply kind_equal_sound in Hkind.
  apply negb_true_iff in Hlo.
  apply negb_true_iff in Hro.
  subst kn jn lo ro.
  simpl.
  symmetry.
  apply checked_certificate_sound.
  exact Hnew.
Qed.

Definition before_lt_inner : GuardCertificate :=
  {| comparison := StrictLt; join_kind := Inner;
     filter_left := false; filter_right := false |}.
Definition after_lt_inner : GuardCertificate :=
  {| comparison := StrictLt; join_kind := Inner;
     filter_left := true; filter_right := true |}.

Example admits_valid_pair :
  transition_ok before_lt_inner after_lt_inner = true.
Proof. reflexivity. Qed.

Example rejects_nullsafe_new_predicate :
  transition_ok before_lt_inner
    {| comparison := NullSafeEq; join_kind := Inner;
       filter_left := true; filter_right := true |} = false.
Proof. reflexivity. Qed.

Example rejects_join_kind_switch :
  transition_ok before_lt_inner
    {| comparison := StrictLt; join_kind := LeftOuter;
       filter_left := false; filter_right := true |} = false.
Proof. reflexivity. Qed.

Example rejects_existing_guard_in_original :
  transition_ok
    {| comparison := StrictLt; join_kind := Inner;
       filter_left := true; filter_right := false |}
    after_lt_inner = false.
Proof. reflexivity. Qed.

Example rejects_preserved_outer_side_guard :
  transition_ok
    {| comparison := StrictLt; join_kind := LeftOuter;
       filter_left := false; filter_right := false |}
    {| comparison := StrictLt; join_kind := LeftOuter;
       filter_left := true; filter_right := false |} = false.
Proof. reflexivity. Qed.
