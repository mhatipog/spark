(** V15: deep-embedded checker for null-rejection guard insertion.

    Unlike V10/V13, the kernel receives the actual OLD and NEW plan TERMS
    (resolved attribute ids, 3VL expressions, Filter/Project/Join/opaque
    operators). The executable [check] itself decides that NEW is OLD with
    IsNotNull guard filters inserted above join inputs, where every guarded
    attribute is (a) output by that input, (b) null-rejected by the unchanged
    join condition, and (c) never on the preserved side of a LEFT OUTER
    join. Everything else must match syntactically.

    [check_sound] holds for EVERY interpretation of leaves and opaque
    operators (any database, any list function), so frames such as
    Sort/Aggregate need no semantics of their own.

    Trusted, NOT proved here: the JSON -> term printer; that Spark's
    evaluator agrees with [eval_e] on this expression grammar (ints, no
    ANSI errors); that equal leaf/opaque descriptors denote equal behaviour
    (source identity); hidden optimizer state (tags, caches).
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool Arith.PeanoNat.
Import ListNotations.

(** * Values, rows, expressions *)

Inductive val := VInt (z : Z) | VBool (b : bool).
Definition cell := option val.                (* None = SQL NULL *)
Definition row := list (nat * cell).          (* keyed by resolved ExprId *)

Fixpoint lookup (x : nat) (r : row) : option cell :=
  match r with
  | [] => None
  | (y, v) :: rest => if Nat.eqb x y then Some v else lookup x rest
  end.

Inductive expr :=
| EAttr (x : nat)
| ELit (v : cell)
| ELt (a b : expr) | ELe (a b : expr) | EGt (a b : expr) | EGe (a b : expr)
| EEq (a b : expr)
| ENullSafeEq (a b : expr)
| EIsNotNull (a : expr)
| EAnd (a b : expr)
| EOr (a b : expr).

(** Outer [None] = evaluation error (absent attribute, ill-typed operands). *)
Definition cmp (f : Z -> Z -> bool) (a b : option cell) : option cell :=
  match a, b with
  | Some (Some (VInt x)), Some (Some (VInt y)) => Some (Some (VBool (f x y)))
  | Some None, Some None
  | Some None, Some (Some (VInt _))
  | Some (Some (VInt _)), Some None => Some None
  | _, _ => None
  end.

Definition and3 (a b : option cell) : option cell :=
  match a, b with
  | Some (Some (VBool true)), Some (Some (VBool y)) => Some (Some (VBool y))
  | Some (Some (VBool false)), Some (Some (VBool _))
  | Some (Some (VBool false)), Some None
  | Some None, Some (Some (VBool false)) => Some (Some (VBool false))
  | Some (Some (VBool true)), Some None
  | Some None, Some (Some (VBool true))
  | Some None, Some None => Some None
  | _, _ => None
  end.

Definition or3 (a b : option cell) : option cell :=
  match a, b with
  | Some (Some (VBool false)), Some (Some (VBool y)) => Some (Some (VBool y))
  | Some (Some (VBool true)), Some (Some (VBool _))
  | Some (Some (VBool true)), Some None
  | Some None, Some (Some (VBool true)) => Some (Some (VBool true))
  | Some (Some (VBool false)), Some None
  | Some None, Some (Some (VBool false))
  | Some None, Some None => Some None
  | _, _ => None
  end.

Fixpoint eval_e (e : expr) (r : row) : option cell :=
  match e with
  | EAttr x => lookup x r
  | ELit v => Some v
  | ELt a b => cmp Z.ltb (eval_e a r) (eval_e b r)
  | ELe a b => cmp Z.leb (eval_e a r) (eval_e b r)
  | EGt a b => cmp (fun x y => Z.ltb y x) (eval_e a r) (eval_e b r)
  | EGe a b => cmp (fun x y => Z.leb y x) (eval_e a r) (eval_e b r)
  | EEq a b => cmp Z.eqb (eval_e a r) (eval_e b r)
  | ENullSafeEq a b =>
      match eval_e a r, eval_e b r with
      | Some None, Some None => Some (Some (VBool true))
      | Some None, Some (Some (VInt _))
      | Some (Some (VInt _)), Some None => Some (Some (VBool false))
      | Some (Some (VInt x)), Some (Some (VInt y)) => Some (Some (VBool (Z.eqb x y)))
      | _, _ => None
      end
  | EIsNotNull a =>
      match eval_e a r with
      | Some None => Some (Some (VBool false))
      | Some (Some _) => Some (Some (VBool true))
      | None => None
      end
  | EAnd a b => and3 (eval_e a r) (eval_e b r)
  | EOr a b => or3 (eval_e a r) (eval_e b r)
  end.

(** SQL WHERE / ON semantics: keep only TRUE (not FALSE, NULL or error). *)
Definition is_true (c : option cell) : bool :=
  match c with Some (Some (VBool true)) => true | _ => false end.

(** * Plans *)

Inductive jkind := Inner | LeftOuter.

Inductive plan :=
| PLeaf (k : nat) (o : list nat)                   (* opaque source *)
| PFilter (c : expr) (p : plan)
| PProject (ids : list nat) (p : plan)             (* attribute-only *)
| POp1 (k : nat) (o : list nat) (p : plan)         (* opaque unary operator *)
| PJoin (j : jkind) (c : option expr) (l r : plan).

Fixpoint out (p : plan) : list nat :=
  match p with
  | PLeaf _ o | POp1 _ o _ => o
  | PFilter _ q => out q
  | PProject ids _ => ids
  | PJoin _ _ l r => out l ++ out r
  end.

Definition getv (x : nat) (r : row) : cell :=
  match lookup x r with Some v => v | None => None end.
Definition normalize (ks : list nat) (r : row) : row :=
  map (fun x => (x, getv x r)) ks.

Definition cond_true (c : option expr) (r : row) : bool :=
  match c with None => true | Some e => is_true (eval_e e r) end.
Definition matches (c : option expr) (lr : row) (rs : list row) : list row :=
  filter (fun rr => cond_true c (lr ++ rr)) rs.
Definition pad (ks : list nat) : row := map (fun x => (x, None)) ks.
Arguments matches : simpl never.
Arguments normalize : simpl never.
Arguments pad : simpl never.

Definition join (j : jkind) (c : option expr) (rout : list nat)
    (ls rs : list row) : list row :=
  match j with
  | Inner => flat_map (fun lr => map (app lr) (matches c lr rs)) ls
  | LeftOuter => flat_map (fun lr =>
      match matches c lr rs with
      | [] => [lr ++ pad rout]
      | ms => map (app lr) ms
      end) ls
  end.

Section Semantics.
  (** Universally quantified interpretation of every opaque part. *)
  Variable db : nat -> list nat -> list row.
  Variable ops : nat -> list nat -> list row -> list row.

  Fixpoint eval (p : plan) : list row :=
    match p with
    | PLeaf k o => map (normalize o) (db k o)
    | PFilter c q => filter (fun t => is_true (eval_e c t)) (eval q)
    | PProject ids q => map (normalize ids) (eval q)
    | POp1 k o q => map (normalize o) (ops k o (eval q))
    | PJoin j c l r => join j c (out r) (eval l) (eval r)
    end.
End Semantics.

(** * The executable checker *)

Definition jk_eqb (a b : jkind) : bool :=
  match a, b with Inner, Inner | LeftOuter, LeftOuter => true | _, _ => false end.

Definition val_eq_dec : forall a b : val, {a = b} + {a <> b}.
Proof. decide equality; [apply Z.eq_dec | apply bool_dec]. Defined.
Definition expr_eq_dec : forall a b : expr, {a = b} + {a <> b}.
Proof.
  decide equality; try apply Nat.eq_dec; decide equality; apply val_eq_dec.
Defined.
Definition jk_eq_dec : forall a b : jkind, {a = b} + {a <> b}.
Proof. decide equality. Defined.
Definition oexpr_eq_dec : forall a b : option expr, {a = b} + {a <> b}.
Proof. decide equality; apply expr_eq_dec. Defined.
Definition plan_eq_dec : forall a b : plan, {a = b} + {a <> b}.
Proof.
  decide equality;
    first [ apply Nat.eq_dec | apply (list_eq_dec Nat.eq_dec)
          | apply expr_eq_dec | apply oexpr_eq_dec | apply jk_eq_dec ].
Defined.

Definition plan_eqb (a b : plan) : bool := if plan_eq_dec a b then true else false.
Definition expr_eqb (a b : expr) : bool := if expr_eq_dec a b then true else false.
Definition oexpr_eqb (a b : option expr) : bool := if oexpr_eq_dec a b then true else false.
Definition ids_eqb (a b : list nat) : bool :=
  if list_eq_dec Nat.eq_dec a b then true else false.
Definition memb (x : nat) (xs : list nat) : bool := existsb (Nat.eqb x) xs.

Definition operand_ids (e : expr) : list nat :=
  match e with EAttr x => [x] | _ => [] end.

(** Attributes that must be non-NULL whenever [e] evaluates to TRUE. *)
Fixpoint nr (e : expr) : list nat :=
  match e with
  | EAnd a b => nr a ++ nr b
  | ELt a b | ELe a b | EGt a b | EGe a b | EEq a b =>
      operand_ids a ++ operand_ids b
  | EIsNotNull a => operand_ids a
  | _ => []
  end.
Definition nr_cond (c : option expr) : list nat :=
  match c with Some e => nr e | None => [] end.

(** A guard is a conjunction of IsNotNull(attribute). *)
Fixpoint guard_ids (g : expr) : option (list nat) :=
  match g with
  | EIsNotNull (EAttr x) => Some [x]
  | EAnd a b =>
      match guard_ids a, guard_ids b with
      | Some xs, Some ys => Some (xs ++ ys)
      | _, _ => None
      end
  | _ => None
  end.

Definition guard_ok (j : jkind) (c : option expr) (is_left : bool)
    (lo ro : list nat) (g : expr) : bool :=
  match guard_ids g with
  | None => false
  | Some xs =>
      forallb (fun x => memb x (nr_cond c)) xs &&
      (if is_left
       then jk_eqb j Inner && forallb (fun x => memb x lo) xs
       else forallb (fun x => memb x ro && negb (memb x lo)) xs)
  end.

Fixpoint check (o n : plan) {struct o} : bool :=
  plan_eqb o n ||
  match o, n with
  | PFilter c p, PFilter c' p' => expr_eqb c c' && check p p'
  | PProject ids p, PProject ids' p' => ids_eqb ids ids' && check p p'
  | POp1 k o1 p, POp1 k' o1' p' => Nat.eqb k k' && ids_eqb o1 o1' && check p p'
  | PJoin j c l r, PJoin j' c' l' r' =>
      jk_eqb j j' && oexpr_eqb c c' &&
      (check l l' ||
       match l' with
       | PFilter g l0 => check l l0 && guard_ok j c true (out l) (out r) g
       | _ => false
       end) &&
      (check r r' ||
       match r' with
       | PFilter g r0 => check r r0 && guard_ok j c false (out l) (out r) g
       | _ => false
       end)
  | _, _ => false
  end.

(** * Proof *)

Lemma memb_In : forall x xs, memb x xs = true <-> In x xs.
Proof.
  intros x xs. unfold memb. rewrite existsb_exists. split.
  - intros [y [Hy Heq]]. apply Nat.eqb_eq in Heq. subst. exact Hy.
  - intros H. exists x. split; [exact H | apply Nat.eqb_refl].
Qed.

Lemma lookup_app_in : forall x a b,
    In x (map fst a) -> lookup x (a ++ b) = lookup x a.
Proof.
  intros x a b. induction a as [|[y v] a IH]; simpl; [tauto|].
  destruct (Nat.eqb x y) eqn:E; [reflexivity|].
  intros [H|H].
  - subst. rewrite Nat.eqb_refl in E. discriminate.
  - auto.
Qed.

Lemma lookup_app_notin : forall x a b,
    ~ In x (map fst a) -> lookup x (a ++ b) = lookup x b.
Proof.
  intros x a b. induction a as [|[y v] a IH]; simpl; intros H; [reflexivity|].
  destruct (Nat.eqb x y) eqn:E.
  - apply Nat.eqb_eq in E. subst. exfalso. apply H. left. reflexivity.
  - apply IH. intros H'. apply H. right. exact H'.
Qed.

Lemma normalize_keys : forall ks t, map fst (normalize ks t) = ks.
Proof.
  intros ks t. unfold normalize.
  induction ks as [|k ks IH]; simpl; [reflexivity | now rewrite IH].
Qed.

Lemma pad_keys : forall ks, map fst (pad ks) = ks.
Proof.
  intros ks. unfold pad.
  induction ks as [|k ks IH]; simpl; [reflexivity | now rewrite IH].
Qed.

Lemma eval_keys : forall db ops p t, In t (eval db ops p) -> map fst t = out p.
Proof.
  intros db ops p.
  induction p as [k o|c q IH|ids q IH|k o q IH|j c l IHl r IHr];
    intros t Hin; simpl in Hin.
  - apply in_map_iff in Hin as [t0 [<- _]]. apply normalize_keys.
  - apply filter_In in Hin as [Hin _]. simpl. auto.
  - apply in_map_iff in Hin as [t0 [<- _]]. apply normalize_keys.
  - apply in_map_iff in Hin as [t0 [<- _]]. apply normalize_keys.
  - simpl. destruct j; simpl in Hin; apply in_flat_map in Hin as [lr [Hl Hr]].
    + apply in_map_iff in Hr as [rr [<- Hm]].
      unfold matches in Hm. apply filter_In in Hm as [Hm _].
      rewrite map_app, (IHl _ Hl), (IHr _ Hm). reflexivity.
    + destruct (matches c lr (eval db ops r)) as [|m ms] eqn:Em.
      * destruct Hr as [<-|[]]. rewrite map_app, (IHl _ Hl), pad_keys. reflexivity.
      * change (In t (map (app lr) (m :: ms))) in Hr.
        apply in_map_iff in Hr as [rr [<- Hm]].
        rewrite <- Em in Hm. unfold matches in Hm. apply filter_In in Hm as [Hm _].
        rewrite map_app, (IHl _ Hl), (IHr _ Hm). reflexivity.
Qed.

Lemma is_true_eq : forall a, is_true a = true -> a = Some (Some (VBool true)).
Proof. intros [[[z|[|]]|]|]; simpl; intros H; try discriminate; reflexivity. Qed.

Lemma cmp_true_inv : forall f a b, is_true (cmp f a b) = true ->
    exists x y, a = Some (Some (VInt x)) /\ b = Some (Some (VInt y)).
Proof.
  intros f [[[x|bx]|]|] [[[y|by']|]|]; simpl; intros H; try discriminate; eauto.
Qed.

Lemma and3_true_inv : forall a b, is_true (and3 a b) = true ->
    is_true a = true /\ is_true b = true.
Proof.
  intros [[[x|[|]]|]|] [[[y|[|]]|]|]; simpl; intros H; try discriminate; auto.
Qed.

Lemma operand_attr : forall a x, In x (operand_ids a) -> a = EAttr x.
Proof.
  intros a x. destruct a; simpl; intros H; try contradiction.
  destruct H as [<-|[]]. reflexivity.
Qed.

Lemma cmp_nr : forall f a b x t,
    In x (operand_ids a ++ operand_ids b) ->
    is_true (cmp f (eval_e a t) (eval_e b t)) = true ->
    exists v, lookup x t = Some (Some v).
Proof.
  intros f a b x t Hin Ht.
  apply cmp_true_inv in Ht as [p [q [Ha Hb]]].
  apply in_app_or in Hin as [H|H]; apply operand_attr in H; subst;
    simpl in *; eauto.
Qed.

Lemma nr_sound : forall e y t, In y (nr e) -> is_true (eval_e e t) = true ->
    exists v, lookup y t = Some (Some v).
Proof.
  induction e; intros y t Hin Ht; simpl in Hin; try contradiction;
    simpl in Ht; try (eapply cmp_nr; eassumption).
  - (* IsNotNull *)
    apply operand_attr in Hin. subst. simpl in Ht.
    destruct (lookup y t) as [[v|]|]; try discriminate. eauto.
  - (* And *)
    apply and3_true_inv in Ht as [H1 H2].
    apply in_app_or in Hin as [H|H]; eauto.
Qed.

Lemma guard_true : forall g xs t, guard_ids g = Some xs ->
    (forall x, In x xs -> exists v, lookup x t = Some (Some v)) ->
    is_true (eval_e g t) = true.
Proof.
  induction g; intros xs t Hg Hall; simpl in Hg; try discriminate.
  - destruct g as [z| | | | | | | | | |]; try discriminate. injection Hg as <-.
    destruct (Hall z (or_introl eq_refl)) as [v Hv]. simpl. rewrite Hv. reflexivity.
  - destruct (guard_ids g1) as [xs1|] eqn:E1; try discriminate.
    destruct (guard_ids g2) as [xs2|] eqn:E2; try discriminate.
    injection Hg as <-. simpl.
    rewrite (is_true_eq _ (IHg1 xs1 t eq_refl
               (fun x H => Hall x (in_or_app _ _ _ (or_introl H))))).
    rewrite (is_true_eq _ (IHg2 xs2 t eq_refl
               (fun x H => Hall x (in_or_app _ _ _ (or_intror H))))).
    reflexivity.
Qed.

Lemma filter_implied : forall (A : Type) (p g : A -> bool) xs,
    (forall x, In x xs -> p x = true -> g x = true) ->
    filter p (filter g xs) = filter p xs.
Proof.
  intros A p g xs H. induction xs as [|x xs IH]; simpl; [reflexivity|].
  assert (IH' : filter p (filter g xs) = filter p xs).
  { apply IH. intros y Hy. apply H. right. exact Hy. }
  destruct (g x) eqn:Eg; simpl.
  - destruct (p x); [f_equal|]; exact IH'.
  - destruct (p x) eqn:Ep.
    + rewrite (H x (or_introl eq_refl) Ep) in Eg. discriminate.
    + exact IH'.
Qed.

Lemma filter_none : forall (A : Type) (f : A -> bool) xs,
    (forall x, In x xs -> f x = false) -> filter f xs = [].
Proof.
  intros A f xs H. induction xs as [|x xs IH]; simpl; [reflexivity|].
  rewrite (H x (or_introl eq_refl)). apply IH. intros y Hy. apply H. right. exact Hy.
Qed.

Lemma filter_all : forall (A : Type) (f : A -> bool) xs,
    (forall x, f x = true) -> filter f xs = xs.
Proof.
  intros A f xs H. induction xs as [|x xs IH]; simpl; [reflexivity|].
  rewrite H. now rewrite IH.
Qed.

Lemma join_right_guard : forall j c ro ls rs (G : row -> bool),
    (forall lr rr, In lr ls -> In rr rs -> cond_true c (lr ++ rr) = true -> G rr = true) ->
    join j c ro ls (filter G rs) = join j c ro ls rs.
Proof.
  intros j c ro ls rs G H. induction ls as [|lr ls IH]; [destruct j; reflexivity|].
  assert (Hm : matches c lr (filter G rs) = matches c lr rs).
  { unfold matches. apply filter_implied. intros rr Hrr Hc.
    exact (H lr rr (or_introl eq_refl) Hrr Hc). }
  assert (IH' : join j c ro ls (filter G rs) = join j c ro ls rs).
  { apply IH. intros lr' rr Hl Hr. apply H. right. exact Hl. exact Hr. }
  destruct j; simpl in *; rewrite Hm, IH'; reflexivity.
Qed.

Lemma join_left_guard : forall c ro ls rs (G : row -> bool),
    (forall lr rr, In lr ls -> In rr rs -> cond_true c (lr ++ rr) = true -> G lr = true) ->
    join Inner c ro (filter G ls) rs = join Inner c ro ls rs.
Proof.
  intros c ro ls rs G H. induction ls as [|lr ls IH]; [reflexivity|].
  assert (IH' := IH (fun l r Hl => H l r (or_intror Hl))).
  simpl in *. destruct (G lr) eqn:EG; simpl; rewrite IH'.
  - reflexivity.
  - assert (Hm : matches c lr rs = []).
    { unfold matches. apply filter_none. intros rr Hrr.
      destruct (cond_true c (lr ++ rr)) eqn:Ec; [|reflexivity].
      rewrite (H lr rr (or_introl eq_refl) Hrr Ec) in EG. discriminate. }
    rewrite Hm. reflexivity.
Qed.

Definition sound_rel (a b : plan) : Prop :=
  out a = out b /\ forall db ops, eval db ops a = eval db ops b.

Lemma left_side : forall j c l r l',
    (forall n, check l n = true -> sound_rel l n) ->
    (check l l' ||
     match l' with
     | PFilter g l0 => check l l0 && guard_ok j c true (out l) (out r) g
     | _ => false
     end) = true ->
    exists G : row -> bool,
      out l' = out l /\
      (forall db ops, eval db ops l' = filter G (eval db ops l)) /\
      (j = Inner \/ forall t, G t = true) /\
      (forall lr rr, map fst lr = out l -> cond_true c (lr ++ rr) = true -> G lr = true).
Proof.
  intros j c l r l' IH H. apply orb_true_iff in H as [H|H].
  - destruct (IH l' H) as [Ho He]. exists (fun _ => true). repeat split.
    + now rewrite Ho.
    + intros db ops. rewrite filter_all by reflexivity. now rewrite He.
    + right. reflexivity.
  - destruct l' as [| g l0 | | |]; try discriminate.
    apply andb_true_iff in H as [Hc Hg].
    destruct (IH l0 Hc) as [Ho He].
    unfold guard_ok in Hg. destruct (guard_ids g) as [xs|] eqn:Eg; try discriminate.
    apply andb_true_iff in Hg as [Hnr Hside].
    apply andb_true_iff in Hside as [Hj Hlo].
    exists (fun t => is_true (eval_e g t)). repeat split.
    + simpl. now rewrite Ho.
    + intros db ops. simpl. now rewrite He.
    + left. now destruct j.
    + intros lr rr Hkeys Hcond.
      apply (guard_true g xs lr Eg). intros x Hx.
      rewrite forallb_forall in Hnr, Hlo.
      pose proof (proj1 (memb_In _ _) (Hnr x Hx)) as Hxnr.
      pose proof (proj1 (memb_In _ _) (Hlo x Hx)) as Hxl.
      destruct c as [e|]; simpl in Hxnr; [|contradiction].
      destruct (nr_sound e x (lr ++ rr) Hxnr Hcond) as [v Hv].
      exists v. rewrite <- Hv. symmetry. apply lookup_app_in. now rewrite Hkeys.
Qed.

Lemma right_side : forall j c l r r',
    (forall n, check r n = true -> sound_rel r n) ->
    (check r r' ||
     match r' with
     | PFilter g r0 => check r r0 && guard_ok j c false (out l) (out r) g
     | _ => false
     end) = true ->
    exists G : row -> bool,
      out r' = out r /\
      (forall db ops, eval db ops r' = filter G (eval db ops r)) /\
      (forall lr rr, map fst lr = out l -> map fst rr = out r ->
         cond_true c (lr ++ rr) = true -> G rr = true).
Proof.
  intros j c l r r' IH H. apply orb_true_iff in H as [H|H].
  - destruct (IH r' H) as [Ho He]. exists (fun _ => true). repeat split.
    + now rewrite Ho.
    + intros db ops. rewrite filter_all by reflexivity. now rewrite He.
  - destruct r' as [| g r0 | | |]; try discriminate.
    apply andb_true_iff in H as [Hc Hg].
    destruct (IH r0 Hc) as [Ho He].
    unfold guard_ok in Hg. destruct (guard_ids g) as [xs|] eqn:Eg; try discriminate.
    apply andb_true_iff in Hg as [Hnr Hside].
    exists (fun t => is_true (eval_e g t)). repeat split.
    + simpl. now rewrite Ho.
    + intros db ops. simpl. now rewrite He.
    + intros lr rr Hkl Hkr Hcond.
      apply (guard_true g xs rr Eg). intros x Hx.
      rewrite forallb_forall in Hnr, Hside.
      pose proof (proj1 (memb_In _ _) (Hnr x Hx)) as Hxnr.
      pose proof (Hside x Hx) as Hsx.
      apply andb_true_iff in Hsx as [Hxr Hxl].
      apply negb_true_iff in Hxl.
      destruct c as [e|]; simpl in Hxnr; [|contradiction].
      destruct (nr_sound e x (lr ++ rr) Hxnr Hcond) as [v Hv].
      exists v. rewrite <- Hv. symmetry. apply lookup_app_notin.
      rewrite Hkl. intros Hin. apply memb_In in Hin. congruence.
Qed.

Lemma eqb_dec_true : forall (A : Type) (dec : forall a b : A, {a = b} + {a <> b}) a b,
    (if dec a b then true else false) = true -> a = b.
Proof. intros A dec a b. destruct (dec a b); [auto | discriminate]. Qed.

Theorem check_correct : forall o n, check o n = true -> sound_rel o n.
Proof.
  induction o as [k o1|c p IH|ids p IH|k o1 p IH|j c l IHl r IHr]; intros n Hc;
    simpl in Hc; apply orb_true_iff in Hc as [Heq|Hc];
    try (apply (eqb_dec_true _ plan_eq_dec) in Heq; subst;
         split; [reflexivity | reflexivity]);
    destruct n; try discriminate.
  - apply andb_true_iff in Hc as [Hce Hp].
    apply (eqb_dec_true _ expr_eq_dec) in Hce. subst.
    destruct (IH _ Hp) as [Ho He]. split; [exact Ho|].
    intros db ops. simpl. now rewrite He.
  - apply andb_true_iff in Hc as [Hi Hp].
    apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hi. subst.
    destruct (IH _ Hp) as [Ho He]. split; [reflexivity|].
    intros db ops. simpl. now rewrite He.
  - apply andb_true_iff in Hc as [Hk Hp]. apply andb_true_iff in Hk as [Hk Hi].
    apply Nat.eqb_eq in Hk. apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hi. subst.
    destruct (IH _ Hp) as [Ho He]. split; [reflexivity|].
    intros db ops. simpl. now rewrite He.
  - rename j0 into j', c0 into c', n1 into l', n2 into r'.
    apply andb_true_iff in Hc as [Hc HR]. apply andb_true_iff in Hc as [Hc HL].
    apply andb_true_iff in Hc as [Hj Hce].
    assert (j' = j) by (destruct j, j'; try discriminate; reflexivity). subst j'.
    apply (eqb_dec_true _ oexpr_eq_dec) in Hce. subst c'.
    destruct (left_side j c l r l' IHl HL) as [Gl [Hol [Hel [Hjl Hcl]]]].
    destruct (right_side j c l r r' IHr HR) as [Gr [Hor [Her Hcr]]].
    split.
    + simpl. now rewrite Hol, Hor.
    + intros db ops. simpl. rewrite Hel, Her, Hor.
      rewrite join_right_guard.
      * destruct Hjl as [-> | Htriv].
        -- rewrite join_left_guard; [reflexivity|].
           intros lr rr Hl Hr Hcond.
           exact (Hcl lr rr (eval_keys db ops l lr Hl) Hcond).
        -- now rewrite (filter_all _ Gl _ Htriv).
      * intros lr rr Hl Hr Hcond.
        apply filter_In in Hl as [Hl _].
        exact (Hcr lr rr (eval_keys db ops l lr Hl) (eval_keys db ops r rr Hr) Hcond).
Qed.

Theorem check_sound : forall o n, check o n = true ->
    forall db ops, eval db ops o = eval db ops n.
Proof. intros o n H. exact (proj2 (check_correct o n H)). Qed.

(** * Kernel-evaluated controls on a W17-shaped term *)

Definition w_left := PProject [0] (PLeaf 0 [0; 1; 2]).
Definition w_right := PProject [3] (PLeaf 1 [3; 4; 5]).
Definition w_frame (jn : plan) := POp1 2 [0] (POp1 3 [0; 6] (PProject [0] jn)).
Definition w_old := w_frame (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3))) w_left w_right).
Definition w_new := w_frame (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3)))
                       (PFilter (EIsNotNull (EAttr 0)) w_left)
                       (PFilter (EIsNotNull (EAttr 3)) w_right)).

Example accepts_w17_shape : check w_old w_new = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_wrong_attribute_guard :
  check w_old (w_frame (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3)))
    (PFilter (EIsNotNull (EAttr 1)) w_left) w_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_guard_attribute_from_other_side :
  check w_old (w_frame (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3)))
    w_left (PFilter (EIsNotNull (EAttr 0)) w_right))) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_null_safe_join_guard :
  check (w_frame (PJoin Inner (Some (ENullSafeEq (EAttr 0) (EAttr 3))) w_left w_right))
        (w_frame (PJoin Inner (Some (ENullSafeEq (EAttr 0) (EAttr 3)))
           (PFilter (EIsNotNull (EAttr 0)) w_left) w_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_left_outer_preserved_side_guard :
  check (w_frame (PJoin LeftOuter (Some (ELt (EAttr 0) (EAttr 3))) w_left w_right))
        (w_frame (PJoin LeftOuter (Some (ELt (EAttr 0) (EAttr 3)))
           (PFilter (EIsNotNull (EAttr 0)) w_left) w_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_null_supplying_guard :
  check (w_frame (PJoin LeftOuter (Some (ELt (EAttr 0) (EAttr 3))) w_left w_right))
        (w_frame (PJoin LeftOuter (Some (ELt (EAttr 0) (EAttr 3)))
           w_left (PFilter (EIsNotNull (EAttr 3)) w_right))) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_or_condition_guard :
  check (w_frame (PJoin Inner (Some (EOr (EEq (EAttr 0) (EAttr 3)) (EEq (EAttr 1) (EAttr 4))))
           w_left w_right))
        (w_frame (PJoin Inner (Some (EOr (EEq (EAttr 0) (EAttr 3)) (EEq (EAttr 1) (EAttr 4))))
           (PFilter (EIsNotNull (EAttr 0)) w_left) w_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_frame_edit :
  check w_old (POp1 9 [0] (POp1 3 [0; 6] (PProject [0]
    (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3))) w_left w_right)))) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_join_condition_change :
  check w_old (w_frame (PJoin Inner (Some (ELe (EAttr 0) (EAttr 3)))
    (PFilter (EIsNotNull (EAttr 0)) w_left) w_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Print Assumptions check_sound.
