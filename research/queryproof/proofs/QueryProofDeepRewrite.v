(** V16: kernel-decided certificates for InferFiltersFromConstraints
    (join-input guards AND filter-merge) and PushDownPredicates
    (CombineFilters, push through attribute-only Project, push through
    INNER Join), over the V15 deep embedding (imported unchanged).

    Central law: in SQL 3VL a filter keeps a row iff EVERY conjunct is TRUE.
    Hence two conditions are interchangeable when they have the same conjunct
    set, up to extra IsNotNull(x) where x is null-rejected by the original.

    [check2_sound] holds for every interpretation of leaves and opaque
    operators. The trusted base is as in V15 (printer, evaluator agreement,
    opaque identity, JSON fidelity, no hidden-state coverage).
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool Arith.PeanoNat Lia.
Import ListNotations.
Require Import QueryProofDeepGuard.

(** * Syntax helpers *)

Fixpoint refs (e : expr) : list nat :=
  match e with
  | EAttr x => [x]
  | ELit _ => []
  | ELt a b | ELe a b | EGt a b | EGe a b | EEq a b
  | ENullSafeEq a b | EAnd a b | EOr a b => refs a ++ refs b
  | EIsNotNull a => refs a
  end.

Fixpoint conjs (e : expr) : list expr :=
  match e with EAnd a b => conjs a ++ conjs b | _ => [e] end.

Definition conjs_opt (c : option expr) : list expr :=
  match c with Some e => conjs e | None => [] end.
Definition refs_opt (c : option expr) : list nat :=
  match c with Some e => refs e | None => [] end.

Definition mem_expr (d : expr) (l : list expr) : bool := existsb (expr_eqb d) l.
Definition subset (a b : list nat) : bool := forallb (fun x => memb x b) a.
Definition disjoint (a b : list nat) : bool := forallb (fun x => negb (memb x b)) a.
Definition seteq (a b : list expr) : bool :=
  forallb (fun d => mem_expr d b) a && forallb (fun d => mem_expr d a) b.

(** A conjunct of the NEW condition must be a conjunct of the OLD one, or an
    IsNotNull(x) with x null-rejected by the OLD condition. *)
Definition implied_conj (c d : expr) : bool :=
  mem_expr d (conjs c) ||
  match d with EIsNotNull (EAttr x) => memb x (nr c) | _ => false end.
Definition filter_equiv (c c' : expr) : bool :=
  forallb (fun d => mem_expr d (conjs c')) (conjs c) &&
  forallb (implied_conj c) (conjs c').

Definition push_ok (c : expr) (jc jc' : option expr) (lo ro : list nat)
    (lp rp : option expr) : bool :=
  seteq (conjs_opt jc ++ conjs c) (conjs_opt lp ++ conjs_opt rp ++ conjs_opt jc') &&
  subset (refs_opt lp) lo && subset (refs_opt rp) ro && disjoint (refs_opt rp) lo.

(** LEFT OUTER: WHERE conjuncts over the preserved (left) side may move into
    the left input; the remaining conjuncts stay above the join. *)
Definition lo_ok (c : expr) (rest lp : option expr) (lo : list nat) : bool :=
  seteq (conjs c) (conjs_opt lp ++ conjs_opt rest) && subset (refs_opt lp) lo.

Definition wrap (sp : option expr) (s : plan) : plan :=
  match sp with Some e => PFilter e s | None => s end.

(** Either the side is unchanged-up-to-check, or a filter was pushed onto it. *)
Definition side (chk : plan -> bool) (s' : plan) (P : option expr -> bool) : bool :=
  (chk s' && P None) ||
  match s' with PFilter e s0 => chk s0 && P (Some e) | _ => false end.

(** * The checker *)

Fixpoint check2 (o n : plan) {struct o} : bool :=
  plan_eqb o n ||
  match o, n with
  | PFilter c p, PFilter c' p' =>
      (filter_equiv c c' && check2 p p') ||
      match p with
      | PFilter c2 q => filter_equiv (EAnd c2 c) c' && check2 q p'    (* CombineFilters *)
      | _ => false
      end ||
      match p, p' with                                  (* LEFT OUTER push, rest kept *)
      | PJoin LeftOuter jc l r, PJoin LeftOuter jc' l' r' =>
          oexpr_eqb jc jc' && check2 r r' &&
          side (check2 l) l' (fun lp => lo_ok c (Some c') lp (out l))
      | _, _ => false
      end
  | PFilter c p, PProject ids' p' =>                                 (* through Project *)
      match p, p' with
      | PProject ids q, PFilter c' q' =>
          ids_eqb ids ids' && subset (refs c) ids && subset ids (out q) &&
          ((filter_equiv c c' && check2 q q') ||
           match q with                                 (* push + CombineFilters *)
           | PFilter c2 q0 => filter_equiv (EAnd c2 c) c' && check2 q0 q'
           | _ => false
           end)
      | _, _ => false
      end
  | PFilter c p, PJoin Inner jc' l' r' =>                            (* through Join *)
      match p with
      | PJoin Inner jc l r =>
          side (check2 l) l' (fun lp =>
            side (check2 r) r' (fun rp => push_ok c jc jc' (out l) (out r) lp rp))
      | _ => false
      end
  | PFilter c p, PJoin LeftOuter jc' l' r' =>                        (* LEFT OUTER push *)
      match p with
      | PJoin LeftOuter jc l r =>
          oexpr_eqb jc jc' && check2 r r' &&
          side (check2 l) l' (fun lp => lo_ok c None lp (out l))
      | _ => false
      end
  | PProject ids p, PProject ids' p' => ids_eqb ids ids' && check2 p p'
  | POp1 k o1 p, POp1 k' o1' p' => Nat.eqb k k' && ids_eqb o1 o1' && check2 p p'
  | PJoin j c l r, PJoin j' c' l' r' =>                               (* IFFC guards *)
      jk_eqb j j' && oexpr_eqb c c' &&
      (check2 l l' ||
       match l' with
       | PFilter g l0 => check2 l l0 && guard_ok j c true (out l) (out r) g
       | _ => false
       end) &&
      (check2 r r' ||
       match r' with
       | PFilter g r0 => check2 r r0 && guard_ok j c false (out l) (out r) g
       | _ => false
       end)
  | _, _ => false
  end.

(** * Semantic lemmas *)

Lemma and3_is_true : forall a b, is_true (and3 a b) = is_true a && is_true b.
Proof. intros [[[x|[|]]|]|] [[[y|[|]]|]|]; reflexivity. Qed.

Lemma conj_true : forall e t,
    is_true (eval_e e t) = forallb (fun d => is_true (eval_e d t)) (conjs e).
Proof.
  induction e; intros t; simpl; try (rewrite andb_true_r; reflexivity).
  rewrite and3_is_true, forallb_app, IHe1, IHe2. reflexivity.
Qed.

Lemma mem_expr_In : forall d l, mem_expr d l = true <-> In d l.
Proof.
  intros d l. unfold mem_expr. rewrite existsb_exists. split.
  - intros [y [Hy Heq]]. unfold expr_eqb in Heq.
    destruct (expr_eq_dec d y); [subst; exact Hy | discriminate].
  - intros H. exists d. split; [exact H|]. unfold expr_eqb.
    destruct (expr_eq_dec d d); [reflexivity | contradiction].
Qed.

Lemma forallb_incl : forall (P : expr -> bool) a b,
    forallb (fun d => mem_expr d b) a = true ->
    forallb P b = true -> forallb P a = true.
Proof.
  intros P a b Hab Hb. apply forallb_forall. intros d Hd.
  rewrite forallb_forall in Hab, Hb. apply Hb. apply mem_expr_In. exact (Hab d Hd).
Qed.

Lemma seteq_forallb : forall (P : expr -> bool) a b,
    seteq a b = true -> forallb P a = forallb P b.
Proof.
  intros P a b H. unfold seteq in H. apply andb_true_iff in H as [Hab Hba].
  destruct (forallb P a) eqn:Ea, (forallb P b) eqn:Eb; try reflexivity.
  - rewrite (forallb_incl P b a Hba Ea) in Eb. discriminate.
  - rewrite (forallb_incl P a b Hab Eb) in Ea. discriminate.
Qed.

Lemma filter_equiv_sound : forall c c' t, filter_equiv c c' = true ->
    is_true (eval_e c t) = is_true (eval_e c' t).
Proof.
  intros c c' t H. unfold filter_equiv in H. apply andb_true_iff in H as [Hin Himp].
  destruct (is_true (eval_e c t)) eqn:Ec, (is_true (eval_e c' t)) eqn:Ec'; try reflexivity.
  - (* c TRUE -> c' TRUE *)
    exfalso. rewrite conj_true in Ec'.
    assert (Hall : forallb (fun d => is_true (eval_e d t)) (conjs c') = true).
    { apply forallb_forall. intros d Hd. rewrite forallb_forall in Himp.
      pose proof (Himp d Hd) as Hd'. unfold implied_conj in Hd'.
      apply orb_true_iff in Hd' as [Hm|Hnn].
      - rewrite conj_true in Ec. rewrite forallb_forall in Ec.
        apply Ec. apply mem_expr_In. exact Hm.
      - destruct d as [| | | | | | | | a | |]; try discriminate.
        destruct a as [x| | | | | | | | | |]; try discriminate.
        apply memb_In in Hnn.
        destruct (nr_sound c x t Hnn Ec) as [v Hv]. simpl. rewrite Hv. reflexivity. }
    congruence.
  - (* c' TRUE -> c TRUE *)
    exfalso. rewrite conj_true in Ec, Ec'.
    rewrite (forallb_incl _ (conjs c) (conjs c') Hin Ec') in Ec. discriminate.
Qed.

Lemma eval_agree : forall e t1 t2,
    (forall x, In x (refs e) -> lookup x t1 = lookup x t2) ->
    eval_e e t1 = eval_e e t2.
Proof.
  induction e; intros t1 t2 H; simpl in *;
    try (rewrite (IHe1 t1 t2), (IHe2 t1 t2);
         [reflexivity | intros; apply H; apply in_or_app; auto ..]);
    try reflexivity.
  - apply H. left. reflexivity.
  - rewrite (IHe t1 t2 H). reflexivity.
Qed.

Lemma lookup_in_keys : forall x t, In x (map fst t) -> exists v, lookup x t = Some v.
Proof.
  intros x t. induction t as [|[y v] t IH]; simpl; [tauto|].
  intros [<-|H]; [rewrite Nat.eqb_refl; eauto|].
  destruct (Nat.eqb x y); eauto.
Qed.

Lemma lookup_normalize : forall ks x t, In x ks -> In x (map fst t) ->
    lookup x (normalize ks t) = lookup x t.
Proof.
  intros ks x t Hk Ht. destruct (lookup_in_keys x t Ht) as [v Hv].
  rewrite Hv. unfold normalize.
  induction ks as [|k ks IH]; simpl in *; [contradiction|].
  destruct (Nat.eqb x k) eqn:E.
  - apply Nat.eqb_eq in E. subst. unfold getv. rewrite Hv. reflexivity.
  - destruct Hk as [<-|Hk]; [rewrite Nat.eqb_refl in E; discriminate | auto].
Qed.

Lemma filter_filter_and : forall (A : Type) (f g : A -> bool) l,
    filter f (filter g l) = filter (fun x => g x && f x) l.
Proof.
  intros A f g l. induction l as [|x l IH]; simpl; [reflexivity|].
  destruct (g x); simpl; [destruct (f x); simpl; congruence | exact IH].
Qed.

Lemma filter_map_comm : forall (A B : Type) (f : B -> bool) (g : A -> B) l,
    filter f (map g l) = map g (filter (fun x => f (g x)) l).
Proof.
  intros A B f g l. induction l as [|x l IH]; simpl; [reflexivity|].
  destruct (f (g x)); simpl; congruence.
Qed.

Lemma filter_ext_in' : forall (A : Type) (f g : A -> bool) l,
    (forall x, In x l -> f x = g x) -> filter f l = filter g l.
Proof.
  intros A f g l H. induction l as [|x l IH]; simpl; [reflexivity|].
  rewrite (H x (or_introl eq_refl)).
  rewrite IH; [reflexivity | intros y Hy; apply H; right; exact Hy].
Qed.

Definition opt_pred (sp : option expr) : row -> bool :=
  match sp with Some e => fun t => is_true (eval_e e t) | None => fun _ => true end.

Lemma opt_pred_conj : forall sp t,
    opt_pred sp t = forallb (fun d => is_true (eval_e d t)) (conjs_opt sp).
Proof. intros [e|] t; simpl; [apply conj_true | reflexivity]. Qed.

Lemma cond_true_conj : forall c t,
    cond_true c t = forallb (fun d => is_true (eval_e d t)) (conjs_opt c).
Proof. intros [e|] t; simpl; [apply conj_true | reflexivity]. Qed.

Lemma opt_pred_agree : forall sp t1 t2,
    (forall x, In x (refs_opt sp) -> lookup x t1 = lookup x t2) ->
    opt_pred sp t1 = opt_pred sp t2.
Proof. intros [e|] t1 t2 H; simpl in *; [now rewrite (eval_agree e t1 t2 H) | reflexivity]. Qed.

Lemma eval_wrap : forall db ops sp s,
    eval db ops (wrap sp s) = filter (opt_pred sp) (eval db ops s).
Proof. intros db ops [e|] s; simpl; [reflexivity | symmetry; now apply filter_all]. Qed.

Lemma out_wrap : forall sp s, out (wrap sp s) = out s.
Proof. intros [e|] s; reflexivity. Qed.

Lemma side_inv : forall chk s' P, side chk s' P = true ->
    exists sp s0, P sp = true /\ chk s0 = true /\ s' = wrap sp s0.
Proof.
  intros chk s' P H. unfold side in H. apply orb_true_iff in H as [H|H].
  - apply andb_true_iff in H as [Hc HP]. exists None, s'. auto.
  - destruct s' as [| e s0 | | |]; try discriminate.
    apply andb_true_iff in H as [Hc HP]. exists (Some e), s0. auto.
Qed.

Lemma inner_join_filter_shift : forall jc jc' (C L R : row -> bool) ro ro' ls rs,
    (forall lr rr, In lr ls -> In rr rs ->
       cond_true jc (lr ++ rr) && C (lr ++ rr) = L lr && R rr && cond_true jc' (lr ++ rr)) ->
    filter C (join Inner jc ro ls rs) = join Inner jc' ro' (filter L ls) (filter R rs).
Proof.
  intros jc jc' C L R ro ro' ls rs H. unfold row in *.
  induction ls as [|lr ls IH]; [reflexivity|].
  assert (IH' := IH (fun a b Ha => H a b (or_intror Ha))).
  unfold join in *. cbn [flat_map filter].
  rewrite filter_app.
  destruct (L lr) eqn:EL; cbn [flat_map].
  - f_equal; [|exact IH']. unfold matches.
    rewrite filter_map_comm, filter_filter_and, filter_filter_and.
    f_equal. apply filter_ext_in'. intros rr Hrr.
    rewrite (H lr rr (or_introl eq_refl) Hrr), EL. reflexivity.
  - assert (Hnil : filter C (map (app lr) (matches jc lr rs)) = []).
    { unfold matches. rewrite filter_map_comm, filter_filter_and.
      rewrite filter_none; [reflexivity|]. intros rr Hrr.
      rewrite (H lr rr (or_introl eq_refl) Hrr), EL. reflexivity. }
    rewrite Hnil. exact IH'.
Qed.

(** Generic forms of the V15 side lemmas, for any recursive checker [chk]. *)
Lemma left_side_gen : forall (chk : plan -> plan -> bool) j c l r l',
    (forall n, chk l n = true -> sound_rel l n) ->
    (chk l l' ||
     match l' with
     | PFilter g l0 => chk l l0 && guard_ok j c true (out l) (out r) g
     | _ => false
     end) = true ->
    exists G : row -> bool,
      out l' = out l /\
      (forall db ops, eval db ops l' = filter G (eval db ops l)) /\
      (j = Inner \/ forall t, G t = true) /\
      (forall lr rr, map fst lr = out l -> cond_true c (lr ++ rr) = true -> G lr = true).
Proof.
  intros chk j c l r l' IH H. apply orb_true_iff in H as [H|H].
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

Lemma right_side_gen : forall (chk : plan -> plan -> bool) j c l r r',
    (forall n, chk r n = true -> sound_rel r n) ->
    (chk r r' ||
     match r' with
     | PFilter g r0 => chk r r0 && guard_ok j c false (out l) (out r) g
     | _ => false
     end) = true ->
    exists G : row -> bool,
      out r' = out r /\
      (forall db ops, eval db ops r' = filter G (eval db ops r)) /\
      (forall lr rr, map fst lr = out l -> map fst rr = out r ->
         cond_true c (lr ++ rr) = true -> G rr = true).
Proof.
  intros chk j c l r r' IH H. apply orb_true_iff in H as [H|H].
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

Lemma subset_In : forall a b x, subset a b = true -> In x a -> In x b.
Proof.
  intros a b x H Hx. unfold subset in H. rewrite forallb_forall in H.
  apply memb_In. exact (H x Hx).
Qed.

Lemma disjoint_notin : forall a b x, disjoint a b = true -> In x a -> ~ In x b.
Proof.
  intros a b x H Hx Hb. unfold disjoint in H. rewrite forallb_forall in H.
  pose proof (H x Hx) as Hn. apply negb_true_iff in Hn.
  apply memb_In in Hb. congruence.
Qed.

Lemma left_outer_filter_shift : forall jc (C L Rest : row -> bool) ro ls rs,
    (forall lr x, In lr ls -> C (lr ++ x) = L lr && Rest (lr ++ x)) ->
    filter C (join LeftOuter jc ro ls rs) = filter Rest (join LeftOuter jc ro (filter L ls) rs).
Proof.
  intros jc C L Rest ro ls rs H. unfold row in *.
  induction ls as [|lr ls IH]; [reflexivity|].
  assert (IH' := IH (fun a x Ha => H a x (or_intror Ha))).
  unfold join in *. cbn [flat_map filter]. rewrite filter_app.
  destruct (L lr) eqn:EL; cbn [flat_map]; rewrite ?filter_app.
  - f_equal; [|exact IH']. apply filter_ext_in'. intros t Ht.
    destruct (matches jc lr rs) as [|m ms].
    + destruct Ht as [<-|[]]. rewrite (H lr _ (or_introl eq_refl)), EL. reflexivity.
    + apply in_map_iff in Ht as [x [<- _]].
      rewrite (H lr x (or_introl eq_refl)), EL. reflexivity.
  - assert (Hnil : forall blk, (forall t, In t blk -> exists x, t = lr ++ x) ->
              filter C blk = []).
    { intros blk Hb. apply filter_none. intros t Ht.
      destruct (Hb t Ht) as [x ->]. rewrite (H lr x (or_introl eq_refl)), EL. reflexivity. }
    rewrite Hnil; [exact IH'|]. intros t Ht.
    destruct (matches jc lr rs) as [|m ms].
    + destruct Ht as [<-|[]]. eauto.
    + apply in_map_iff in Ht as [x [<- _]]. eauto.
Qed.

Lemma lo_bool : forall c rest lp lo lr x,
    lo_ok c rest lp lo = true -> map fst lr = lo ->
    is_true (eval_e c (lr ++ x)) = opt_pred lp lr && opt_pred rest (lr ++ x).
Proof.
  intros c rest lp lo lr x H Hk. unfold lo_ok in H.
  apply andb_true_iff in H as [Hseq Hsub].
  rewrite (opt_pred_agree lp lr (lr ++ x)).
  2: { intros y Hy. symmetry. apply lookup_app_in. rewrite Hk.
       exact (subset_In _ _ _ Hsub Hy). }
  rewrite (conj_true c), (opt_pred_conj lp), (opt_pred_conj rest), <- forallb_app.
  apply seteq_forallb. exact Hseq.
Qed.

Lemma push_bool : forall c jc jc' lp rp t,
    seteq (conjs_opt jc ++ conjs c) (conjs_opt lp ++ conjs_opt rp ++ conjs_opt jc') = true ->
    cond_true jc t && is_true (eval_e c t) = opt_pred lp t && opt_pred rp t && cond_true jc' t.
Proof.
  intros c jc jc' lp rp t H.
  rewrite (cond_true_conj jc t), (conj_true c t), (opt_pred_conj lp t),
          (opt_pred_conj rp t), (cond_true_conj jc' t).
  rewrite <- forallb_app, <- andb_assoc, <- forallb_app, <- forallb_app.
  apply seteq_forallb. exact H.
Qed.

Fixpoint size (p : plan) : nat :=
  match p with
  | PLeaf _ _ => 1
  | PFilter _ q | PProject _ q | POp1 _ _ q => S (size q)
  | PJoin _ _ l r => S (size l + size r)
  end.

Theorem check2_correct_k : forall k o n, size o <= k -> check2 o n = true -> sound_rel o n.
Proof.
  induction k as [|k IHk]; intros o n Hsize Hc; [destruct o; simpl in Hsize; lia|].
  assert (IH : forall q, size q <= k -> forall m, check2 q m = true -> sound_rel q m)
    by (intros q Hq m Hm; exact (IHk q m Hq Hm)).
  destruct o as [kk o1|c p|ids p|kk o1 p|j c l r];
    simpl in Hc; apply orb_true_iff in Hc as [Heq|Hc];
    try (apply (eqb_dec_true _ plan_eq_dec) in Heq; subst; split; reflexivity);
    simpl in Hsize.
  - (* Leaf *) destruct n; discriminate.
  - (* Filter *)
    destruct n as [| c' p' | ids' p' | | j' jc' l' r']; try discriminate.
    + (* Filter / Filter *)
      apply orb_true_iff in Hc as [Hc|Hlo]; [apply orb_true_iff in Hc as [Hc|Hc]|].
      * apply andb_true_iff in Hc as [Hfe Hp].
        destruct (IH p ltac:(lia) _ Hp) as [Ho He]. split; [exact Ho|].
        intros db ops. simpl. rewrite <- He.
        apply filter_ext_in'. intros t _. apply filter_equiv_sound. exact Hfe.
      * destruct p as [| c2 q | | |]; try discriminate.
        apply andb_true_iff in Hc as [Hfe Hq]. simpl in Hsize.
        destruct (IH q ltac:(lia) _ Hq) as [Ho He]. split; [exact Ho|].
        intros db ops. simpl. rewrite filter_filter_and, <- He.
        apply filter_ext_in'. intros t _.
        rewrite <- (filter_equiv_sound _ _ t Hfe). simpl.
        rewrite and3_is_true. reflexivity.
      * (* LEFT OUTER push with remaining filter *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        destruct p' as [| | | | j2 jc' l' r']; try discriminate. destruct j2; try discriminate.
        apply andb_true_iff in Hlo as [Hlo Hside]. apply andb_true_iff in Hlo as [Hjc Hr].
        apply (eqb_dec_true _ oexpr_eq_dec) in Hjc. subst jc'.
        simpl in Hsize.
        destruct (side_inv _ _ _ Hside) as [lp [l0 [Hok [Hl0 ->]]]].
        destruct (IH l ltac:(lia) _ Hl0) as [Hol Hel].
        destruct (IH r ltac:(lia) _ Hr) as [Hor Her].
        split.
        -- simpl. rewrite out_wrap. now rewrite Hol, Hor.
        -- intros db ops. cbn [eval]. rewrite eval_wrap, <- Hel, <- Her, <- Hor.
           apply left_outer_filter_shift. intros lr x Hlr.
           exact (lo_bool c (Some c') lp (out l) lr x Hok (eval_keys db ops l lr Hlr)).
    + (* push through Project, optionally combined with the filter below *)
      destruct p as [| | ids q | |]; try discriminate.
      destruct p' as [| c' q' | | |]; try discriminate.
      apply andb_true_iff in Hc as [Hc Halt]. apply andb_true_iff in Hc as [Hc Hiq].
      apply andb_true_iff in Hc as [Hids Hrefs].
      apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hids. subst ids'.
      simpl in Hsize.
      assert (Hnorm : forall t, map fst t = out q ->
                is_true (eval_e c (normalize ids t)) = is_true (eval_e c t)).
      { intros t Ht. f_equal. apply eval_agree. intros x Hx.
        pose proof (subset_In _ _ _ Hrefs Hx) as Hxi.
        apply lookup_normalize; [exact Hxi|]. rewrite Ht. exact (subset_In _ _ _ Hiq Hxi). }
      apply orb_true_iff in Halt as [Halt|Halt].
      * apply andb_true_iff in Halt as [Hfe Hq].
        destruct (IH q ltac:(lia) _ Hq) as [Ho He]. split; [reflexivity|].
        intros db ops. simpl. rewrite <- He, filter_map_comm. f_equal.
        apply filter_ext_in'. intros t Ht.
        rewrite (Hnorm t (eval_keys db ops q t Ht)). apply filter_equiv_sound. exact Hfe.
      * destruct q as [| c2 q0 | | |]; try discriminate.
        apply andb_true_iff in Halt as [Hfe Hq]. simpl in Hsize.
        destruct (IH q0 ltac:(lia) _ Hq) as [Ho He]. split; [reflexivity|].
        intros db ops. cbn [eval]. rewrite <- He, filter_map_comm, filter_filter_and. f_equal.
        apply filter_ext_in'. intros t Ht.
        assert (Hk : map fst t = out (PFilter c2 q0)) by (simpl; exact (eval_keys db ops q0 t Ht)).
        rewrite (Hnorm t Hk), <- (filter_equiv_sound _ _ t Hfe). simpl.
        rewrite and3_is_true. reflexivity.
    + destruct j'.
      * (* push through Inner Join *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        simpl in Hsize.
        destruct (side_inv _ _ _ Hc) as [lp [l0 [Hr [Hl0 ->]]]].
        destruct (side_inv _ _ _ Hr) as [rp [r0 [Hok [Hr0 ->]]]].
        destruct (IH l ltac:(lia) _ Hl0) as [Hol Hel].
        destruct (IH r ltac:(lia) _ Hr0) as [Hor Her].
        unfold push_ok in Hok.
        apply andb_true_iff in Hok as [Hok Hdis]. apply andb_true_iff in Hok as [Hok Hrs].
        apply andb_true_iff in Hok as [Hseq Hls].
        split.
        -- simpl. rewrite !out_wrap. now rewrite Hol, Hor.
        -- intros db ops. cbn [eval]. rewrite !eval_wrap, <- Hel, <- Her.
           apply inner_join_filter_shift. intros lr rr Hlr Hrr.
           pose proof (eval_keys db ops l lr Hlr) as Kl.
           pose proof (eval_keys db ops r rr Hrr) as Kr.
           rewrite (opt_pred_agree lp lr (lr ++ rr)).
           2: { intros x Hx. symmetry. apply lookup_app_in. rewrite Kl.
                exact (subset_In _ _ _ Hls Hx). }
           rewrite (opt_pred_agree rp rr (lr ++ rr)).
           2: { intros x Hx. symmetry. apply lookup_app_notin. rewrite Kl.
                exact (disjoint_notin _ _ _ Hdis Hx). }
           apply push_bool. exact Hseq.
      * (* LEFT OUTER push, nothing left above *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        apply andb_true_iff in Hc as [Hc Hside]. apply andb_true_iff in Hc as [Hjc Hr].
        apply (eqb_dec_true _ oexpr_eq_dec) in Hjc. subst jc'.
        simpl in Hsize.
        destruct (side_inv _ _ _ Hside) as [lp [l0 [Hok [Hl0 ->]]]].
        destruct (IH l ltac:(lia) _ Hl0) as [Hol Hel].
        destruct (IH r ltac:(lia) _ Hr) as [Hor Her].
        split.
        -- simpl. rewrite out_wrap. now rewrite Hol, Hor.
        -- intros db ops. cbn [eval]. rewrite eval_wrap, <- Hel, <- Her, <- Hor.
           rewrite <- (filter_all _ (fun _ => true)
             (join LeftOuter jc (out r) (filter (opt_pred lp) (eval db ops l)) (eval db ops r)))
             by reflexivity.
           apply left_outer_filter_shift. intros lr x Hlr.
           exact (lo_bool c None lp (out l) lr x Hok (eval_keys db ops l lr Hlr)).
  - (* Project *)
    destruct n; try discriminate.
    apply andb_true_iff in Hc as [Hi Hp].
    apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hi. subst.
    destruct (IH p ltac:(lia) _ Hp) as [Ho He]. split; [reflexivity|].
    intros db ops. simpl. now rewrite He.
  - (* opaque unary *)
    destruct n; try discriminate.
    apply andb_true_iff in Hc as [Hk Hp]. apply andb_true_iff in Hk as [Hk Hi].
    apply Nat.eqb_eq in Hk. apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hi. subst.
    destruct (IH p ltac:(lia) _ Hp) as [Ho He]. split; [reflexivity|].
    intros db ops. simpl. now rewrite He.
  - (* Join with IFFC guards *)
    destruct n as [| | | | j' c' l' r']; try discriminate.
    apply andb_true_iff in Hc as [Hc HR]. apply andb_true_iff in Hc as [Hc HL].
    apply andb_true_iff in Hc as [Hj Hce].
    assert (j' = j) by (destruct j, j'; try discriminate; reflexivity). subst j'.
    apply (eqb_dec_true _ oexpr_eq_dec) in Hce. subst c'.
    destruct (left_side_gen check2 j c l r l' (IH l ltac:(lia)) HL)
      as [Gl [Hol [Hel [Hjl Hcl]]]].
    destruct (right_side_gen check2 j c l r r' (IH r ltac:(lia)) HR)
      as [Gr [Hor [Her Hcr]]].
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

Theorem check2_sound : forall o n, check2 o n = true ->
    forall db ops, eval db ops o = eval db ops n.
Proof.
  intros o n H. exact (proj2 (check2_correct_k (size o) o n (le_n _) H)).
Qed.

(** * Kernel-evaluated controls *)

Definition t_left := PLeaf 0 [0; 1; 2].
Definition t_right := PLeaf 1 [3; 4; 5].
Definition jcond := Some (EEq (EAttr 0) (EAttr 3)).
Definition pos_j1 := EGt (EAttr 1) (ELit (Some (VInt 0))).
Definition pos_r4 := ELt (EAttr 4) (ELit (Some (VInt 3))).

Example accepts_v15_guard_shape : check2 w_old w_new = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_iffc_filter_merge :
  check2 (PFilter pos_j1 t_left)
         (PFilter (EAnd (EIsNotNull (EAttr 1)) pos_j1) t_left) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_merge_of_unimplied_guard :
  check2 (PFilter pos_j1 t_left)
         (PFilter (EAnd (EIsNotNull (EAttr 2)) pos_j1) t_left) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_combine_filters :
  check2 (PFilter pos_j1 (PFilter (EIsNotNull (EAttr 0)) t_left))
         (PFilter (EAnd (EIsNotNull (EAttr 0)) pos_j1) t_left) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_push_through_project :
  check2 (PFilter (EIsNotNull (EAttr 0)) (PProject [0] t_left))
         (PProject [0] (PFilter (EIsNotNull (EAttr 0)) t_left)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_push_of_unprojected_reference :
  check2 (PFilter (EIsNotNull (EAttr 0)) (PProject [0] t_left))
         (PProject [0] (PFilter (EIsNotNull (EAttr 1)) t_left)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_push_through_inner_join :
  check2 (PFilter (EAnd pos_j1 (EAnd pos_r4 (ELt (EAttr 1) (EAttr 4))))
            (PJoin Inner jcond t_left t_right))
         (PJoin Inner (Some (EAnd (ELt (EAttr 1) (EAttr 4)) (EEq (EAttr 0) (EAttr 3))))
            (PFilter pos_j1 t_left) (PFilter pos_r4 t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_push_to_wrong_side :
  check2 (PFilter pos_j1 (PJoin Inner jcond t_left t_right))
         (PJoin Inner jcond t_left (PFilter pos_j1 t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_dropped_conjunct :
  check2 (PFilter (EAnd pos_j1 pos_r4) (PJoin Inner jcond t_left t_right))
         (PJoin Inner jcond (PFilter pos_j1 t_left) t_right) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_push_through_left_outer :
  check2 (PFilter pos_r4 (PJoin LeftOuter jcond t_left t_right))
         (PJoin LeftOuter jcond t_left (PFilter pos_r4 t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_push_project_and_combine :
  check2 (PFilter (EIsNotNull (EAttr 0)) (PProject [0] (PFilter pos_j1 t_left)))
         (PProject [0] (PFilter (EAnd pos_j1 (EIsNotNull (EAttr 0))) t_left)) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_preserved_side_push :
  check2 (PFilter pos_j1 (PJoin LeftOuter jcond t_left t_right))
         (PJoin LeftOuter jcond (PFilter pos_j1 t_left) t_right) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_push_keeping_right_conjunct :
  check2 (PFilter (EAnd pos_j1 pos_r4) (PJoin LeftOuter jcond t_left t_right))
         (PFilter pos_r4 (PJoin LeftOuter jcond (PFilter pos_j1 t_left) t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_left_outer_push_dropping_right_conjunct :
  check2 (PFilter (EAnd pos_j1 pos_r4) (PJoin LeftOuter jcond t_left t_right))
         (PJoin LeftOuter jcond (PFilter pos_j1 t_left) t_right) = false.
Proof. vm_compute. reflexivity. Qed.

Print Assumptions check2_sound.
