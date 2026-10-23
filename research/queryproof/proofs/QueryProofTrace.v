(** V20: composition of kernel-decided rule certificates across an observed
    Catalyst optimizer trace, under an explicit hidden-state effect contract.

    1. [prn]/[check4] extend V19's [check3] with Spark's projection rewrites
       (ColumnPruning, CollapseProject, RemoveNoopOperators): "equal on the
       needed columns N", proved through Project, Filter, INNER/LEFT OUTER
       Join and opaque operators. Under an opaque operator the input must be
       unchanged; that is conservative, because opaque operators may read any column.
    2. [trace_ok] checks every event, CONNECTIVITY (the output of event i is
       syntactically the input of event i+1, which exposes omitted plan changes)
       and every observed hidden-tag list.
    3. The EFFECT CONTRACT is an explicit section hypothesis: the meaning of a
       plan does not depend on hidden optimizer state (tags, ineffective-rule
       bits, caches) once all observed tags are contract-OK. Hidden state may
       still steer WHICH rewrites the optimizer performs; that cannot break
       soundness, because every performed rewrite is certified on the observed
       plans. [trace_sound] then equates the meanings of the first and last plans.

    The contract itself is NOT proved. It is justified by a static Spark 4.2.0
    tag read-site inventory (TAG_INVENTORY.json, EFFECT_CONTRACT.json) and
    checked dynamically on observed tag names.
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool Arith.PeanoNat Lia.
Import ListNotations.
Require Import QueryProofDeepWide.

Fixpoint nodupb (l : list nat) : bool :=
  match l with [] => true | x :: r => negb (memb x r) && nodupb r end.
Definition restrict (xs dom : list nat) : list nat := filter (fun x => memb x dom) xs.

Fixpoint prn (k : nat) (N : list nat) (o n : plan) : bool :=
  match k with
  | 0 => false
  | S k =>
    (ids_eqb (out o) (out n) && check3 o n) ||
    match n with
    | PProject b n0 => subset N b && subset b (out n0) && prn k N o n0
    | _ => false
    end ||
    match o with
    | PProject a o0 => subset N a && subset a (out o0) && prn k N o0 n
    | _ => false
    end ||
    match o, n with
    | PProject a o0, PProject a' n0 => ids_eqb a a' && prn k a o0 n0
    | PFilter c o0, PFilter c' n0 =>
        cond_equiv (conjs c) (conjs c') && subset (refs c) (out o0) &&
        subset (refs c) (out n0) && prn k (N ++ refs c) o0 n0
    | PJoin j c l r, PJoin j' c' l' r' =>
        jk_eqb j j' && oexpr_eqb c c' &&
        disjoint (out l) (out r) && disjoint (out l') (out r') &&
        subset (out l') (out l) && subset (out r') (out r) &&
        subset (refs_opt c) (out l ++ out r) &&
        subset (restrict (N ++ refs_opt c) (out l)) (out l') &&
        subset (restrict (N ++ refs_opt c) (out r)) (out r') &&
        prn k (restrict (N ++ refs_opt c) (out l)) l l' &&
        prn k (restrict (N ++ refs_opt c) (out r)) r r'
    | POp1 kk o1 o0, POp1 kk' o1' n0 =>
        Nat.eqb kk kk' && ids_eqb o1 o1' && ids_eqb (out o0) (out n0) &&
        nodupb (out o0) && prn k (out o0) o0 n0
    | _, _ => false
    end
  end.

Definition check4 (o n : plan) : bool :=
  check3 o n || (ids_eqb (out o) (out n) && nodupb (out o) && prn (size o + size n) (out o) o n).

(** * Row lemmas *)

Lemma lookup_some_key : forall x t v, lookup x t = Some v -> In x (map fst t).
Proof.
  intros x t v. induction t as [|[y w] t IH]; simpl; [discriminate|].
  destruct (Nat.eqb x y) eqn:E; [apply Nat.eqb_eq in E; subst; auto | auto].
Qed.

Lemma lookup_none_key : forall x t, ~ In x (map fst t) -> lookup x t = None.
Proof.
  intros x t H. destruct (lookup x t) as [v|] eqn:E; [|reflexivity].
  exfalso. apply H. exact (lookup_some_key x t v E).
Qed.

Lemma lookup_getv : forall x t, In x (map fst t) -> lookup x t = Some (getv x t).
Proof.
  intros x t H. destruct (lookup_in_keys x t H) as [v Hv]. unfold getv. now rewrite Hv.
Qed.

Lemma getv_notin : forall x t, ~ In x (map fst t) -> getv x t = None.
Proof. intros x t H. unfold getv. now rewrite lookup_none_key. Qed.

Lemma getv_app_in : forall x a b, In x (map fst a) -> getv x (a ++ b) = getv x a.
Proof. intros. unfold getv. now rewrite lookup_app_in. Qed.

Lemma getv_app_notin : forall x a b, ~ In x (map fst a) -> getv x (a ++ b) = getv x b.
Proof. intros. unfold getv. now rewrite lookup_app_notin. Qed.

Lemma lookup_norm_in : forall M x t, In x M -> lookup x (normalize M t) = Some (getv x t).
Proof.
  intros M x t. unfold normalize. induction M as [|m M IH]; simpl; [tauto|].
  destruct (Nat.eqb x m) eqn:E.
  - apply Nat.eqb_eq in E. subst. auto.
  - intros [<-|H]; [rewrite Nat.eqb_refl in E; discriminate | auto].
Qed.

Lemma getv_norm_in : forall M x t, In x M -> getv x (normalize M t) = getv x t.
Proof. intros. unfold getv at 1. now rewrite lookup_norm_in. Qed.

Lemma getv_norm_notin : forall M x t, ~ In x M -> getv x (normalize M t) = None.
Proof. intros. apply getv_notin. now rewrite normalize_keys. Qed.

Lemma getv_pad : forall x K, getv x (pad K) = None.
Proof.
  intros x K. unfold getv, pad. induction K as [|k K IH]; simpl; [reflexivity|].
  destruct (Nat.eqb x k); [reflexivity | exact IH].
Qed.

Lemma normalize_ext : forall N t1 t2,
    (forall x, In x N -> getv x t1 = getv x t2) -> normalize N t1 = normalize N t2.
Proof.
  intros N t1 t2 H. unfold normalize. apply map_ext_in. intros x Hx. now rewrite H.
Qed.

Lemma normalize_compose : forall N M t, subset N M = true ->
    normalize N (normalize M t) = normalize N t.
Proof.
  intros N M t H. apply normalize_ext. intros x Hx.
  apply getv_norm_in. exact (subset_In _ _ _ H Hx).
Qed.

Lemma memb_false : forall x xs, memb x xs = false -> ~ In x xs.
Proof. intros x xs H Hin. apply memb_In in Hin. congruence. Qed.

Lemma normalize_self : forall t ks, map fst t = ks -> nodupb ks = true -> normalize ks t = t.
Proof.
  induction t as [|[k v] t IH]; intros ks Hk Hnd; simpl in Hk; subst ks; [reflexivity|].
  simpl in Hnd. apply andb_true_iff in Hnd as [Hn Hnd]. apply negb_true_iff in Hn.
  unfold normalize. simpl. unfold getv at 1. simpl. rewrite Nat.eqb_refl. f_equal.
  rewrite <- (IH (map fst t) eq_refl Hnd) at 2. unfold normalize.
  apply map_ext_in. intros x Hx. f_equal. unfold getv. simpl.
  destruct (Nat.eqb x k) eqn:E; [|reflexivity].
  apply Nat.eqb_eq in E. subst. exfalso. exact (memb_false _ _ Hn Hx).
Qed.

Lemma map_normalize_self : forall db ops p,
    nodupb (out p) = true -> map (normalize (out p)) (eval db ops p) = eval db ops p.
Proof.
  intros db ops p Hnd. rewrite <- map_id. apply map_ext_in. intros t Ht.
  apply normalize_self; [exact (eval_keys db ops p t Ht) | exact Hnd].
Qed.

(** * Normalised join lemma *)

Lemma restrict_in : forall x xs dom, In x xs -> In x dom -> In x (restrict xs dom).
Proof. intros. apply filter_In. split; [auto | now apply memb_In]. Qed.

Lemma restrict_sub : forall x xs dom, In x (restrict xs dom) -> In x dom.
Proof. intros x xs dom H. apply filter_In in H as [_ H]. now apply memb_In. Qed.

Section JoinNorm.
  Variables (N Nl Nr KL KR : list nat) (c : option expr).
  Hypothesis Hdis : forall x, In x KL -> ~ In x KR.
  Hypothesis HNl : forall x, In x Nl -> In x KL.
  Hypothesis HNr : forall x, In x Nr -> In x KR.
  Hypothesis Hrefs : forall x, In x (refs_opt c) -> In x Nl \/ In x Nr.
  Hypothesis HNL : forall x, In x N -> In x KL -> In x Nl.
  Hypothesis HNR : forall x, In x N -> In x KR -> In x Nr.

  Lemma pair_getv : forall lr rr x,
      map fst lr = KL -> map fst rr = KR -> (In x Nl \/ In x Nr) ->
      getv x (normalize Nl lr ++ normalize Nr rr) = getv x (lr ++ rr).
  Proof.
    intros lr rr x Kl Kr [H|H].
    - rewrite (getv_app_in x (normalize Nl lr) (normalize Nr rr)) by (rewrite normalize_keys; exact H).
      rewrite (getv_app_in x lr rr) by (rewrite Kl; exact (HNl x H)).
      now apply getv_norm_in.
    - assert (Hnl : ~ In x Nl) by (intros Hl; exact (Hdis x (HNl x Hl) (HNr x H))).
      rewrite (getv_app_notin x (normalize Nl lr) (normalize Nr rr)) by (rewrite normalize_keys; exact Hnl).
      rewrite (getv_app_notin x lr rr) by (rewrite Kl; intros Hk; exact (Hdis x Hk (HNr x H))).
      now apply getv_norm_in.
  Qed.

  Lemma pair_cond : forall lr rr, map fst lr = KL -> map fst rr = KR ->
      cond_true c (normalize Nl lr ++ normalize Nr rr) = cond_true c (lr ++ rr).
  Proof.
    intros lr rr Kl Kr. destruct c as [e|]; [|reflexivity]. simpl. f_equal.
    apply eval_agree. intros x Hx. destruct (Hrefs x Hx) as [H|H].
    - rewrite (lookup_app_in x (normalize Nl lr) (normalize Nr rr)) by (rewrite normalize_keys; exact H).
      rewrite (lookup_app_in x lr rr) by (rewrite Kl; exact (HNl x H)).
      rewrite lookup_norm_in by exact H. symmetry. apply lookup_getv. rewrite Kl. exact (HNl x H).
    - assert (Hnl : ~ In x Nl) by (intros Hl; exact (Hdis x (HNl x Hl) (HNr x H))).
      rewrite (lookup_app_notin x (normalize Nl lr) (normalize Nr rr)) by (rewrite normalize_keys; exact Hnl).
      rewrite (lookup_app_notin x lr rr) by (rewrite Kl; intros Hk; exact (Hdis x Hk (HNr x H))).
      rewrite lookup_norm_in by exact H. symmetry. apply lookup_getv. rewrite Kr. exact (HNr x H).
  Qed.

  Lemma pair_norm : forall lr rr, map fst lr = KL -> map fst rr = KR ->
      normalize N (normalize Nl lr ++ normalize Nr rr) = normalize N (lr ++ rr).
  Proof.
    intros lr rr Kl Kr. apply normalize_ext. intros x Hx.
    destruct (in_dec Nat.eq_dec x KL) as [HL|HL].
    - apply pair_getv; auto.
    - destruct (in_dec Nat.eq_dec x KR) as [HR|HR].
      + apply pair_getv; auto.
      + rewrite (getv_app_notin x lr rr) by (rewrite Kl; exact HL).
        rewrite (getv_notin x rr) by (rewrite Kr; exact HR).
        rewrite (getv_app_notin x (normalize Nl lr) (normalize Nr rr))
          by (rewrite normalize_keys; intros H; exact (HL (HNl x H))).
        apply getv_norm_notin. intros H. exact (HR (HNr x H)).
  Qed.

  Lemma pad_norm : forall lr, map fst lr = KL ->
      normalize N (normalize Nl lr ++ pad Nr) = normalize N (lr ++ pad KR).
  Proof.
    intros lr Kl. apply normalize_ext. intros x Hx.
    destruct (in_dec Nat.eq_dec x KL) as [HL|HL].
    - rewrite (getv_app_in x (normalize Nl lr) (pad Nr)) by (rewrite normalize_keys; exact (HNL x Hx HL)).
      rewrite (getv_app_in x lr (pad KR)) by (rewrite Kl; exact HL).
      apply getv_norm_in. exact (HNL x Hx HL).
    - rewrite (getv_app_notin x lr (pad KR)) by (rewrite Kl; exact HL).
      rewrite (getv_app_notin x (normalize Nl lr) (pad Nr))
        by (rewrite normalize_keys; intros H; exact (HL (HNl x H))).
      now rewrite !getv_pad.
  Qed.

  Lemma join_norm : forall j ro L R,
      (forall lr, In lr L -> map fst lr = KL) -> (forall rr, In rr R -> map fst rr = KR) ->
      (j = LeftOuter -> ro = KR) ->
      map (normalize N) (join j c ro L R) =
      map (normalize N) (join j c Nr (map (normalize Nl) L) (map (normalize Nr) R)).
  Proof.
    intros j ro L R KLs KRs Hro.
    assert (Hm : forall lr, In lr L ->
              matches c (normalize Nl lr) (map (normalize Nr) R) =
              map (normalize Nr) (matches c lr R)).
    { intros lr Hlr. unfold matches. rewrite filter_map_comm. f_equal.
      apply filter_ext_in'. intros rr Hrr.
      exact (pair_cond lr rr (KLs lr Hlr) (KRs rr Hrr)). }
    assert (Hblock : forall lr, In lr L ->
              map (normalize N) (map (app lr) (matches c lr R)) =
              map (normalize N) (map (app (normalize Nl lr)) (map (normalize Nr) (matches c lr R)))).
    { intros lr Hlr. rewrite !map_map. apply map_ext_in. intros rr Hrr.
      unfold matches in Hrr. apply filter_In in Hrr as [Hrr _].
      symmetry. exact (pair_norm lr rr (KLs lr Hlr) (KRs rr Hrr)). }
    destruct j; unfold join; rewrite !flat_map_concat_map, !concat_map, !map_map;
      f_equal; apply map_ext_in; intros lr Hlr.
    - rewrite Hm by exact Hlr. exact (Hblock lr Hlr).
    - rewrite Hm by exact Hlr.
      destruct (matches c lr R) as [|m ms] eqn:E; simpl.
      + rewrite (Hro eq_refl). f_equal. symmetry. exact (pad_norm lr (KLs lr Hlr)).
      + specialize (Hblock lr Hlr). rewrite E in Hblock. simpl in Hblock. exact Hblock.
  Qed.
End JoinNorm.

(** * prn soundness *)

Lemma prn_sound : forall k N o n, prn k N o n = true ->
    forall db ops, map (normalize N) (eval db ops o) = map (normalize N) (eval db ops n).
Proof.
  induction k as [|k IH]; intros N o n H db ops; [discriminate|].
  simpl in H. repeat (apply orb_true_iff in H as [H|H]).
  - (* check3 *)
    apply andb_true_iff in H as [_ H]. now rewrite (check3_sound o n H db ops).
  - (* insert a Project into NEW *)
    destruct n as [| | b n0 | |]; try discriminate.
    apply andb_true_iff in H as [H Hp]. apply andb_true_iff in H as [Hnb Hb].
    rewrite (IH N o n0 Hp db ops). simpl. rewrite map_map. apply map_ext_in. intros t _.
    symmetry. now apply normalize_compose.
  - (* remove a Project from OLD *)
    destruct o as [| | a o0 | |]; try discriminate.
    apply andb_true_iff in H as [H Hp]. apply andb_true_iff in H as [Hna Ha].
    rewrite <- (IH N o0 n Hp db ops). simpl. rewrite map_map. apply map_ext_in. intros t _.
    now apply normalize_compose.
  - destruct o as [kk o1|c o0|a o0|kk o1 o0|j c l r]; destruct n as [kk' o1'|c' n0|a' n0|kk' o1' n0|j' c' l' r'];
      try discriminate.
    + (* Filter / Filter *)
      apply andb_true_iff in H as [H Hp]. apply andb_true_iff in H as [H Hrn].
      apply andb_true_iff in H as [Hce Hro].
      pose proof (IH _ _ _ Hp db ops) as E.
      set (M := N ++ refs c) in *.
      assert (HNM : subset N M = true).
      { unfold subset. apply forallb_forall. intros x Hx. apply memb_In. apply in_or_app. auto. }
      assert (Hfn : forall C X, (forall t, In t X -> C t = C (normalize M t)) ->
                map (normalize N) (filter C X) = map (normalize N) (filter C (map (normalize M) X))).
      { intros C X HC. rewrite filter_map_comm, map_map.
        rewrite (filter_ext_in' _ C (fun x => C (normalize M x)) X HC).
        apply map_ext_in. intros t _. symmetry. now apply normalize_compose. }
      assert (Hagree : forall t (cc : expr), subset (refs cc) M = true ->
                (forall x, In x (refs cc) -> In x (map fst t)) ->
                is_true (eval_e cc t) = is_true (eval_e cc (normalize M t))).
      { intros t cc HM Hk. f_equal. apply eval_agree. intros x Hx.
        rewrite lookup_norm_in by exact (subset_In _ _ _ HM Hx).
        apply lookup_getv. exact (Hk x Hx). }
      assert (HcM : subset (refs c) M = true).
      { unfold subset. apply forallb_forall. intros x Hx. apply memb_In. apply in_or_app. auto. }
      cbn [eval].
      rewrite (Hfn (fun t => is_true (eval_e c t)) (eval db ops o0)).
      2: { intros t Ht. apply Hagree; [exact HcM|]. intros x Hx.
           rewrite (eval_keys db ops o0 t Ht). exact (subset_In _ _ _ Hro Hx). }
      rewrite (Hfn (fun t => is_true (eval_e c' t)) (eval db ops n0)).
      2: { intros t Ht. rewrite !conj_true, <- (cond_equiv_sound _ _ t Hce),
                <- (cond_equiv_sound _ _ (normalize M t) Hce), <- !conj_true.
           apply Hagree; [exact HcM|]. intros x Hx.
           rewrite (eval_keys db ops n0 t Ht). exact (subset_In _ _ _ Hrn Hx). }
      rewrite E. f_equal. apply filter_ext_in'. intros t _.
      rewrite !conj_true. apply cond_equiv_sound. exact Hce.
    + (* Project / Project *)
      apply andb_true_iff in H as [Ha Hp].
      apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Ha. subst a'.
      pose proof (IH _ _ _ Hp db ops) as E. cbn [eval].
      rewrite !map_map. rewrite <- (map_map (normalize a) (normalize N) (eval db ops o0)).
      rewrite <- (map_map (normalize a) (normalize N) (eval db ops n0)). now rewrite E.
    + (* opaque / opaque *)
      apply andb_true_iff in H as [H Hp]. apply andb_true_iff in H as [H Hnd].
      apply andb_true_iff in H as [H Hout]. apply andb_true_iff in H as [Hk Ho1].
      apply Nat.eqb_eq in Hk. apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Ho1.
      apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hout. subst kk' o1'.
      pose proof (IH _ _ _ Hp db ops) as E.
      rewrite (map_normalize_self db ops o0 Hnd) in E.
      rewrite Hout in E. rewrite Hout in Hnd. rewrite (map_normalize_self db ops n0 Hnd) in E.
      cbn [eval]. now rewrite E.
    + (* Join / Join *)
      apply andb_true_iff in H as [H Hpr]. apply andb_true_iff in H as [H Hpl].
      apply andb_true_iff in H as [H Hsr]. apply andb_true_iff in H as [H Hsl].
      apply andb_true_iff in H as [H Hrc]. apply andb_true_iff in H as [H Hrr].
      apply andb_true_iff in H as [H Hll]. apply andb_true_iff in H as [H Hd'].
      apply andb_true_iff in H as [H Hd]. apply andb_true_iff in H as [Hj Hc].
      apply (eqb_dec_true _ jk_eq_dec) in Hj. apply (eqb_dec_true _ oexpr_eq_dec) in Hc.
      subst j' c'.
      set (Nl := restrict (N ++ refs_opt c) (out l)) in *.
      set (Nr := restrict (N ++ refs_opt c) (out r)) in *.
      assert (Hrefs : forall x, In x (refs_opt c) -> In x Nl \/ In x Nr).
      { intros x Hx. pose proof (subset_In _ _ _ Hrc Hx) as Hlr.
        apply in_app_or in Hlr as [Hl|Hr]; [left|right]; apply restrict_in; auto;
          apply in_or_app; auto. }
      assert (HNlS : forall x, In x Nl -> In x (out l)) by (intros x; apply restrict_sub).
      assert (HNrS : forall x, In x Nr -> In x (out r)) by (intros x; apply restrict_sub).
      assert (HNL : forall x, In x N -> In x (out l) -> In x Nl)
        by (intros x Hx Hk; apply restrict_in; [apply in_or_app; auto | exact Hk]).
      assert (HNR : forall x, In x N -> In x (out r) -> In x Nr)
        by (intros x Hx Hk; apply restrict_in; [apply in_or_app; auto | exact Hk]).
      cbn [eval].
      rewrite (join_norm N Nl Nr (out l) (out r) c (fun x Hx => disjoint_notin _ _ _ Hd Hx)
                 HNlS HNrS Hrefs HNL HNR j (out r) (eval db ops l) (eval db ops r)
                 (eval_keys db ops l) (eval_keys db ops r) (fun _ => eq_refl)).
      rewrite (join_norm N Nl Nr (out l') (out r') c (fun x Hx => disjoint_notin _ _ _ Hd' Hx)
                 (fun x Hx => subset_In _ _ _ Hsl Hx) (fun x Hx => subset_In _ _ _ Hsr Hx)
                 Hrefs
                 (fun x Hx Hk => HNL x Hx (subset_In _ _ _ Hll Hk))
                 (fun x Hx Hk => HNR x Hx (subset_In _ _ _ Hrr Hk))
                 j (out r') (eval db ops l') (eval db ops r')
                 (eval_keys db ops l') (eval_keys db ops r') (fun _ => eq_refl)).
      now rewrite (IH _ _ _ Hpl db ops), (IH _ _ _ Hpr db ops).
Qed.

Theorem check4_sound : forall o n, check4 o n = true ->
    forall db ops, eval db ops o = eval db ops n.
Proof.
  intros o n H db ops. unfold check4 in H. apply orb_true_iff in H as [H|H].
  - exact (check3_sound o n H db ops).
  - apply andb_true_iff in H as [H Hp]. apply andb_true_iff in H as [Hout Hnd].
    apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hout.
    pose proof (prn_sound _ _ _ _ Hp db ops) as E.
    rewrite (map_normalize_self db ops o Hnd) in E. rewrite Hout in E, Hnd.
    rewrite (map_normalize_self db ops n Hnd) in E. exact E.
Qed.

(** * Traces and the effect contract *)

Record event := mkEvent { ev_before : plan; ev_hb : list nat; ev_after : plan; ev_ha : list nat }.

Definition event_ok (ok_tag : nat -> bool) (e : event) : bool :=
  check4 (ev_before e) (ev_after e) && forallb ok_tag (ev_hb e) && forallb ok_tag (ev_ha e).

Fixpoint trace_ok (ok_tag : nat -> bool) (es : list event) : bool :=
  match es with
  | [] => true
  | e :: rest =>
      event_ok ok_tag e &&
      match rest with [] => true | e2 :: _ => plan_eqb (ev_after e) (ev_before e2) end &&
      trace_ok ok_tag rest
  end.

Lemma last_cons_default : forall (A : Type) (x y : A) (l : list A) d,
    last (x :: y :: l) d = last (y :: l) y.
Proof.
  intros A x y l d. simpl. revert y. induction l as [|z l IH]; intros y; [reflexivity|].
  simpl. destruct l; [reflexivity|]. apply IH.
Qed.

Lemma trace_eval : forall ok_tag es e, trace_ok ok_tag (e :: es) = true ->
    forall db ops, eval db ops (ev_before e) = eval db ops (ev_after (last (e :: es) e)).
Proof.
  intros ok_tag es. induction es as [|e2 es IH]; intros e H db ops; simpl in H.
  - rewrite !andb_true_r in H. unfold event_ok in H.
    apply andb_true_iff in H as [H _]. apply andb_true_iff in H as [H _].
    exact (check4_sound _ _ H db ops).
  - apply andb_true_iff in H as [H Hrest]. apply andb_true_iff in H as [He Hc].
    unfold event_ok in He. apply andb_true_iff in He as [He _]. apply andb_true_iff in He as [He _].
    apply (eqb_dec_true _ plan_eq_dec) in Hc.
    rewrite (check4_sound _ _ He db ops), Hc.
    rewrite last_cons_default. exact (IH e2 Hrest db ops).
Qed.

Lemma trace_tags_first : forall ok_tag e es, trace_ok ok_tag (e :: es) = true ->
    forallb ok_tag (ev_hb e) = true.
Proof.
  intros ok_tag e es H. simpl in H. apply andb_true_iff in H as [H _].
  apply andb_true_iff in H as [H _]. unfold event_ok in H.
  apply andb_true_iff in H as [H _]. apply andb_true_iff in H as [_ H]. exact H.
Qed.

Lemma trace_tags_last : forall ok_tag es e, trace_ok ok_tag (e :: es) = true ->
    forallb ok_tag (ev_ha (last (e :: es) e)) = true.
Proof.
  intros ok_tag es. induction es as [|e2 es IH]; intros e H; simpl in H.
  - rewrite !andb_true_r in H. unfold event_ok in H. simpl.
    apply andb_true_iff in H as [_ H]. exact H.
  - apply andb_true_iff in H as [_ Hrest].
    rewrite last_cons_default. exact (IH e2 Hrest).
Qed.

Section EffectContract.
  (** Any model of Spark's hidden optimizer state; only its tags are observed. *)
  Variable Hidden : Type.
  Variable tags_of : Hidden -> list nat.
  Variable ok_tag : nat -> bool.
  Variable den : Hidden -> plan -> (nat -> list nat -> list row) ->
                 (nat -> list nat -> list row -> list row) -> list row.
  Hypothesis effect_contract : forall h p db ops,
      forallb ok_tag (tags_of h) = true -> den h p db ops = eval db ops p.

  Theorem trace_sound : forall e es, trace_ok ok_tag (e :: es) = true ->
      forall hb ha db ops,
        tags_of hb = ev_hb e -> tags_of ha = ev_ha (last (e :: es) e) ->
        den hb (ev_before e) db ops = den ha (ev_after (last (e :: es) e)) db ops.
  Proof.
    intros e es H hb ha db ops Hb Ha.
    rewrite (effect_contract hb), (effect_contract ha).
    - exact (trace_eval ok_tag es e H db ops).
    - rewrite Ha. exact (trace_tags_last ok_tag es e H).
    - rewrite Hb. exact (trace_tags_first ok_tag e es H).
  Qed.
End EffectContract.

(** * Kernel-evaluated controls *)

Definition t_l := PLeaf 0 [0; 1; 2].
Definition t_r := PLeaf 1 [3; 4; 5].
Definition jc := Some (EEq (EAttr 0) (EAttr 3)).

Example accepts_column_pruning_over_join :
  check4 (PProject [0; 4] (PJoin Inner jc t_l t_r))
         (PProject [0; 4] (PJoin Inner jc (PProject [0] t_l) (PProject [3; 4] t_r))) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_pruning_a_join_key :
  check4 (PProject [0; 4] (PJoin Inner jc t_l t_r))
         (PProject [0; 4] (PJoin Inner jc (PProject [0] t_l) (PProject [4] t_r))) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_pruning_an_output_column :
  check4 (PProject [0; 4] (PJoin Inner jc t_l t_r))
         (PProject [0; 4] (PJoin Inner jc (PProject [0] t_l) (PProject [3] t_r))) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_pruning :
  check4 (PProject [0; 4] (PJoin LeftOuter jc t_l t_r))
         (PProject [0; 4] (PJoin LeftOuter jc (PProject [0] t_l) (PProject [3; 4] t_r))) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_collapse_project :
  check4 (PProject [0] (PProject [0; 1] t_l)) (PProject [0] t_l) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_remove_noop_project :
  check4 (PProject [0; 1; 2] t_l) t_l = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_pruning_under_opaque_operator :
  check4 (POp1 7 [0; 9] (PJoin Inner jc t_l t_r))
         (POp1 7 [0; 9] (PProject [0] (PJoin Inner jc (PProject [0] t_l) (PProject [3] t_r)))) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_pruning_below_filter :
  check4 (PProject [0] (PFilter (EGt (EAttr 1) (ELit (Some (VInt 0)))) t_l))
         (PProject [0] (PFilter (EGt (EAttr 1) (ELit (Some (VInt 0)))) (PProject [0; 1] t_l))) = true.
Proof. vm_compute. reflexivity. Qed.

Example trace_connectivity_is_checked :
  trace_ok (fun _ => true)
    [mkEvent (PProject [0; 1; 2] t_l) [] t_l [];
     mkEvent (PProject [0] t_l) [] (PProject [0] t_l) []] = false.
Proof. vm_compute. reflexivity. Qed.

Example trace_rejects_unknown_tag :
  trace_ok (fun x => Nat.eqb x 0) [mkEvent (PProject [0; 1; 2] t_l) [0] t_l [5]] = false.
Proof. vm_compute. reflexivity. Qed.

Example two_step_trace_accepted :
  trace_ok (fun _ => true)
    [mkEvent (PProject [0; 4] (PJoin Inner jc t_l t_r)) []
             (PProject [0; 4] (PJoin Inner jc (PProject [0] t_l) (PProject [3; 4] t_r))) [];
     mkEvent (PProject [0; 4] (PJoin Inner jc (PProject [0] t_l) (PProject [3; 4] t_r))) []
             (PProject [0; 4] (PJoin Inner jc (PFilter (EIsNotNull (EAttr 0)) (PProject [0] t_l))
                                             (PFilter (EIsNotNull (EAttr 3)) (PProject [3; 4] t_r)))) []] = true.
Proof. vm_compute. reflexivity. Qed.

Print Assumptions trace_sound.
Print Assumptions check4_sound.
