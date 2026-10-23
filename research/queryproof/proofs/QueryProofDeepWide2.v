(** V21 (copy of V19 QueryProofDeepWide.v, which stays frozen) adds:
    - TRANSITIVE implication: a new conjunct may be an old conjunct with y
      renamed to x (or x to y) where an old strict x = y holds (both values
      are then equal and non-NULL); covers IFFC constraint propagation through
      equi-join keys and inferred contradictions such as x < x;
    - EliminateOuterJoin: Filter(c, LeftOuter(l, r)) to Filter(c, Inner(l, r))
      when c null-rejects an attribute of r that l does not output.

    V19: wider kernel-decided certificates (self-contained; V15/V16 frozen).

    Grammar adds IsNull, IN (literal lists) and width-bounded integer Add
    (overflow = evaluation error, matching Spark's default ANSI mode).

    One unified INNER-join law replaces V15 guards and V16 pushdown:
    the conjuncts of (WHERE above, ON, existing input filters) may be
    redistributed into (new input filters, new ON). The new conjunct set must
    contain every old conjunct and may add IsNotNull(x) when x is null-rejected
    by some old conjunct. Every input-side conjunct reads only that input's
    attributes (right side: disjoint from the left output). This covers IFFC
    guards, WHERE pushdown, ON-clause pushdown and fused push+combine.
    For LEFT OUTER, the same law applies to the null-supplying (right) side of
    the ON condition only; WHERE conjuncts on the preserved side follow V16.

    Trusted, NOT proved: the printer; agreement of Spark's evaluator with
    [eval_e] (errors are treated as "not TRUE", whereas Spark fails the query
    on an ANSI overflow); opaque-descriptor identity; JSON fidelity
    (TreeNode JSON erases Add's eval mode); hidden optimizer state.
*)
From Coq Require Import Lists.List ZArith.ZArith Bool.Bool Arith.PeanoNat Lia.
Import ListNotations.

(** * Values, rows, expressions *)

Inductive val := VInt (z : Z) | VBool (b : bool).
Definition cell := option val.
Definition row := list (nat * cell).

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
| EIsNull (a : expr)
| EIn (a : expr) (vs : list cell)
| EAdd (w : Z) (a b : expr)
| EAnd (a b : expr)
| EOr (a b : expr).

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

Definition in_range (w z : Z) : bool :=
  Z.leb (- 2 ^ (w - 1)) z && Z.ltb z (2 ^ (w - 1)).

Definition add3 (w : Z) (a b : option cell) : option cell :=
  match a, b with
  | Some (Some (VInt x)), Some (Some (VInt y)) =>
      if in_range w (x + y) then Some (Some (VInt (x + y))) else None
  | Some None, Some None
  | Some None, Some (Some (VInt _))
  | Some (Some (VInt _)), Some None => Some None
  | _, _ => None
  end.

(** SQL IN over literals: NULL probe -> NULL; a match -> TRUE;
    otherwise NULL if the list holds a NULL, else FALSE. *)
Definition in3 (a : option cell) (vs : list cell) : option cell :=
  match a with
  | Some None => Some None
  | Some (Some (VInt x)) =>
      if existsb (fun v => match v with Some (VInt y) => Z.eqb x y | _ => false end) vs
      then Some (Some (VBool true))
      else if existsb (fun v => match v with None => true | _ => false end) vs
           then Some None else Some (Some (VBool false))
  | _ => None
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
  | EIsNull a =>
      match eval_e a r with
      | Some None => Some (Some (VBool true))
      | Some (Some _) => Some (Some (VBool false))
      | None => None
      end
  | EIn a vs => in3 (eval_e a r) vs
  | EAdd w a b => add3 w (eval_e a r) (eval_e b r)
  | EAnd a b => and3 (eval_e a r) (eval_e b r)
  | EOr a b => or3 (eval_e a r) (eval_e b r)
  end.

Definition is_true (c : option cell) : bool :=
  match c with Some (Some (VBool true)) => true | _ => false end.

(** * Plans and semantics *)

Inductive jkind := Inner | LeftOuter.

Inductive plan :=
| PLeaf (k : nat) (o : list nat)
| PFilter (c : expr) (p : plan)
| PProject (ids : list nat) (p : plan)
| POp1 (k : nat) (o : list nat) (p : plan)
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

(** * Decidable equality and syntax helpers *)

Definition val_eq_dec : forall a b : val, {a = b} + {a <> b}.
Proof. decide equality; [apply Z.eq_dec | apply bool_dec]. Defined.
Definition cell_eq_dec : forall a b : cell, {a = b} + {a <> b}.
Proof. decide equality. apply val_eq_dec. Defined.
Definition expr_eq_dec : forall a b : expr, {a = b} + {a <> b}.
Proof.
  decide equality;
    first [ apply Nat.eq_dec | apply cell_eq_dec | apply Z.eq_dec
          | apply (list_eq_dec cell_eq_dec) ].
Defined.
Definition oexpr_eq_dec : forall a b : option expr, {a = b} + {a <> b}.
Proof. decide equality; apply expr_eq_dec. Defined.
Definition jk_eq_dec : forall a b : jkind, {a = b} + {a <> b}.
Proof. decide equality. Defined.
Definition plan_eq_dec : forall a b : plan, {a = b} + {a <> b}.
Proof.
  decide equality;
    first [ apply Nat.eq_dec | apply (list_eq_dec Nat.eq_dec)
          | apply expr_eq_dec | apply oexpr_eq_dec | apply jk_eq_dec ].
Defined.

Definition plan_eqb a b : bool := if plan_eq_dec a b then true else false.
Definition expr_eqb a b : bool := if expr_eq_dec a b then true else false.
Definition oexpr_eqb a b : bool := if oexpr_eq_dec a b then true else false.
Definition ids_eqb (a b : list nat) : bool :=
  if list_eq_dec Nat.eq_dec a b then true else false.
Definition jk_eqb (a b : jkind) : bool := if jk_eq_dec a b then true else false.
Definition memb (x : nat) (xs : list nat) : bool := existsb (Nat.eqb x) xs.

Fixpoint refs (e : expr) : list nat :=
  match e with
  | EAttr x => [x]
  | ELit _ => []
  | ELt a b | ELe a b | EGt a b | EGe a b | EEq a b
  | ENullSafeEq a b | EAnd a b | EOr a b | EAdd _ a b => refs a ++ refs b
  | EIsNotNull a | EIsNull a | EIn a _ => refs a
  end.

(** Attributes that must be non-NULL whenever the VALUE of [e] is non-NULL. *)
Fixpoint nr_val (e : expr) : list nat :=
  match e with
  | EAttr x => [x]
  | EAdd _ a b => nr_val a ++ nr_val b
  | _ => []
  end.

(** Attributes that must be non-NULL whenever [e] is TRUE. *)
Fixpoint nr (e : expr) : list nat :=
  match e with
  | EAnd a b => nr a ++ nr b
  | ELt a b | ELe a b | EGt a b | EGe a b | EEq a b => nr_val a ++ nr_val b
  | EIsNotNull a | EIn a _ => nr_val a
  | _ => []
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

(** Replace attribute [y] by [x]. *)
Fixpoint rename (y x : nat) (e : expr) : expr :=
  match e with
  | EAttr z => if Nat.eqb z y then EAttr x else EAttr z
  | ELit v => ELit v
  | ELt a b => ELt (rename y x a) (rename y x b)
  | ELe a b => ELe (rename y x a) (rename y x b)
  | EGt a b => EGt (rename y x a) (rename y x b)
  | EGe a b => EGe (rename y x a) (rename y x b)
  | EEq a b => EEq (rename y x a) (rename y x b)
  | ENullSafeEq a b => ENullSafeEq (rename y x a) (rename y x b)
  | EIsNotNull a => EIsNotNull (rename y x a)
  | EIsNull a => EIsNull (rename y x a)
  | EIn a vs => EIn (rename y x a) vs
  | EAdd w a b => EAdd w (rename y x a) (rename y x b)
  | EAnd a b => EAnd (rename y x a) (rename y x b)
  | EOr a b => EOr (rename y x a) (rename y x b)
  end.

(** Strict attribute equalities among the old conjuncts. *)
Definition eq_pairs (olds : list expr) : list (nat * nat) :=
  flat_map (fun o => match o with EEq (EAttr x) (EAttr y) => [(x, y)] | _ => [] end) olds.

(** [olds] and [news] select the same rows: every old conjunct is new, and each
    new conjunct is old, or IsNotNull(x) with x null-rejected by an old one, or an
    old conjunct transported along an old strict equality x = y (V21). *)
Definition implied (olds : list expr) (d : expr) : bool :=
  mem_expr d olds ||
  match d with
  | EIsNotNull (EAttr x) => existsb (fun o => memb x (nr o)) olds
  | _ => false
  end ||
  existsb (fun o => existsb (fun xy =>
             expr_eqb d (rename (snd xy) (fst xy) o) || expr_eqb d (rename (fst xy) (snd xy) o))
           (eq_pairs olds)) olds.
Definition cond_equiv (olds news : list expr) : bool :=
  forallb (fun d => mem_expr d news) olds && forallb (implied olds) news.

Definition wrap (sp : option expr) (s : plan) : plan :=
  match sp with Some e => PFilter e s | None => s end.

(** Side alternatives: the old input may carry a filter [o0] above its base, and
    the new input may carry a filter [n0] above its base; the bases must check. *)
Definition side2 (cs : plan -> bool) (cq : plan -> bool) (s s' : plan)
    (P : option expr -> option expr -> bool) : bool :=
  (cs s' && P None None) ||
  match s' with PFilter e q' => cs q' && P None (Some e) | _ => false end ||
  match s with
  | PFilter e0 _ =>
      (cq s' && P (Some e0) None) ||
      match s' with PFilter e q' => cq q' && P (Some e0) (Some e) | _ => false end
  | _ => false
  end.

Definition inner_ok (c : option expr) (jc jc' : option expr) (lo ro : list nat)
    (lo0 lp ro0 rp : option expr) : bool :=
  cond_equiv (conjs_opt c ++ conjs_opt jc ++ conjs_opt lo0 ++ conjs_opt ro0)
             (conjs_opt lp ++ conjs_opt rp ++ conjs_opt jc') &&
  subset (refs_opt lo0 ++ refs_opt lp) lo &&
  subset (refs_opt ro0 ++ refs_opt rp) ro &&
  disjoint (refs_opt ro0 ++ refs_opt rp) lo.

Definition outer_ok (jc jc' : option expr) (lo ro : list nat) (ro0 rp : option expr) : bool :=
  cond_equiv (conjs_opt jc ++ conjs_opt ro0) (conjs_opt rp ++ conjs_opt jc') &&
  subset (refs_opt ro0 ++ refs_opt rp) ro &&
  disjoint (refs_opt ro0 ++ refs_opt rp) lo.

Definition lo_ok (c : expr) (rest lp : option expr) (lo : list nat) : bool :=
  cond_equiv (conjs c) (conjs_opt lp ++ conjs_opt rest) && subset (refs_opt lp) lo.

Definition sub_check (chk : plan -> plan -> bool) (s : plan) : plan -> bool :=
  match s with PFilter _ q => chk q | _ => fun _ => false end.

(** Conjunction of a list; the conjuncts of [e] that are not conjuncts of [e0]. *)
Fixpoint conj_of (l : list expr) : option expr :=
  match l with
  | [] => None
  | d :: rest => match conj_of rest with Some r => Some (EAnd d r) | None => Some d end
  end.
Definition rest_of (e e0 : expr) : list expr :=
  filter (fun d => negb (mem_expr d (conjs e0))) (conjs e).

Definition pchk1 (chk : plan -> plan -> bool) (s : plan) : plan -> bool :=
  match s with PProject _ q => chk q | _ => fun _ => false end.
Definition pchk2 (chk : plan -> plan -> bool) (s : plan) : plan -> bool :=
  match s with PProject _ (PFilter _ q0) => chk q0 | _ => fun _ => false end.

(** [side2], or the same filter alternatives underneath an unchanged
    attribute-only Project whose columns cover the moved conjuncts. *)
Definition side3 (cs cq cp1 cp2 : plan -> bool) (s s' : plan)
    (P : option expr -> option expr -> bool) : bool :=
  side2 cs cq s s' P ||
  match s, s' with
  | PProject ids q, PProject ids' q' =>
      ids_eqb ids ids' &&
      (match q' with
       | PFilter e q2 => cp1 q2 && subset (refs e) ids && subset ids (out q) && P None (Some e)
       | _ => false
       end ||
       match q with
       | PFilter e0 q0 =>
           (subset (refs e0) ids && subset ids (out q0) && cp2 q' && P (Some e0) None) ||
           match q' with
           | PFilter e q2 =>
               (cp2 q2 && forallb (fun d => mem_expr d (conjs e)) (conjs e0) &&
                subset (refs_opt (conj_of (rest_of e e0))) ids && subset ids (out q0) &&
                P None (conj_of (rest_of e e0))) ||
               (* V21: both filters only read projected columns, so the old filter
                  joins the pool of old conjuncts (enables transport from it) *)
               (cp2 q2 && subset (refs e0) ids && subset (refs e) ids && subset ids (out q0) &&
                P (Some e0) (Some e))
           | _ => false
           end
       | _ => false
       end)
  | _, _ => false
  end.

(** * The checker *)

Fixpoint check3 (o n : plan) {struct o} : bool :=
  plan_eqb o n ||
  match o, n with
  | PFilter c p, PFilter c' p' =>
      (cond_equiv (conjs c) (conjs c') && check3 p p') ||
      match p with
      | PFilter c2 q => cond_equiv (conjs c2 ++ conjs c) (conjs c') && check3 q p'
      | _ => false
      end ||
      match p, p' with
      | PJoin LeftOuter jc l r, PJoin LeftOuter jc' l' r' =>
          oexpr_eqb jc jc' && check3 r r' &&
          (check3 l l' && lo_ok c (Some c') None (out l) ||
           match l' with
           | PFilter e l0 => check3 l l0 && lo_ok c (Some c') (Some e) (out l)
           | _ => false
           end)
      | _, _ => false
      end ||
      match p, p' with                                  (* EliminateOuterJoin (V21) *)
      | PJoin LeftOuter jc l r, PJoin Inner jc' l' r' =>
          cond_equiv (conjs c) (conjs c') && oexpr_eqb jc jc' && check3 l l' && check3 r r' &&
          existsb (fun x => memb x (out r) && negb (memb x (out l))) (nr c)
      | _, _ => false
      end
  | PFilter c p, PProject ids' p' =>
      match p, p' with
      | PProject ids q, PFilter c' q' =>
          ids_eqb ids ids' && subset (refs c) ids && subset ids (out q) &&
          ((cond_equiv (conjs c) (conjs c') && check3 q q') ||
           match q with
           | PFilter c2 q0 => cond_equiv (conjs c2 ++ conjs c) (conjs c') && check3 q0 q'
           | _ => false
           end)
      | _, _ => false
      end
  | PFilter c p, PJoin Inner jc' l' r' =>
      match p with
      | PJoin Inner jc l r =>
          side3 (check3 l) (sub_check check3 l) (pchk1 check3 l) (pchk2 check3 l) l l' (fun lo0 lp =>
            side3 (check3 r) (sub_check check3 r) (pchk1 check3 r) (pchk2 check3 r) r r' (fun ro0 rp =>
              inner_ok (Some c) jc jc' (out l) (out r) lo0 lp ro0 rp))
      | _ => false
      end
  | PFilter c p, PJoin LeftOuter jc' l' r' =>
      match p with
      | PJoin LeftOuter jc l r =>
          oexpr_eqb jc jc' && check3 r r' &&
          (check3 l l' && lo_ok c None None (out l) ||
           match l' with
           | PFilter e l0 => check3 l l0 && lo_ok c None (Some e) (out l)
           | _ => false
           end)
      | _ => false
      end
  | PProject ids p, PProject ids' p' => ids_eqb ids ids' && check3 p p'
  | POp1 k o1 p, POp1 k' o1' p' => Nat.eqb k k' && ids_eqb o1 o1' && check3 p p'
  | PJoin Inner jc l r, PJoin Inner jc' l' r' =>
      side3 (check3 l) (sub_check check3 l) (pchk1 check3 l) (pchk2 check3 l) l l' (fun lo0 lp =>
        side3 (check3 r) (sub_check check3 r) (pchk1 check3 r) (pchk2 check3 r) r r' (fun ro0 rp =>
          inner_ok None jc jc' (out l) (out r) lo0 lp ro0 rp))
  | PJoin LeftOuter jc l r, PJoin LeftOuter jc' l' r' =>
      check3 l l' &&
      side3 (check3 r) (sub_check check3 r) (pchk1 check3 r) (pchk2 check3 r) r r' (fun ro0 rp =>
        outer_ok jc jc' (out l) (out r) ro0 rp)
  | _, _ => false
  end.

(** * Basic lemmas *)

Lemma memb_In : forall x xs, memb x xs = true <-> In x xs.
Proof.
  intros x xs. unfold memb. rewrite existsb_exists. split.
  - intros [y [Hy Heq]]. apply Nat.eqb_eq in Heq. subst. exact Hy.
  - intros H. exists x. split; [exact H | apply Nat.eqb_refl].
Qed.

Lemma mem_expr_In : forall d l, mem_expr d l = true <-> In d l.
Proof.
  intros d l. unfold mem_expr. rewrite existsb_exists. split.
  - intros [y [Hy Heq]]. unfold expr_eqb in Heq.
    destruct (expr_eq_dec d y); [subst; exact Hy | discriminate].
  - intros H. exists d. split; [exact H|]. unfold expr_eqb.
    destruct (expr_eq_dec d d); [reflexivity | contradiction].
Qed.

Lemma eqb_dec_true : forall (A : Type) (dec : forall a b : A, {a = b} + {a <> b}) a b,
    (if dec a b then true else false) = true -> a = b.
Proof. intros A dec a b. destruct (dec a b); [auto | discriminate]. Qed.

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

Lemma lookup_in_keys : forall x t, In x (map fst t) -> exists v, lookup x t = Some v.
Proof.
  intros x t. induction t as [|[y v] t IH]; simpl; [tauto|].
  intros [<-|H]; [rewrite Nat.eqb_refl; eauto|].
  destruct (Nat.eqb x y); eauto.
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

Lemma filter_all : forall (A : Type) (f : A -> bool) xs,
    (forall x, f x = true) -> filter f xs = xs.
Proof.
  intros A f xs H. induction xs as [|x xs IH]; simpl; [reflexivity|].
  rewrite H. now rewrite IH.
Qed.

Lemma filter_none : forall (A : Type) (f : A -> bool) xs,
    (forall x, In x xs -> f x = false) -> filter f xs = [].
Proof.
  intros A f xs H. induction xs as [|x xs IH]; simpl; [reflexivity|].
  rewrite (H x (or_introl eq_refl)). apply IH. intros y Hy. apply H. right. exact Hy.
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

(** * Expression semantics lemmas *)

Lemma and3_is_true : forall a b, is_true (and3 a b) = is_true a && is_true b.
Proof. intros [[[x|[|]]|]|] [[[y|[|]]|]|]; reflexivity. Qed.

Lemma conj_true : forall e t,
    is_true (eval_e e t) = forallb (fun d => is_true (eval_e d t)) (conjs e).
Proof.
  induction e; intros t; simpl; try (rewrite andb_true_r; reflexivity).
  rewrite and3_is_true, forallb_app, IHe1, IHe2. reflexivity.
Qed.

Lemma is_true_eq : forall a, is_true a = true -> a = Some (Some (VBool true)).
Proof. intros [[[z|[|]]|]|]; simpl; intros H; try discriminate; reflexivity. Qed.

Lemma nr_val_sound : forall e x t v, In x (nr_val e) -> eval_e e t = Some (Some v) ->
    exists u, lookup x t = Some (Some u).
Proof.
  induction e; intros y t vv Hin Hev; simpl in Hin; try contradiction.
  - destruct Hin as [<-|[]]. simpl in Hev. eauto.
  - simpl in Hev. unfold add3 in Hev.
    destruct (eval_e e1 t) as [[[x1|b1]|]|] eqn:E1; try discriminate;
    destruct (eval_e e2 t) as [[[x2|b2]|]|] eqn:E2; try discriminate;
    try (destruct (in_range w (x1 + x2)); discriminate).
    apply in_app_or in Hin as [H|H]; eauto.
Qed.

Lemma cmp_true_inv : forall f a b, is_true (cmp f a b) = true ->
    exists x y, a = Some (Some (VInt x)) /\ b = Some (Some (VInt y)).
Proof.
  intros f [[[x|bx]|]|] [[[y|by']|]|]; simpl; intros H; try discriminate; eauto.
Qed.

Lemma cmp_nr : forall f a b x t,
    In x (nr_val a ++ nr_val b) ->
    is_true (cmp f (eval_e a t) (eval_e b t)) = true ->
    exists v, lookup x t = Some (Some v).
Proof.
  intros f a b x t Hin Ht.
  apply cmp_true_inv in Ht as [p [q [Ha Hb]]].
  apply in_app_or in Hin as [H|H]; eauto using nr_val_sound.
Qed.

Lemma nr_sound : forall e y t, In y (nr e) -> is_true (eval_e e t) = true ->
    exists v, lookup y t = Some (Some v).
Proof.
  induction e; intros y t Hin Ht; simpl in Hin; try contradiction;
    simpl in Ht; try (eapply cmp_nr; eassumption).
  - (* IsNotNull *)
    destruct (eval_e e t) as [[v|]|] eqn:E; try discriminate. eauto using nr_val_sound.
  - (* In *)
    unfold in3 in Ht.
    destruct (eval_e e t) as [[[x|b]|]|] eqn:E; try discriminate. eauto using nr_val_sound.
  - (* And *)
    rewrite and3_is_true in Ht. apply andb_true_iff in Ht as [H1 H2].
    apply in_app_or in Hin as [H|H]; eauto.
Qed.

Lemma eval_agree : forall e t1 t2,
    (forall x, In x (refs e) -> lookup x t1 = lookup x t2) ->
    eval_e e t1 = eval_e e t2.
Proof.
  induction e; intros t1 t2 H; simpl in *;
    try (rewrite (IHe1 t1 t2), (IHe2 t1 t2);
         [reflexivity | intros; apply H; apply in_or_app; auto ..]);
    try (rewrite (IHe t1 t2 H); reflexivity);
    try reflexivity.
  apply H. left. reflexivity.
Qed.

Lemma forallb_incl : forall (P : expr -> bool) a b,
    forallb (fun d => mem_expr d b) a = true ->
    forallb P b = true -> forallb P a = true.
Proof.
  intros P a b Hab Hb. apply forallb_forall. intros d Hd.
  rewrite forallb_forall in Hab, Hb. apply Hb. apply mem_expr_In. exact (Hab d Hd).
Qed.

Lemma rename_eval : forall ry rx e t, lookup rx t = lookup ry t ->
    eval_e (rename ry rx e) t = eval_e e t.
Proof.
  intros ry rx e t H. induction e; simpl;
    try (rewrite IHe1, IHe2; reflexivity); try (rewrite IHe; reflexivity); try reflexivity.
  destruct (Nat.eqb x ry) eqn:E; simpl; [|reflexivity].
  apply Nat.eqb_eq in E. subst. exact H.
Qed.

Lemma eq_pairs_sound : forall olds x y, In (x, y) (eq_pairs olds) ->
    In (EEq (EAttr x) (EAttr y)) olds.
Proof.
  intros olds x y H. unfold eq_pairs in H. apply in_flat_map in H as [o [Ho H]].
  destruct o; simpl in H; try contradiction.
  destruct o1; simpl in H; try contradiction.
  destruct o2; simpl in H; try contradiction.
  destruct H as [H|[]]. injection H as -> ->. exact Ho.
Qed.

Lemma cond_equiv_sound : forall olds news t, cond_equiv olds news = true ->
    forallb (fun d => is_true (eval_e d t)) olds =
    forallb (fun d => is_true (eval_e d t)) news.
Proof.
  intros olds news t H. unfold cond_equiv in H. apply andb_true_iff in H as [Hin Himp].
  destruct (forallb (fun d => is_true (eval_e d t)) olds) eqn:Eo,
           (forallb (fun d => is_true (eval_e d t)) news) eqn:En; try reflexivity.
  - exfalso.
    assert (Hall : forallb (fun d => is_true (eval_e d t)) news = true).
    { apply forallb_forall. intros d Hd. rewrite forallb_forall in Himp.
      pose proof (Himp d Hd) as Hd'. unfold implied in Hd'.
      apply orb_true_iff in Hd' as [Hd'|Hren]; [apply orb_true_iff in Hd' as [Hm|Hnn]|].
      - rewrite forallb_forall in Eo. apply Eo. apply mem_expr_In. exact Hm.
      - destruct d as [| | | | | | | | a | | | | |]; try discriminate.
        destruct a as [x| | | | | | | | | | | | |]; try discriminate.
        apply existsb_exists in Hnn as [o [Ho Hx]]. apply memb_In in Hx.
        rewrite forallb_forall in Eo.
        destruct (nr_sound o x t Hx (Eo o Ho)) as [v Hv]. simpl. rewrite Hv. reflexivity.
      - (* transported along an old strict equality *)
        apply existsb_exists in Hren as [o [Ho Hp]].
        apply existsb_exists in Hp as [[x y] [Hxy Hdo]]. simpl in Hdo.
        rewrite forallb_forall in Eo.
        assert (Heq : lookup x t = lookup y t).
        { pose proof (Eo _ (eq_pairs_sound olds x y Hxy)) as Ht. simpl in Ht.
          destruct (cmp_true_inv _ _ _ Ht) as [a [b [Ha Hb]]].
          rewrite Ha, Hb in Ht. simpl in Ht.
          destruct (Z.eqb a b) eqn:Eab; [|discriminate]. apply Z.eqb_eq in Eab. subst b.
          now rewrite Ha, Hb. }
        apply orb_true_iff in Hdo as [Hdo|Hdo]; apply (eqb_dec_true _ expr_eq_dec) in Hdo;
          subst d.
        + rewrite (rename_eval y x o t Heq). exact (Eo o Ho).
        + rewrite (rename_eval x y o t (eq_sym Heq)). exact (Eo o Ho). }
    congruence.
  - exfalso. rewrite (forallb_incl _ olds news Hin En) in Eo. discriminate.
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

Definition sound_rel (a b : plan) : Prop :=
  out a = out b /\ forall db ops, eval db ops a = eval db ops b.

(** * Join algebra *)

(** An inner join over filtered inputs, followed by a filter, as one flat_map. *)
Lemma inner_join_flat : forall jc (C L R : row -> bool) ro ls rs,
    filter C (join Inner jc ro (filter L ls) (filter R rs)) =
    flat_map (fun lr => map (app lr)
      (filter (fun rr => L lr && R rr && cond_true jc (lr ++ rr) && C (lr ++ rr)) rs)) ls.
Proof.
  intros jc C L R ro ls rs. unfold row in *.
  induction ls as [|lr ls IH]; [reflexivity|].
  unfold join in *. cbn [flat_map filter]. destruct (L lr) eqn:EL; cbn [flat_map].
  - rewrite filter_app. f_equal; [|exact IH].
    unfold matches. rewrite filter_map_comm, filter_filter_and, filter_filter_and.
    f_equal. apply filter_ext_in'. intros rr _. simpl.
    destruct (R rr); destruct (cond_true jc (lr ++ rr)); destruct (C (lr ++ rr)); reflexivity.
  - simpl. rewrite (filter_none _ (fun _ : list (nat * cell) => false) rs) by reflexivity.
    exact IH.
Qed.

Lemma flat_map_ext_in' : forall (A B : Type) (f g : A -> list B) l,
    (forall x, In x l -> f x = g x) -> flat_map f l = flat_map g l.
Proof.
  intros A B f g l H. induction l as [|x l IH]; simpl; [reflexivity|].
  rewrite (H x (or_introl eq_refl)). f_equal. apply IH. intros y Hy. apply H. right. exact Hy.
Qed.

Lemma inner_join_shift : forall jc jc' (C L1 R1 L2 R2 : row -> bool) ro ro' ls rs,
    (forall lr rr, In lr ls -> In rr rs ->
       L1 lr && R1 rr && cond_true jc (lr ++ rr) && C (lr ++ rr) =
       L2 lr && R2 rr && cond_true jc' (lr ++ rr)) ->
    filter C (join Inner jc ro (filter L1 ls) (filter R1 rs)) =
    join Inner jc' ro' (filter L2 ls) (filter R2 rs).
Proof.
  intros jc jc' C L1 R1 L2 R2 ro ro' ls rs H.
  rewrite inner_join_flat.
  rewrite <- (filter_all _ (fun _ => true) (join Inner jc' ro' (filter L2 ls) (filter R2 rs)))
    by reflexivity.
  rewrite inner_join_flat. apply flat_map_ext_in'. intros lr Hlr. f_equal.
  apply filter_ext_in'. intros rr Hrr. rewrite (H lr rr Hlr Hrr). rewrite andb_true_r.
  reflexivity.
Qed.

Lemma left_outer_right_shift : forall jc jc' (R1 R2 : row -> bool) ro ls rs,
    (forall lr rr, In lr ls -> In rr rs ->
       R1 rr && cond_true jc (lr ++ rr) = R2 rr && cond_true jc' (lr ++ rr)) ->
    join LeftOuter jc ro ls (filter R1 rs) = join LeftOuter jc' ro ls (filter R2 rs).
Proof.
  intros jc jc' R1 R2 ro ls rs H. unfold row in *.
  induction ls as [|lr ls IH]; [reflexivity|].
  assert (Hm : matches jc lr (filter R1 rs) = matches jc' lr (filter R2 rs)).
  { unfold matches. rewrite !filter_filter_and. apply filter_ext_in'.
    intros rr Hrr. exact (H lr rr (or_introl eq_refl) Hrr). }
  assert (IH' := IH (fun a b Ha => H a b (or_intror Ha))).
  unfold join in *. cbn [flat_map]. rewrite Hm, IH'. reflexivity.
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

Lemma lookup_pad_in : forall x K, In x K -> lookup x (pad K) = Some None.
Proof.
  intros x K. unfold pad. induction K as [|k K IH]; simpl; [tauto|].
  destruct (Nat.eqb x k) eqn:E; [reflexivity|].
  intros [<-|H]; [rewrite Nat.eqb_refl in E; discriminate | auto].
Qed.

(** EliminateOuterJoin: padded rows carry NULL in every right attribute, so a
    filter that null-rejects one of them discards exactly those rows. *)
Lemma eoj_shift : forall jc (C : row -> bool) KL KR x ls rs,
    (forall lr, In lr ls -> map fst lr = KL) -> In x KR -> ~ In x KL ->
    (forall t, C t = true -> exists v, lookup x t = Some (Some v)) ->
    filter C (join LeftOuter jc KR ls rs) = filter C (join Inner jc KR ls rs).
Proof.
  intros jc C KL KR x ls rs Hk Hx Hnx HC. unfold row in *.
  induction ls as [|lr ls IH]; [reflexivity|].
  assert (IH' := IH (fun a Ha => Hk a (or_intror Ha))).
  unfold join in *. cbn [flat_map]. rewrite !filter_app, IH'. f_equal.
  destruct (matches jc lr rs) as [|m ms] eqn:E; [|reflexivity].
  simpl. destruct (C (lr ++ pad KR)) eqn:Ec; [|reflexivity].
  destruct (HC _ Ec) as [v Hv].
  rewrite lookup_app_notin in Hv by (rewrite (Hk lr (or_introl eq_refl)); exact Hnx).
  rewrite lookup_pad_in in Hv by exact Hx. discriminate.
Qed.

(** * Side decomposition *)

Lemma side2_inv : forall (chk : plan -> plan -> bool) s s' P,
    side2 (chk s) (sub_check chk s) s s' P = true ->
    exists o0 base n0 base',
      s = wrap o0 base /\ s' = wrap n0 base' /\ chk base base' = true /\ P o0 n0 = true.
Proof.
  intros chk s s' P H. unfold side2 in H.
  apply orb_true_iff in H as [H|H]; [apply orb_true_iff in H as [H|H]|].
  - apply andb_true_iff in H as [Hc HP]. exists None, s, None, s'. auto.
  - destruct s' as [| e q' | | |]; try discriminate.
    apply andb_true_iff in H as [Hc HP]. exists None, s, (Some e), q'. auto.
  - destruct s as [| e0 q | | |]; try discriminate.
    apply orb_true_iff in H as [H|H].
    + apply andb_true_iff in H as [Hc HP]. exists (Some e0), q, None, s'. auto.
    + destruct s' as [| e q' | | |]; try discriminate.
      apply andb_true_iff in H as [Hc HP]. exists (Some e0), q, (Some e), q'. auto.
Qed.

Lemma conj_of_pred : forall l t,
    opt_pred (conj_of l) t = forallb (fun d => is_true (eval_e d t)) l.
Proof.
  induction l as [|d l IH]; intros t; [reflexivity|]. simpl.
  destruct (conj_of l) as [r|] eqn:E; simpl in *.
  - rewrite and3_is_true. f_equal. exact (IH t).
  - rewrite <- (IH t). rewrite andb_true_r. reflexivity.
Qed.

Lemma split_filter : forall e e0 t,
    forallb (fun d => mem_expr d (conjs e)) (conjs e0) = true ->
    is_true (eval_e e t) = is_true (eval_e e0 t) && opt_pred (conj_of (rest_of e e0)) t.
Proof.
  intros e e0 t Hin. rewrite conj_of_pred, !conj_true. unfold rest_of.
  set (P := fun d => is_true (eval_e d t)).
  destruct (forallb P (conjs e)) eqn:Ea.
  - rewrite (forallb_incl P _ _ Hin Ea). simpl. symmetry. apply forallb_forall.
    intros d Hd. apply filter_In in Hd as [Hd _]. rewrite forallb_forall in Ea. exact (Ea d Hd).
  - symmetry. apply Bool.not_true_iff_false. intros H.
    apply andb_true_iff in H as [H0 HR]. rewrite <- Bool.not_true_iff_false in Ea. apply Ea.
    apply forallb_forall. intros d Hd. rewrite forallb_forall in H0, HR.
    destruct (mem_expr d (conjs e0)) eqn:Em.
    + apply H0. apply mem_expr_In. exact Em.
    + apply HR. apply filter_In. rewrite Em. auto.
Qed.

Lemma proj_filter : forall db ops ids e q,
    subset (refs e) ids = true -> subset ids (out q) = true ->
    eval db ops (PProject ids (PFilter e q)) = filter (opt_pred (Some e)) (eval db ops (PProject ids q)).
Proof.
  intros db ops ids e q Hr Hi. simpl. rewrite filter_map_comm. f_equal. apply filter_ext_in'.
  intros t Ht. f_equal. symmetry. apply eval_agree. intros x Hx.
  pose proof (subset_In _ _ _ Hr Hx) as Hxi. apply lookup_normalize; [exact Hxi|].
  rewrite (eval_keys db ops q t Ht). exact (subset_In _ _ _ Hi Hxi).
Qed.

Fixpoint size (p : plan) : nat :=
  match p with
  | PLeaf _ _ => 1
  | PFilter _ q | PProject _ q | POp1 _ _ q => S (size q)
  | PJoin _ _ l r => S (size l + size r)
  end.

Lemma size_wrap : forall o0 b, size b <= size (wrap o0 b).
Proof. intros [e|] b; simpl; lia. Qed.

Lemma inner_bool : forall c jc jc' lo ro lo0 lp ro0 rp lr rr,
    inner_ok c jc jc' lo ro lo0 lp ro0 rp = true ->
    map fst lr = lo ->
    opt_pred lo0 lr && opt_pred ro0 rr && cond_true jc (lr ++ rr) && cond_true c (lr ++ rr) =
    opt_pred lp lr && opt_pred rp rr && cond_true jc' (lr ++ rr).
Proof.
  intros c jc jc' lo ro lo0 lp ro0 rp lr rr H Hk.
  unfold inner_ok in H.
  apply andb_true_iff in H as [H Hdis]. apply andb_true_iff in H as [H Hrs].
  apply andb_true_iff in H as [Heq Hls].
  set (t := lr ++ rr).
  assert (AL : forall sp, subset (refs_opt sp) lo = true -> opt_pred sp lr = opt_pred sp t).
  { intros sp Hs. apply opt_pred_agree. intros x Hx. symmetry. apply lookup_app_in.
    rewrite Hk. exact (subset_In _ _ _ Hs Hx). }
  assert (AR : forall sp, disjoint (refs_opt sp) lo = true -> opt_pred sp rr = opt_pred sp t).
  { intros sp Hs. apply opt_pred_agree. intros x Hx. symmetry. apply lookup_app_notin.
    rewrite Hk. exact (disjoint_notin _ _ _ Hs Hx). }
  unfold subset, disjoint in *. rewrite !forallb_app in Hls, Hdis.
  apply andb_true_iff in Hls as [Hl0 Hlp]. apply andb_true_iff in Hdis as [Hr0 Hrp].
  rewrite (AL lo0 Hl0), (AL lp Hlp), (AR ro0 Hr0), (AR rp Hrp).
  rewrite !opt_pred_conj, !cond_true_conj.
  pose proof (cond_equiv_sound _ _ t Heq) as E. rewrite !forallb_app in E.
  destruct (forallb _ (conjs_opt lo0)), (forallb _ (conjs_opt ro0)),
           (forallb _ (conjs_opt jc)), (forallb _ (conjs_opt c)),
           (forallb _ (conjs_opt lp)), (forallb _ (conjs_opt rp)),
           (forallb _ (conjs_opt jc')); simpl in *; congruence.
Qed.

Lemma outer_bool : forall jc jc' lo ro ro0 rp lr rr,
    outer_ok jc jc' lo ro ro0 rp = true -> map fst lr = lo ->
    opt_pred ro0 rr && cond_true jc (lr ++ rr) = opt_pred rp rr && cond_true jc' (lr ++ rr).
Proof.
  intros jc jc' lo ro ro0 rp lr rr H Hk. unfold outer_ok in H.
  apply andb_true_iff in H as [H Hdis]. apply andb_true_iff in H as [Heq Hrs].
  set (t := lr ++ rr).
  assert (AR : forall sp, disjoint (refs_opt sp) lo = true -> opt_pred sp rr = opt_pred sp t).
  { intros sp Hs. apply opt_pred_agree. intros x Hx. symmetry. apply lookup_app_notin.
    rewrite Hk. exact (disjoint_notin _ _ _ Hs Hx). }
  unfold disjoint in Hdis. rewrite forallb_app in Hdis.
  apply andb_true_iff in Hdis as [Hr0 Hrp].
  rewrite (AR ro0 Hr0), (AR rp Hrp), !opt_pred_conj, !cond_true_conj.
  pose proof (cond_equiv_sound _ _ t Heq) as E. rewrite !forallb_app in E.
  destruct (forallb _ (conjs_opt ro0)), (forallb _ (conjs_opt jc)),
           (forallb _ (conjs_opt rp)), (forallb _ (conjs_opt jc')); simpl in *; congruence.
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
  apply cond_equiv_sound. exact Hseq.
Qed.

(** * Soundness *)

Theorem check3_correct_k : forall k o n, size o <= k -> check3 o n = true -> sound_rel o n.
Proof.
  induction k as [|k IHk]; intros o n Hsize Hc; [destruct o; simpl in Hsize; lia|].
  assert (IH : forall q, size q <= k -> forall m, check3 q m = true -> sound_rel q m)
    by (intros q Hq m Hm; exact (IHk q m Hq Hm)).
  assert (Hproj : forall ids q q', size q <= k -> check3 q q' = true ->
            sound_rel (PProject ids q) (PProject ids q')).
  { intros ids q q' Hq H. destruct (IH q Hq q' H) as [_ He]. split; [reflexivity|].
    intros db ops. simpl. now rewrite He. }
  assert (Hside : forall s s' P, size s <= k ->
            side3 (check3 s) (sub_check check3 s) (pchk1 check3 s) (pchk2 check3 s) s s' P = true ->
            exists o0 n0 B B', P o0 n0 = true /\ sound_rel B B' /\ out s = out B /\
              out s' = out B' /\
              (forall db ops, eval db ops s = filter (opt_pred o0) (eval db ops B)) /\
              (forall db ops, eval db ops s' = filter (opt_pred n0) (eval db ops B'))).
  { intros s s' P Hs H. unfold side3 in H. apply orb_true_iff in H as [H|H].
    - destruct (side2_inv check3 s s' P H) as [o0 [b [n0 [b' [-> [-> [Hb HP]]]]]]].
      pose proof (size_wrap o0 b) as Hw.
      destruct (IH b ltac:(lia) b' Hb) as [Ho He].
      exists o0, n0, b, b'. split; [exact HP|]. split; [split; assumption|].
      split; [apply out_wrap|]. split; [apply out_wrap|].
      split; intros db ops; apply eval_wrap.
    - destruct s as [| | ids q | |]; try discriminate.
      destruct s' as [| | ids' q' | |]; try discriminate.
      apply andb_true_iff in H as [Hids H].
      apply (eqb_dec_true _ (list_eq_dec Nat.eq_dec)) in Hids. subst ids'. simpl in Hs.
      apply orb_true_iff in H as [H|H].
      + destruct q' as [| e q2 | | |]; try discriminate.
        apply andb_true_iff in H as [H HP]. apply andb_true_iff in H as [H Hiq].
        apply andb_true_iff in H as [Hq Hre]. simpl in Hq.
        destruct (IH q ltac:(lia) q2 Hq) as [Hoq _].
        exists None, (Some e), (PProject ids q), (PProject ids q2).
        split; [exact HP|]. split; [exact (Hproj ids q q2 ltac:(lia) Hq)|].
        split; [reflexivity|]. split; [reflexivity|]. split; intros db ops.
        * symmetry. apply filter_all. reflexivity.
        * apply proj_filter; [exact Hre | rewrite <- Hoq; exact Hiq].
      + destruct q as [| e0 q0 | | |]; try discriminate. simpl in Hs.
        apply orb_true_iff in H as [H|H].
        * apply andb_true_iff in H as [H HP]. apply andb_true_iff in H as [H Hq].
          apply andb_true_iff in H as [Hre0 Hiq]. simpl in Hq.
          exists (Some e0), None, (PProject ids q0), (PProject ids q').
          split; [exact HP|]. split; [exact (Hproj ids q0 q' ltac:(lia) Hq)|].
          split; [reflexivity|]. split; [reflexivity|]. split; intros db ops.
          -- apply proj_filter; assumption.
          -- symmetry. apply filter_all. reflexivity.
        * destruct q' as [| e q2 | | |]; try discriminate.
          apply orb_true_iff in H as [H|H].
          2: { apply andb_true_iff in H as [H HP]. apply andb_true_iff in H as [H Hiq].
               apply andb_true_iff in H as [H Hre]. apply andb_true_iff in H as [Hq Hre0].
               simpl in Hq. destruct (IH q0 ltac:(lia) q2 Hq) as [Hoq _].
               exists (Some e0), (Some e), (PProject ids q0), (PProject ids q2).
               split; [exact HP|]. split; [exact (Hproj ids q0 q2 ltac:(lia) Hq)|].
               split; [reflexivity|]. split; [reflexivity|]. split; intros db ops.
               - apply proj_filter; assumption.
               - apply proj_filter; [exact Hre | rewrite <- Hoq; exact Hiq]. }
          apply andb_true_iff in H as [H HP]. apply andb_true_iff in H as [H Hiq].
          apply andb_true_iff in H as [H Hrr]. apply andb_true_iff in H as [Hq Hin].
          simpl in Hq. destruct (IH q0 ltac:(lia) q2 Hq) as [Hoq Heq].
          exists None, (conj_of (rest_of e e0)),
                 (PProject ids (PFilter e0 q0)), (PProject ids (PFilter e0 q2)).
          split; [exact HP|]. split.
          { split; [reflexivity|]. intros db ops. simpl. now rewrite Heq. }
          split; [reflexivity|]. split; [reflexivity|]. split; intros db ops.
          -- symmetry. apply filter_all. reflexivity.
          -- cbn [eval]. rewrite filter_map_comm, filter_filter_and. f_equal.
             apply filter_ext_in'. intros t Ht.
             rewrite (split_filter e e0 t Hin). f_equal.
             symmetry. apply opt_pred_agree. intros x Hx.
             pose proof (subset_In _ _ _ Hrr Hx) as Hxi. apply lookup_normalize; [exact Hxi|].
             rewrite (eval_keys db ops q2 t Ht), <- Hoq. exact (subset_In _ _ _ Hiq Hxi). }
  destruct o as [kk o1|c p|ids p|kk o1 p|j jc l r];
    simpl in Hc; apply orb_true_iff in Hc as [Heq|Hc];
    try (apply (eqb_dec_true _ plan_eq_dec) in Heq; subst; split; reflexivity);
    simpl in Hsize.
  - destruct n; discriminate.
  - (* Filter *)
    destruct n as [| c' p' | ids' p' | | j' jc' l' r']; try discriminate.
    + apply orb_true_iff in Hc as [Hc|Heoj];
        [apply orb_true_iff in Hc as [Hc|Hlo]; [apply orb_true_iff in Hc as [Hc|Hc]|]|].
      * apply andb_true_iff in Hc as [Hfe Hp].
        destruct (IH p ltac:(lia) _ Hp) as [Ho He]. split; [exact Ho|].
        intros db ops. simpl. rewrite <- He. apply filter_ext_in'. intros t _.
        rewrite !conj_true. apply cond_equiv_sound. exact Hfe.
      * destruct p as [| c2 q | | |]; try discriminate.
        apply andb_true_iff in Hc as [Hfe Hq]. simpl in Hsize.
        destruct (IH q ltac:(lia) _ Hq) as [Ho He]. split; [exact Ho|].
        intros db ops. simpl. rewrite filter_filter_and, <- He.
        apply filter_ext_in'. intros t _.
        rewrite (conj_true c'), <- (cond_equiv_sound _ _ t Hfe), forallb_app, <- !conj_true.
        reflexivity.
      * destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        destruct p' as [| | | | j2 jc' l' r']; try discriminate. destruct j2; try discriminate.
        apply andb_true_iff in Hlo as [Hlo Hl]. apply andb_true_iff in Hlo as [Hjc Hr].
        apply (eqb_dec_true _ oexpr_eq_dec) in Hjc. subst jc'. simpl in Hsize.
        destruct (IH r ltac:(lia) _ Hr) as [Hor Her].
        apply orb_true_iff in Hl as [Hl|Hl].
        -- apply andb_true_iff in Hl as [Hl Hok].
           destruct (IH l ltac:(lia) _ Hl) as [Hol Hel]. split.
           ++ simpl. now rewrite Hol, Hor.
           ++ intros db ops. cbn [eval]. rewrite <- Hel, <- Her, <- Hor.
              rewrite <- (filter_all _ (opt_pred None) (eval db ops l)) at 2 by reflexivity.
              apply left_outer_filter_shift. intros lr x Hlr.
              exact (lo_bool c (Some c') None (out l) lr x Hok (eval_keys db ops l lr Hlr)).
        -- destruct l' as [| e l0 | | |]; try discriminate.
           apply andb_true_iff in Hl as [Hl Hok].
           destruct (IH l ltac:(lia) _ Hl) as [Hol Hel]. split.
           ++ simpl. now rewrite Hol, Hor.
           ++ intros db ops. cbn [eval]. rewrite <- Hel, <- Her, <- Hor.
              apply (left_outer_filter_shift jc _ (opt_pred (Some e))). intros lr x Hlr.
              exact (lo_bool c (Some c') (Some e) (out l) lr x Hok (eval_keys db ops l lr Hlr)).
      * (* EliminateOuterJoin (V21) *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        destruct p' as [| | | | j2 jc' l' r']; try discriminate. destruct j2; try discriminate.
        apply andb_true_iff in Heoj as [H Hx]. apply andb_true_iff in H as [H Hr].
        apply andb_true_iff in H as [H Hl]. apply andb_true_iff in H as [Hce Hjc].
        apply (eqb_dec_true _ oexpr_eq_dec) in Hjc. subst jc'. simpl in Hsize.
        destruct (IH l ltac:(lia) _ Hl) as [Hol Hel].
        destruct (IH r ltac:(lia) _ Hr) as [Hor Her].
        apply existsb_exists in Hx as [x [Hxn Hx]]. apply andb_true_iff in Hx as [Hxr Hxl].
        apply memb_In in Hxr. apply negb_true_iff in Hxl.
        assert (Hnl : ~ In x (out l)) by (intros Hin; apply memb_In in Hin; congruence).
        split.
        -- simpl. now rewrite Hol, Hor.
        -- intros db ops. cbn [eval]. rewrite <- Hel, <- Her, <- Hor.
           rewrite (eoj_shift jc (fun t => is_true (eval_e c t)) (out l) (out r) x
                      (eval db ops l) (eval db ops r) (eval_keys db ops l) Hxr Hnl
                      (fun t Ht => nr_sound c x t Hxn Ht)).
           apply filter_ext_in'. intros t _. rewrite !conj_true. apply cond_equiv_sound. exact Hce.
    + (* push through Project *)
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
        rewrite (Hnorm t (eval_keys db ops q t Ht)), !conj_true.
        apply cond_equiv_sound. exact Hfe.
      * destruct q as [| c2 q0 | | |]; try discriminate.
        apply andb_true_iff in Halt as [Hfe Hq]. simpl in Hsize.
        destruct (IH q0 ltac:(lia) _ Hq) as [Ho He]. split; [reflexivity|].
        intros db ops. cbn [eval]. rewrite <- He, filter_map_comm, filter_filter_and. f_equal.
        apply filter_ext_in'. intros t Ht.
        assert (Hk : map fst t = out (PFilter c2 q0)) by (simpl; exact (eval_keys db ops q0 t Ht)).
        rewrite (Hnorm t Hk), (conj_true c'), <- (cond_equiv_sound _ _ t Hfe),
                forallb_app, <- !conj_true.
        reflexivity.
    + destruct j'.
      * (* unified INNER law with a WHERE above *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        simpl in Hsize.
        destruct (Hside l l' _ ltac:(lia) Hc) as [lo0 [lp [LB [LB' [Hr [[HolB HelB] [Hol [Hol' [Hel Hel']]]]]]]]].
        destruct (Hside r r' _ ltac:(lia) Hr) as [ro0 [rp [RB [RB' [Hok [[HorB HerB] [Hor [Hor' [Her Her']]]]]]]]].
        split.
        -- simpl. rewrite Hol, Hol', Hor, Hor', HolB, HorB. reflexivity.
        -- intros db ops. cbn [eval]. rewrite Hel, Hel', Her, Her', <- HelB, <- HerB.
           apply inner_join_shift. intros lr rr Hlr Hrr.
           apply (inner_bool (Some c) jc jc' (out l) (out r)).
           ++ exact Hok.
           ++ rewrite Hol. exact (eval_keys db ops LB lr Hlr).
      * (* LEFT OUTER: WHERE conjuncts into the preserved side, nothing kept above *)
        destruct p as [| | | | j jc l r]; try discriminate. destruct j; try discriminate.
        apply andb_true_iff in Hc as [Hc Hl]. apply andb_true_iff in Hc as [Hjc Hr].
        apply (eqb_dec_true _ oexpr_eq_dec) in Hjc. subst jc'. simpl in Hsize.
        destruct (IH r ltac:(lia) _ Hr) as [Hor Her].
        apply orb_true_iff in Hl as [Hl|Hl].
        -- apply andb_true_iff in Hl as [Hl Hok].
           destruct (IH l ltac:(lia) _ Hl) as [Hol Hel]. split.
           ++ simpl. now rewrite Hol, Hor.
           ++ intros db ops. cbn [eval]. rewrite <- Hel, <- Her, <- Hor.
              rewrite (left_outer_filter_shift jc _ (opt_pred None) (fun _ => true) _ _ _
                (fun lr x Hlr => lo_bool c None None (out l) lr x Hok (eval_keys db ops l lr Hlr))).
              rewrite !filter_all by reflexivity. reflexivity.
        -- destruct l' as [| e l0 | | |]; try discriminate.
           apply andb_true_iff in Hl as [Hl Hok].
           destruct (IH l ltac:(lia) _ Hl) as [Hol Hel]. split.
           ++ simpl. now rewrite Hol, Hor.
           ++ intros db ops. cbn [eval]. rewrite <- Hel, <- Her, <- Hor.
              rewrite (left_outer_filter_shift jc _ (opt_pred (Some e)) (fun _ => true) _ _ _
                (fun lr x Hlr => lo_bool c None (Some e) (out l) lr x Hok (eval_keys db ops l lr Hlr))).
              rewrite (filter_all _ (fun _ : row => true)) by reflexivity. reflexivity.
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
  - (* Join *)
    destruct n as [| | | | j' jc' l' r']; destruct j; simpl in Hc; try discriminate;
      destruct j'; simpl in Hc; try discriminate.
    + (* unified INNER law, no WHERE above *)
      destruct (Hside l l' _ ltac:(lia) Hc) as [lo0 [lp [LB [LB' [Hr [[HolB HelB] [Hol [Hol' [Hel Hel']]]]]]]]].
      destruct (Hside r r' _ ltac:(lia) Hr) as [ro0 [rp [RB [RB' [Hok [[HorB HerB] [Hor [Hor' [Her Her']]]]]]]]].
      split.
      * simpl. rewrite Hol, Hol', Hor, Hor', HolB, HorB. reflexivity.
      * intros db ops. cbn [eval]. rewrite Hel, Hel', Her, Her', <- HelB, <- HerB.
        rewrite <- (filter_all _ (fun _ => true)
          (join Inner jc (out r) (filter (opt_pred lo0) (eval db ops LB))
             (filter (opt_pred ro0) (eval db ops RB)))) by reflexivity.
        apply inner_join_shift. intros lr rr Hlr Hrr.
        pose proof (inner_bool None jc jc' (out l) (out r) lo0 lp ro0 rp lr rr Hok) as B.
        rewrite Hol in B. specialize (B (eval_keys db ops LB lr Hlr)).
        simpl in B. rewrite andb_true_r in B. rewrite andb_true_r. exact B.
    + (* LEFT OUTER: right side of ON *)
      apply andb_true_iff in Hc as [Hl Hr].
      destruct (IH l ltac:(lia) _ Hl) as [Hol Hel].
      destruct (Hside r r' _ ltac:(lia) Hr) as [ro0 [rp [RB [RB' [Hok [[HorB HerB] [Hor [Hor' [Her Her']]]]]]]]].
      split.
      * simpl. rewrite Hol, Hor, Hor', HorB. reflexivity.
      * intros db ops. cbn [eval]. rewrite <- Hel, Her, Her', <- HerB, Hor', <- HorB, <- Hor.
        apply left_outer_right_shift. intros lr rr Hlr Hrr.
        exact (outer_bool jc jc' (out l) (out r) ro0 rp lr rr Hok
                 (eval_keys db ops l lr Hlr)).
Qed.

Theorem check3_sound : forall o n, check3 o n = true ->
    forall db ops, eval db ops o = eval db ops n.
Proof.
  intros o n H. exact (proj2 (check3_correct_k (size o) o n (le_n _) H)).
Qed.

(** * Kernel-evaluated controls *)

Definition t_left := PLeaf 0 [0; 1; 2].
Definition t_right := PLeaf 1 [3; 4; 5].
Definition jcond := Some (EEq (EAttr 0) (EAttr 3)).
Definition lit (z : Z) := ELit (Some (VInt z)).
Definition pos_j1 := EGt (EAttr 1) (lit 0).
Definition pos_r4 := ELt (EAttr 4) (lit 3).

Example accepts_iffc_guards :
  check3 (PJoin Inner jcond t_left t_right)
         (PJoin Inner jcond (PFilter (EIsNotNull (EAttr 0)) t_left)
                            (PFilter (EIsNotNull (EAttr 3)) t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_left_outer_preserved_guard :
  check3 (PJoin LeftOuter jcond t_left t_right)
         (PJoin LeftOuter jcond (PFilter (EIsNotNull (EAttr 0)) t_left) t_right) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_null_supplying_guard :
  check3 (PJoin LeftOuter jcond t_left t_right)
         (PJoin LeftOuter jcond t_left (PFilter (EIsNotNull (EAttr 3)) t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_on_clause_pushdown :
  check3 (PJoin Inner (Some (EAnd (EEq (EAttr 0) (EAttr 3)) (EAnd pos_j1 pos_r4))) t_left t_right)
         (PJoin Inner jcond (PFilter pos_j1 t_left) (PFilter pos_r4 t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example accepts_left_outer_on_right_pushdown :
  check3 (PJoin LeftOuter (Some (EAnd (EEq (EAttr 0) (EAttr 3)) pos_r4)) t_left t_right)
         (PJoin LeftOuter jcond t_left (PFilter pos_r4 t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_left_outer_on_left_pushdown :
  check3 (PJoin LeftOuter (Some (EAnd (EEq (EAttr 0) (EAttr 3)) pos_j1)) t_left t_right)
         (PJoin LeftOuter jcond (PFilter pos_j1 t_left) t_right) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_fused_push_and_combine :
  check3 (PFilter pos_r4 (PJoin Inner jcond t_left (PFilter (EIsNotNull (EAttr 5)) t_right)))
         (PJoin Inner jcond t_left (PFilter (EAnd (EIsNotNull (EAttr 5)) pos_r4) t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_fused_push_dropping_existing_filter :
  check3 (PFilter pos_r4 (PJoin Inner jcond t_left (PFilter (EIsNotNull (EAttr 5)) t_right)))
         (PJoin Inner jcond t_left (PFilter pos_r4 t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_isnotnull_from_in_and_add :
  check3 (PFilter (EAnd (EIn (EAttr 1) [Some (VInt 1); None]) (EGt (EAdd 32 (EAttr 2) (lit 1)) (lit 0))) t_left)
         (PFilter (EAnd (EAnd (EIsNotNull (EAttr 1)) (EIsNotNull (EAttr 2)))
                        (EAnd (EIn (EAttr 1) [Some (VInt 1); None]) (EGt (EAdd 32 (EAttr 2) (lit 1)) (lit 0)))) t_left) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_isnotnull_from_isnull :
  check3 (PFilter (EIsNull (EAttr 1)) t_left)
         (PFilter (EAnd (EIsNotNull (EAttr 1)) (EIsNull (EAttr 1))) t_left) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_push_through_left_outer_into_right :
  check3 (PFilter pos_r4 (PJoin LeftOuter jcond t_left t_right))
         (PJoin LeftOuter jcond t_left (PFilter pos_r4 t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_wrong_side_inner_push :
  check3 (PFilter pos_j1 (PJoin Inner jcond t_left t_right))
         (PJoin Inner jcond t_left (PFilter pos_j1 t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_on_push_through_project_into_filter :
  check3 (PJoin Inner (Some (EAnd (EEq (EAttr 0) (EAttr 3)) pos_j1))
            (PProject [0; 1] (PFilter (EIsNotNull (EAttr 2)) t_left)) t_right)
         (PJoin Inner jcond
            (PProject [0; 1] (PFilter (EAnd (EIsNotNull (EAttr 2)) pos_j1) t_left)) t_right) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_push_through_project_of_unprojected_column :
  check3 (PJoin Inner (Some (EAnd (EEq (EAttr 0) (EAttr 3)) (EGt (EAttr 2) (lit 0))))
            (PProject [0; 1] t_left) t_right)
         (PJoin Inner jcond (PProject [0; 1] (PFilter (EGt (EAttr 2) (lit 0)) t_left)) t_right) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_transitive_iffc_constraint :
  check3 (PJoin Inner (Some (EEq (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left) t_right)
         (PJoin Inner (Some (EEq (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left)
                (PFilter (EGe (EAttr 3) (lit 1)) t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_transport_along_null_safe_equality :
  check3 (PJoin Inner (Some (ENullSafeEq (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left) t_right)
         (PJoin Inner (Some (ENullSafeEq (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left)
                (PFilter (EGe (EAttr 3) (lit 1)) t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_transport_without_equality :
  check3 (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left) t_right)
         (PJoin Inner (Some (ELt (EAttr 0) (EAttr 3))) (PFilter (EGe (EAttr 0) (lit 1)) t_left)
                (PFilter (EGe (EAttr 3) (lit 1)) t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_eliminate_outer_join :
  check3 (PFilter (EGt (EAttr 4) (lit 0)) (PJoin LeftOuter jcond t_left t_right))
         (PFilter (EGt (EAttr 4) (lit 0)) (PJoin Inner jcond t_left t_right)) = true.
Proof. vm_compute. reflexivity. Qed.

Example rejects_eliminate_outer_join_on_preserved_side :
  check3 (PFilter (EGt (EAttr 1) (lit 0)) (PJoin LeftOuter jcond t_left t_right))
         (PFilter (EGt (EAttr 1) (lit 0)) (PJoin Inner jcond t_left t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example rejects_eliminate_outer_join_with_isnull :
  check3 (PFilter (EIsNull (EAttr 4)) (PJoin LeftOuter jcond t_left t_right))
         (PFilter (EIsNull (EAttr 4)) (PJoin Inner jcond t_left t_right)) = false.
Proof. vm_compute. reflexivity. Qed.

Example accepts_transport_from_filter_under_project :
  check3 (PJoin Inner (Some (EEq (EAttr 0) (EAttr 3)))
            (PProject [0] (PFilter (EGe (EAttr 0) (lit 1)) t_left)) (PProject [3; 4] t_right))
         (PJoin Inner (Some (EEq (EAttr 0) (EAttr 3)))
            (PProject [0] (PFilter (EAnd (EIsNotNull (EAttr 0)) (EGe (EAttr 0) (lit 1))) t_left))
            (PFilter (EAnd (EGe (EAttr 3) (lit 1)) (EIsNotNull (EAttr 3))) (PProject [3; 4] t_right))) = true.
Proof. vm_compute. reflexivity. Qed.

Print Assumptions check3_sound.
