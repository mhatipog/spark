# QueryProof Round P — two-sided join pruning, bag semantics, and counterexamples

**Date:** 2026-10-20  
**Status:** MATHEMATICAL SUBLEMMA + FINITE INDEPENDENT MODEL FALSIFICATION. **Not** a Catalyst/JVM transition certificate, prospective SQL holdout, or a mechanically kernel-checked theorem.  
**Repository branch:** `research/queryproof-adversarial-tag-frame-20261020`

## Why we investigated this

Prospective O32 first run certified **16/16** targeted extrema/nested aggregate suffixes but **0/16** INNER JOIN, LEFT JOIN, DISTINCT, and HAVING challenges ([O32 checkpoint](2026-10-20-prospective-o32-first-run.md)). The posthoc cases M29 and M33 exhibit actual Spark `ColumnPruning` transformations that insert projections *inside both join inputs*. The old fixed-base projection checker correctly abstains with `PROJECT_BASE_CHANGED`; we must not reinterpret its abstention as a failure of Spark equivalence. We need a theorem specifically handling a changing two-input base.

## Exact relational sublemma

Let `L` and `R` be **finite bags** of rows, with nonnegative integer multiplicity functions `mu_L(l)` and `mu_R(r)`. Define exact *identity-column* projections `alpha` and `beta`, keeping all original input identifiers and types for the retained columns. Assume:

1. The inner or left-outer join predicate `theta(l,r)` is **pure and deterministic**, with SQL three-valued comparisons, and factors through the retained fields: `theta(l,r) = theta_bar(alpha(l),beta(r))`.
2. The final output projection `o(l,r)` likewise factors through the retained fields, including for the left-outer NULL-extended right row: `o = o_bar(alpha(l),beta(r))`.
3. The projections preserve **bag multiplicity**: `mu_alphaL(u) = sum_{l:alpha(l)=u} mu_L(l)`; analogously on the right. A `DISTINCT` or set interpretation is explicitly excluded.
4. The transformations are exact attribute selections, not value-changing expressions, introduced casts, volatile computations, reordering-sensitive effects, or name/exprId substitution.

Then the *final output bags* are equal when these projections are pushed into the two inputs of an **INNER JOIN** or a **LEFT OUTER JOIN**. This is a logical bag-level lemma. It does not claim equality of raw intermediate join schemas, execution errors, predicate evaluation counts, ordering, or physical effects.

### Derivation (universal over finite bag inputs, under assumptions)

Set `U=alpha(L)` and `V=beta(R)`, retaining summed duplicate multiplicities. For inner joins, the multiplicity of an output tuple `t` is

```text
mu_inner(t) =
  sum_{u,v} mu_U(u) * mu_V(v)
             * [theta_bar(u,v) is TRUE] * [o_bar(u,v)=t].
```

This follows by distributing the two finite sums over all original input rows, because both predicate and output depend only on retained attributes. The same formula describes a join after the two projections; hence the bags coincide.

For left outer joins, the **matched part** is the same expression. Define the number of matching *right-row occurrences* for each retained left row `u`:

```text
m(u) = sum_v mu_V(v) * [theta_bar(u,v) is TRUE].
```

The NULL-extended unmatched contribution is

```text
mu_unmatched(t) =
  sum_u mu_U(u) * [m(u)=0] * [o_bar(u,NULL_right)=t].
```

Whether a left occurrence has any right match depends only on `u` and the multiplicity-preserving right projection. Thus matched and unmatched terms are each invariant to dropping unused columns. This handles duplicate preserved left rows, duplicate right matches, and SQL `NULL` / UNKNOWN separately from the all-TRUE case.

This is an ordinary relational-algebra identity with explicit hypotheses, not a novel general SQL equivalence algorithm. The new value for QueryProof would lie in *mechanically recognizing, certifying and witnessing it on a real Catalyst transition*.

## Executed finite exhaustive model

Committed runner: `tools/queryproof_join_model_p_20261020/exhaustive_projection_join.py`, Git blob **390c3b463d26ed85e6b4ed096934ff2144ec80f7**. It uses two independently organized algorithms:

- Original full-input nested-loop bag semantics with each source row occurrence kept, plus left-outer NULL extension;
- Project-then-join semantics with `Counter` aggregation, explicit *multiplication* of matching row multiplicities, and unmatched-left row multiplicities.

The input domain has **18** distinct full rows per side: key in `{NULL,0,1}`, value in `{NULL,0,1}`, and removable noise in `{0,1}`. All bags of size 0, 1 or 2 over these rows are enumerated: **190 possible bags on each side**, **36,100 input-bag pairs**. For each pair, evaluate:

- INNER and LEFT OUTER;
- NULL-aware key equality, and key equality AND right-value-is-not-NULL;
- Value-only output and key-plus-value output.

Total independent model comparisons: **36,100 × 2 × 2 × 2 = 288,800**.

**First successful CI:** [39423911030](https://github.com/mhatipog/spark/actions/runs/39423911030), HEAD `6d03078b13f88d9c772662d0718c0beaab87d588`, `QUERYPROOF_P_JOIN_BAG_MODEL`:

| Measurement | Result |
|---|---:|
| Full vs exactly projected rowbag instances | **288,800** |
| Mismatches under theorem assumptions | **0** |
| INNER/JOIN comparisons per predicate/output combination | **36,100** |
| LEFT/JOIN comparisons per predicate/output combination | **36,100** |
| Bag-deduplication mutant | **Falsified** |
| Missing-key/wrong-condition mutant | **Falsified** |
| Spark Catalyst optimizer transitions executed by this test | **0** |

Concrete falsifying mutants discovered automatically:

- **Incorrectly treating a projected bag as a set:** one left row `(key=0,value=NULL)`, two equal right rows `(0,NULL)`; expected output `(NULL,NULL)` occurs **twice**, mutant emits it once. Dropping duplicate multiplicity breaks even INNER JOIN.
- **Dropping join key and accidentally comparing surviving value instead:** left `(key=NULL,value=0)`, right `(key=NULL,value=0)`. Genuine `NULL=NULL` is UNKNOWN, so the INNER JOIN has **no** output; mutant produces `(0,0)`.

## What this does not yet prove

These model checks are independent of Spark and cannot certify M29/M33 or any O32 join case. The algebraic conditions must be **checked against each real before/after Catalyst AST**, with typed attribute IDs, live join condition references, output schema, all retained columns, exact project identity, normalized subtrees and unchanged join type/null-extension behavior.

In particular, it is possible to misuse this lemma by:

- Checking column names instead of Spark expression identities;
- Accidentally omitting condition-only join keys while keeping final-output columns;
- Interpreting projection as duplicate elimination;
- Mislabeling a left join as inner, or losing the unmatched-left branch;
- Assuming a pure predicate when UDFs, errors, evaluation counts or volatile expressions are in play;
- Reusing the lemma across additional rule transitions whose semantics have not been certified.

O32 `16/32` remains untouched, as do all frozen M/H/K/L and failed N first-run data. This model result is **not** another prospectively earned suffix certificate.

## Next precise engineering task

Inspect **one observed M29 `ColumnPruning` before/after JSON AST** without exfiltrating source row contents. Build a source-pinned verifier that recognizes **exact inner equijoin identity projection insertion**, ensuring retained key and output references on both inputs, exprId/type equality, multiplicity-preserving project nodes, and unchanged condition/join shape; otherwise `UNKNOWN`. Feed it adversarial AST mutations (drop condition-only key, swap exprId, remove right child, turn left into inner, alter predicate, change `Project` to `Distinct`). Before writing fresh joins into a new holdout, run it separately on the known M29/M33 development fixtures, rerun independent state/frame/live-RDD/all-rule/tag witnesses, and only then preregister an untouched join-focused corpus.

**Research decision:** The algebra supports a careful join-extension strategy; the observed Spark join proof gap remains real and open.
