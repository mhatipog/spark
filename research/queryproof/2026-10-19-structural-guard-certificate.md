# QueryProof research checkpoint — exact INNER JOIN guard certificates (2026-10-19)

## What is established

A separate Spark 4.2.0 Catalyst certificate now proves the soundness of a narrow
**structural** rewrite, beyond the earlier equality-orbit condition-only checker:

```
Join_Inner(C, L, R)
    -->
Join_Inner(C, Filter(G_L, L), Filter(G_R, R))
```

Either inserted Filter may be absent. The original child subtrees, JOIN metadata,
and join predicate are *byte-for-byte identical* in decoded Catalyst JSON.
The certificate accepts this transformation only when all inserted guards are
side-local, in the supported pure nullable-integral Boolean/equality grammar,
and the exhaustive equality-orbit check proves

```
for every joined row valuation: C == TRUE implies each G_side == TRUE.
```

### Mathematical argument

For any input row bags `L` and `R`, the multiplicity of a pair `(l,r)`
in the original inner join is
`m_L(l) * m_R(r) * 1[C(l,r) = TRUE]`.
After filtering, it is
`m_L(l) * m_R(r) * 1[C(l,r) = TRUE] * product(1[G_side = TRUE])`.
The universal implication makes those multiplicities identical. Thus the
result bags agree for *all* finite input bags, not only the fixed SQL fixture.

The exact finite proof uses the Bell(n+1) equality/NULL orbit quotient for
n integral attribute identities (currently n <= 7). Nonstandard truth domains,
arithmetic, implicit casts, NaN, UDFs, subqueries, nondeterminism, and other
unmodeled semantics remain outside the scope. Each accepted implication is
independently verified by SQLite on the complete orbit representatives.

### Parser and scope safeguards

* Require a well-formed Catalyst preorder tree.
* Require the exact Spark logical Join class, binary arity, `left=0`,
  `right=1`, and precisely identified Inner join type.
* Require unchanged root JOIN metadata and identical original child subtrees.
* Accept exactly zero or one **new** Filter wrapper per side, with Spark's
  actual `child=0` index and exact node fields.
* Require every guarded side to be a direct Project with an integer-typed,
  direct AttributeReference output list; check side output IDs are disjoint.
* Verify that the JOIN predicate is scoped to the union of those outputs and
  each guard is scoped to its own side.
* Prove TRUE-row predicate equivalence by enumerating all equality/NULL orbits,
  and independently check the result in SQLite.
* Abstain (`UNKNOWN`) when any precondition cannot be established. A failing
  implication is not treated as proof of a reachable Spark bug, because child
  relations may satisfy stronger constraints than the unconstrained grammar.

## Actual experimental results

| Cohort | SQL cases | Catalyst changed transitions | Old predicate-only certified | Added INNER JOIN guard certificates | Spark vs SQLite mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| October 18 frozen *development* corpus | 18 | 97 | 19 | **4** (retrospective exploratory: J01, J02, J03, J05) | 0 |
| October 19 newly frozen **prospective confirmation** | 18 | 113 | 1 | **6** (S01, S02, S03, S06, S07, S08) | 0 |

The two certificate families cover distinct event structures (predicate-only
same-shape changes versus newly inserted Filter wrappers). The October 19
confirmation therefore has **7 distinct certified transitions out of 113**,
compared with **1 out of 113** for the old predicate-only checker.

All six new confirmation certificates concerned real
`InferFiltersFromConstraints` transitions; each had two distinct integral
identities, five complete equality/NULL orbits, and an independent
`sqlite_verified=True` result. The left-outer-join negative controls did
not receive the INNER JOIN certificate.

The October 18 extension score is *not* an independent holdout claim.
The October 19 confirmation cases/data were frozen after developing the
new theorem and before executing that new cohort, but chosen deliberately
to probe the transformation family, not sampled blindly from industrial SQL.

CI provenance:

* Baseline hardened replay:
  https://github.com/mhatipog/spark/actions/runs/39298157665
* Structural diagnostics:
  https://github.com/mhatipog/spark/actions/runs/39298874697
* Original-cohort exploratory guard replay:
  https://github.com/mhatipog/spark/actions/runs/39300029362
* Prospective fresh cohort, both jobs passed:
  https://github.com/mhatipog/spark/actions/runs/39300521684

Fidelity anchors:

* New manifest SHA-256:
  `485ad6ecb34cbcdd05b0fa0f36d397b11d1b542d0b57321f5b5a00ebadb9da08`.
* New protocol SHA-256:
  `26d7e3c6a3c6fe9c2b364110a671b99f895bf8e17a66c3be7e17515c13da3c8b`.
* Frozen verifier anchor: commit
  `afad3673f90c8dbf95a3ab11c8b7eec61d238ae9`.
* Confirmation execution anchor:
  `539f80f97ad812ce78ebe2aff14cf2169e3dab91`.

## What remains unsupported

The October 18 UNKNOWN breakdown was
`53 PLAN_SHAPE_CHANGED; 20 MULTIPLE_PLAN_NODES_CHANGED;
3 INPUT_IDS_CHANGED; 2 CHANGED_NODE_NOT_PREDICATE_CONSUMER`.
Shape-only diagnostics showed 55 of those 78 concerned broad Analyzer /
Optimizer alias or View wrapper changes; they should not be confused with
the more interesting optimizer rewrite failures.

Other unsupported effective rules include `ColumnPruning`,
`PushDownPredicates`, `RemoveNoopOperators`, `CollapseProject`, and
nested placements of `InferFiltersFromConstraints`. On the October 19
confirmation, six structurally interesting events remained outside the
direct-Project guard matcher. The current parser is deliberately
conservative. Do not silently relax it or retrospectively rescore the
confirmation as an untouched holdout.

## Next research round

1. Design a *separate* compositional proof for Filter pushdown through
   identity/renaming-only Project. Require explicit expression-ID mapping,
   projection determinism, correct bag multiplicity, and a strict semantic
   frame rule for any ancestor context; reject effects, opaque projections,
   and type-changing aliases.
2. Add counterexample-oriented synthetic tests (mis-scoped IDs, overlapping
   join outputs, invalid child indexes, null-safe equality that cannot imply
   not-null, outer join, duplicate rows and NULL).
3. Freeze a new randomized family-stratified SQL corpus and fresh source rows
   **before** running upgraded verifier code against it. Include additional
   join arity/NULL cases and negative controls, and report abstentions.
4. Replicate the exact theorem on another Spark release/build without changing
   the verifier; keep all raw `toJSON` ASTs private and publish only
   sanitized per-rule diagnostics.
5. Evaluate the method as a correctness research framework, not as a
   newly demonstrated Apache Spark bug. Keep unrelated upstream PR #59445
   and security findings separate from this experimental branch.

Relevant implementation:

* `tools/queryproof_context_validator/equality_orbits.py`
* `tools/queryproof_context_validator/join_prefilter_certificate.py`
* `tools/queryproof_context_validator/test_join_prefilter.py`
* `tools/queryproof_holdout_20261018/run_holdout.py` (original benchmark
  primary scores untouched; new measurements isolated in separate fields)
* `tools/queryproof_structural_confirmation_20261019/{CASES.json,FROZEN_PROTOCOL.json,run_prospective.py}`
* `.github/workflows/queryproof-certificate-boundary.yml`
