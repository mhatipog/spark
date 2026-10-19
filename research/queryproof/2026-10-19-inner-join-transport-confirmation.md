# QueryProof — complete INNER JOIN predicate transport: independent confirmation (2026-10-19)

## Decisive new result

A fifth, separately frozen, family-stratified suite on official PySpark
4.2.0 executed all **18 SQL queries** and observed **147 changed Catalyst
RuleExecutor transitions**. The newly frozen exact INNER JOIN
predicate-transport theorem produced **five independently verified
certificates** (cases T10–T14) and **zero Spark-versus-SQLite SQL row-bag
mismatches**. Both GitHub-hosted CI jobs passed.

- Verification implementation anchor:
  `35314636b8d0ce04d922a3df0f2fa72c0974325c`.
- Fresh manifest, data, and protocol first introduced and executed at:
  `974a3b0dc925d50f440d8bc28dd44e29a4926d97`.
- CI execution and sanitized logs:
  https://github.com/mhatipog/spark/actions/runs/39305299102
- Corpus:
  `tools/queryproof_transport_confirmation_20261019/CASES.json`
- Frozen protocol:
  `tools/queryproof_transport_confirmation_20261019/FROZEN_PROTOCOL.json`

The experimental design uses fixed LCG seed `0x20261021` and new nullable
integral datasets with deliberate duplicate/all-NULL rows. It comprises
5 single-side WHERE filters, 4 conjunctive two-side filters, 3 cross-side
join filters, 2 disjunctive three-valued controls, 2 null contradictions,
and 2 LEFT OUTER controls. It is **design-informed template generation,
not a blind representative industrial workload sample**.

## Exact theorem

Let `L` and `R` be finite bags of rows, `C` an INNER JOIN's original
predicate, and `P` an original post-join `Filter` predicate. A Catalyst
rule proposes replacing

```
Filter(P, Join_Inner(C, L, R))
```

by

```
Join_Inner(C', Filter(G_L, L), Filter(G_R, R)).
```

Either side guard may be absent. Require exact metadata equivalence for
the INNER JOIN except its permitted predicate change, unchanged original
child subtrees, the precise Spark unary Filter schema, identical logical
output expression IDs, two scoped identity/alias-only Project children,
and all formulas in the restricted pure nullable-integral
equality/NULL/NOT/AND/OR grammar.

For any row pair `(l,r)`, the original join's output pair multiplicity is

```
m_L(l) * m_R(r) * 1[C(l,r) = TRUE] * 1[P(l,r) = TRUE].
```

The rewritten multiplicity is

```
m_L(l) * m_R(r) * 1[C'(l,r) = TRUE]
                  * 1[G_L(l) = TRUE] * 1[G_R(r) = TRUE].
```

The proof checks these indicator products are identical on **all
NULL/equality orbits** for the set of scoped integral attributes. By
permutation invariance of the grammar, checking Bell(n+1) orbits is
complete; this certificate supports at most 7 attribute identities.
SQLite independently re-evaluates every orbit in its own SQL engine.

Thus any accepted event preserves the complete output bag for **all
finite input bags that satisfy the modeled pure logical-plan
preconditions**, not merely the finite execution fixture.

The result also lifts through one or more unchanged pure, direct- or
alias-only outer `Project` frames. The outer-frame theorem is a
bag-homomorphism: equal child multisets under deterministic per-row
projection yield identical projected multisets.

## Fresh results

| Frozen cohort | Spark queries | Changed optimizer transitions | Prospectively confirmed new theorem | SQLite failures | Row-bag mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fourth: pure Project frames | 18/18 | 147 | **11 Project-frame certificates** | 0 | 0 |
| Fifth: complete JOIN predicate transport | 18/18 | 147 | **5 JOIN-transport certificates** | 0 | 0 |

The fifth theorem's five accepted cases were `T10` (15 orbits),
`T11`–`T14` (52 orbits each). All are actual
`PushDownPredicates` events, each with **one unchanged pure outer
`Project` frame**. They cover cross-side SQL predicates and disjunctive
NULL logic. Direct unframed transport is 0/147, a useful architectural
observation rather than a proof failure.

Retrospectively, applying the transport checker to the *earlier*
fourth cohort also finds **8 additional** framed transport events, but
those **must not** be included as independent holdout evidence for this
new verifier. Original/earlier suite results remain frozen and are not
rewritten into larger retrospective holdout claims.

## Scientific interpretation

The program has now demonstrated three forms of sound compositional
structural certification on actual Spark 4.2 Catalyst optimizer traces:

1. Redundant side guards implied by an INNER JOIN condition.
2. Filter/Project commutation with explicit attribute-ID substitution,
   including strict context frames.
3. Transport of complete post-join WHERE predicates across INNER JOIN,
   permitting redistribution into join conditions and side filters.

This is an experimentally validated **research prototype of a narrow
runtime-independent proof checker**, not proof that *all* Spark
optimizations are correct, not a production ready validator, and
**not a discovered Spark bug**.

Important remaining UNKNOWN cases include:
- Catalyst's large analyzer/view/subquery alias wrapper transformations.
- Changes to Projection expressions rather than their position only.
- Compound changes across several plan nodes and scopes not yet modeled.
- Arbitrary arithmetic, casts, literals, floating-point NaN, UDFs,
  subqueries, nondeterminism, and other value-domain semantics.
- Broader plan operators such as Aggregate, outer join rewrites and
  Window, not justified by the current INNER JOIN certificates.

The next high-value phase is **external workload generalization and
proof-coverage calibration**, not mere accumulation of handcrafted
positive cases. Freeze an independently sourced collection of SQL
queries or a rule-family-stratified query generator with a locked seed.
Then run an unchanged certifier on a second Spark release, and publish
a coverage matrix per operator/rule/SQL family with UNKNOWN abstentions
and false-positive adversarial mutation tests. Keep the raw ASTs private
and the GitHub runner hosted.

## Implementation and provenance

- `tools/queryproof_context_validator/equality_orbits.py`
- `tools/queryproof_context_validator/join_prefilter_certificate.py`
- `tools/queryproof_context_validator/project_pushdown_certificate.py`
- `tools/queryproof_context_validator/join_filter_transport_certificate.py`
- `tools/queryproof_context_validator/compositional_project_frame_certificate.py`
- `tools/queryproof_context_validator/test_join_filter_transport.py`
- `tools/queryproof_context_validator/test_compositional_project_frame.py`
- `tools/queryproof_holdout_20261018/run_holdout.py`
- `tools/queryproof_transport_confirmation_20261019/`
- `.github/workflows/queryproof-certificate-boundary.yml`

All runs use official public GitHub Actions runners, Java 17, Python 3.11
and released `pyspark==4.2.0`. There is **no self-hosted Oracle VM
runner or private network linkage**, and the unrelated upstream Spark
PR #59445 is untouched.
