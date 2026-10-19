# QueryProof research checkpoint — compositional Catalyst certificates (2026-10-19)

## Executive result

A third independently designed and frozen Spark 4.2.0 SQL cohort confirms
**11 exact, independent SQLite-verified, compositional certificates among
147 observed changed RuleExecutor transitions**, on 18/18 executable queries
with zero Spark-versus-SQLite row-multiset mismatches. The verifier was frozen
before this new cohort was executed. This is a verified **narrow proof-system
coverage gain**, not evidence of a newly discovered Spark optimizer defect.

## Three currently implemented certificate families

1. Predicate-only `Filter`/exact `Inner Join` rewrites: exact equality/NULL
   orbit reasoning from the October 18 base implementation.
2. Redundant side-`Filter` insertion beneath unchanged INNER JOIN: establish
   that the existing join condition being `TRUE` entails each newly inserted
   side guard also being `TRUE`. Original child subtrees and join metadata
   must match exactly. Development proof and new prospective confirmation
   completed earlier on October 19.
3. `Filter` commutation through an identity/renaming-only `Project`, also
   lifted through **unchanged pure `Project` ancestor frames**. Output
   expression IDs are mapped to source IDs; no arithmetic, casts, UDFs,
   nondeterministic computations or unrecognized types qualify.

### Exact projection transport theorem

Let `R` be a finite row *bag*, `E` a deterministic projection consisting
only of direct column references and pure `Alias(AttributeReference)`
renamings, `P` a predicate on the projected output, and `P_E` the same
predicate with expression IDs substituted along `E`. For any output row y:

```
mult_(Filter(P, Project(E, R)))(y)
  = Σ_[r:E(r)=y] mult_R(r) · 1[P(E(r)) = TRUE]

mult_(Project(E, Filter(P_E, R)))(y)
  = Σ_[r:E(r)=y] mult_R(r) · 1[P_E(r) = TRUE].
```

These are equal for all finite input bags whenever `P(E(r))=TRUE` iff
`P_E(r)=TRUE` for every possible input valuation. We prove that condition
by complete Bell(n+1) equality/NULL orbit enumeration for supported nullable
integral attributes (at most seven distinct IDs) and verify every orbit
independently in SQLite.

### Compositional pure-Project frame theorem

Given equal row bags `R1`, `R2` with unchanged output ID schema and a pure
direct/renaming-only projection `E` with unchanged serialized metadata:

```
R1 ≡bag R2  ==>  Project(E, R1) ≡bag Project(E, R2).
```

Both sides' output multiplicities are the same sum over each E-preimage.
The implementation may strip one or more **unchanged, pure** `Project`
ancestors and invoke either of the already-proved structural core
certificates. It rejects changed frames, unknown output scopes, duplicate
join output IDs, and opaque expressions. No rule-name whitelist is used in
the proof. The rule name appears only in sanitized reports.

## Measured results and independent status

| Corpus | New SQL cases | Changed Catalyst transitions | Direct projection swaps | Project-framed certificates | Row-bag mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original 2026-10-18 frozen development corpus | 18 | 97 | 3 (retrospective) | 0 (retrospective) | 0 |
| Earlier 2026-10-19 join confirmation corpus | 18 | 113 | 4 (retrospective for projection theorem) | 5 (retrospective) | 0 |
| New 2026-10-19 prospective direct-projection cohort | 18 | 140 | **3 (prospective)** | 7 (retrospective later extension) | 0 |
| New 2026-10-19 prospective compositional cohort | 18 | 147 | 0 | **11 (prospective)** | 0 |

These certificate counts are **distinct event rewrites** within the new
structural families, but avoid adding counts blindly across rows/versions
or conflating projected and join-guard certificates. The direct-projection
cohort was frozen against commit
`4ce1921f8f8a643cb50c0eead67039e8914525d7`; its prospective
execution anchor was
`1470a7d239d6e57c5f9f6caf1e3b1dfd15c980aa`.
The new compositional verifier was frozen at
`46dfae57c2a47ca0bee0addeff0ee9a982adc599`, **before** freezing
and running the new fourth cohort at
`5462991dcf7d53f1ee99b9c791943de3a3f2bcb3`.

All eleven accepted fourth-cohort certificates occur in genuine
`PushDownPredicates` optimizer transitions (cases C01, C02, C03, C05,
C08, C09, C10, C12, C15, C16, C18). Each uses exactly one unchanged
`Project` outer frame, independently checked via SQLite. C18 originated
from a LEFT OUTER JOIN SQL query, but its local accepted rewrite satisfies
the strict checked *internal* core; the query's surface join spelling is not
the rule's proof premise. No violation of outer-join semantics is inferred.

Both GitHub-hosted CI jobs passed on each of these runs:
- Direct projection theorem, 10 new adversarial unit tests:
  https://github.com/mhatipog/spark/actions/runs/39303005005
- Prospective direct-projection confirmation:
  https://github.com/mhatipog/spark/actions/runs/39303261332
- Compositional development replay (8 more adversarial unit tests):
  https://github.com/mhatipog/spark/actions/runs/39304144242
- New compositional prospective confirmation:
  https://github.com/mhatipog/spark/actions/runs/39304340331

The public GitHub Actions workflow runs in disposable GitHub-hosted
`ubuntu-latest` containers with official `pyspark==4.2.0`, a locally
instrumented disposable Catalyst wheel and temporary raw trace directory.
The raw serialized Catalyst plans are **not published**. No self-hosted
runner, Oracle VM or paid external capacity is attached.

## Research judgment and next frontier

This programme now verifies nontrivial structural optimizer steps,
not merely predicate algebra. Its certifiers are conservative and
compositional, with exact finite semantics inside the supported
grammar and independent validation.

However, 11/147 is **coverage over changed RuleExecutor events in
template-generated queries**, not a Spark-wide correctness rate, a
random industrial workload benchmark, or evidence that the remaining 136
events are incorrect. Two consecutive independent positive confirmation
cohorts are encouraging, but the test families are design-informed and
cannot establish global generalization.

The next mathematically natural certificate is **complete selection
transport through INNER JOIN**:

```
Filter(P, Join_Inner(C, L, R))
  -> Join_Inner(C', Filter(G_L, L), Filter(G_R, R)).
```

For every row pair `(l,r)` the proof obligation is equality of the
truth indicator for `C AND P` versus `C' AND every G`. Because the
pair multiplicity is `m_L(l) m_R(r)` times that indicator, the same
orbit/SQLite machinery can certify this transportation exactly. Any new
proof must be developed and confirmed against **a fifth independently
frozen corpus**; re-testing old corpora only counts as exploratory.

## Files

- `tools/queryproof_context_validator/equality_orbits.py`
- `tools/queryproof_context_validator/join_prefilter_certificate.py`
- `tools/queryproof_context_validator/project_pushdown_certificate.py`
- `tools/queryproof_context_validator/compositional_project_frame_certificate.py`
- `tools/queryproof_context_validator/test_project_pushdown.py`
- `tools/queryproof_context_validator/test_compositional_project_frame.py`
- `tools/queryproof_project_confirmation_20261019/`
- `tools/queryproof_compositional_confirmation_20261019/`
- `.github/workflows/queryproof-certificate-boundary.yml`

Do not conflate these research commits with upstream Spark PR #59445,
which is unrelated and unchanged.
