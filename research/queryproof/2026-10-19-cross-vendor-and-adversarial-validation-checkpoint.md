# QueryProof Round 7 — cross-vendor confirmation and NULL/outer adversarial checkpoint

**Date:** 2026-10-19. Research fork: `mhatipog/spark`, branch
`research/queryproof-certificate-boundary-hardening-20261019`.

## A real out-of-development proof family, independently checked

The contextual INNER JOIN redundant-side-guard certificate is based on
an exact mathematical implication over the nullable integral equality
fragment (not a probabilistic check):

```text
For every valuation: C(left,right)=TRUE
  implies G_left(left)=TRUE and G_right(right)=TRUE.
```

This implication is necessary and sufficient to preserve the **multiset
of matching pairs for all possible input multisets** when one only inserts
the two side Filters beneath an otherwise unchanged INNER JOIN. Under the
same row-multiset model, unchanged deterministic Project frames drawn only
from attributes, direct aliases and literals preserve bag equality.
An unchanged Sort frame preserves bag equality but **not an ordered-sequence
claim**. Every accepted event checks actual old/new Spark Catalyst ASTs,
resolved source attribute identities, join type and side scope; it
exhaustively checks complete Bell(n+1) nullable equality orbits and repeats
the predicate implication independently in SQLite.

This family was implemented in
`tools/queryproof_context_validator/contextual_join_guard_certificate.py`
and fixed **before** either DuckDB validation corpus was executed.
The 17 new unit tests include adversarial controls that reject changed frame metadata,
non-deterministic projections, Coalesce, null-safe non-implications,
wrong-side filters, unresolved scopes, and true outer-join ASTs.

## Separation of discovery and confirmation

| SQL source / evidential status | SQL cases | Catalyst events | Named-rule events | New contextual proofs | Spark–SQLite row disagreements |
|---|---:|---:|---:|---:|---:|
| Spark's PostgreSQL-derived join suite — development, *post-hoc only* | 18 | 91 | 27 | **5** | 0 |
| DuckDB simple/USING/pushdown — **prospective independent vendor** | 18 | 129 | 59 | **9** | 0 |
| DuckDB NULL/duplicate/outer — **prospective adversarial holdout** | 18 | 100 | 46 | **7** | 0 |

**Independent prospective total: 16 certified / 105 named-rule events
(15.24%)**, or 16/229 (6.99%) when including stage-labelled events.
Within `InferFiltersFromConstraints`, it certified
**16 of 28 actual events (57.14%)** over the two independently frozen
DuckDB cohorts. No other rule family is presently claimed.

There were zero reported local proof counterexamples, zero AST decode
failures, and zero Spark–SQLite row-bag mismatches across all 54 queries.
These absences are consistency observations, **not** universal evidence
of zero false negatives or absence of Spark bugs.

The largest accepted DuckDB certificate checked **877 equality/NULL
orbits**, corresponding to six scoped integral variables (Bell(7)=877);
other accepted events required five or 52 orbits. Every accepted event's
SQLite valuation cross-check returned verified.

## Adversarial null/outer lesson

The adversarial sample used verbatim DuckDB `IS NOT DISTINCT FROM`
joins and original duplicate/NULL fixtures, plus LEFT/RIGHT OUTER and
nullable ON predicates. Seven events were certified, six in inner
self-join source queries and one from an SQL query originally written
as a LEFT OUTER JOIN followed by `WHERE k IS NOT NULL`. That query can
legally be transformed into an INNER JOIN upstream; the accepted
`InferFiltersFromConstraints` event's **actual Catalyst AST** was INNER.
The contextual checker rejects genuinely LEFT/RIGHT OUTER join ASTs.

The independent suites are join-oriented regression corpora, not random
production SQL. They use finite fixtures, and their query selection
does not represent arbitrary Catalyst optimizer operations. Original five
checkers individually accepted **zero** external-corpus events; new
contextual support was indispensable. Other visible rule families
(`CollapseProject`, `PushDownPredicates`, `ColumnPruning`, etc.)
remain overwhelmingly UNKNOWN rather than being guessed equivalent.

## Evidence and reproducibility

- Development baseline [run 39306856668](https://github.com/mhatipog/spark/actions/runs/39306856668), frozen 0/91.
- First independent DuckDB [run 39308854909](https://github.com/mhatipog/spark/actions/runs/39308854909), 9/129.
- Adversarial DuckDB [run 39309389699](https://github.com/mhatipog/spark/actions/runs/39309389699), 7/100.
- All source commit IDs, selected query ordinals, fixture rows, fixed
  protocols, worker code, and sanitizer are committed in
  `tools/queryproof_external_postgresql_join_20261019`,
  `tools/queryproof_external_duckdb_join_20261019` and
  `tools/queryproof_external_duckdb_adversarial_20261019`.
- Tests use only disposable GitHub-hosted runners with released
  PySpark 4.2.0, Java 17 and Python 3.11. Raw serialized Catalyst
  plans never appear in logs/artifacts; private temporary files are
  deleted before sanitised reports are written.

## Scientific recommendation

**Continue the research.** The improvement is genuine and independently
confirmed, but the next goal is *sound coverage expansion*, not a hasty
upstream Spark PR or unsupported claim of a new optimizer defect.

1. Build a separate exact certificate for deterministic
   `CollapseProject`: composition of identity/direct-alias/literal
   projections, preserving output ordering, attributes, NULL, and
   duplicate multiplicity. Prove each permitted expression composition
   symbolically before touching Spark traces.
2. Use a **new untouched SQL source**, ideally aggregate/subquery/window
   queries as well as join regression, to measure coverage and abstentions
   without picking favorable cases.
3. Publish per-proof latency and finite-harness throughput, plus explicit
   type/operator boundaries. Distinguish asymptotic Bell-orbit cost from
   measured Spark overhead; do not infer a real-world performance figure
   from the total CI job duration.
4. For any new proof family, preserve the two prospective DuckDB cohorts as
   **historical confirmation only** if its implementation was motivated by
   their shapes. Preregister another independent cohort.
