# QueryProof out-of-design join corpus: first independent-score checkpoint

**Date:** 2026-10-19. **Baseline execution:** [GitHub Actions run 39306856668](https://github.com/mhatipog/spark/actions/runs/39306856668), external-postgresql-join-generalization job, commit `93d2a6d2b1536f20fffde5a96f0a044402a1f079`.

This is the **first** execution of the source-frozen corpus from
`tools/queryproof_external_postgresql_join_20261019/CASES.json`.
The five theorem implementations were unchanged from frozen verifier commit
`35314636b8d0ce04d922a3df0f2fa72c0974325c`.

## Actual measurements

| Measurement | Observation |
| --- | ---: |
| Queries executed | 18/18 |
| Spark version | 4.2.0 |
| SQLite version | 3.45.1 |
| Disagreements between Spark and SQLite row multisets | 0 |
| Catalyst trace events | 91 |
| Certified by any one of five proof families | **0** |
| UNKNOWN by all five proof families | **91** |
| Local counterexample statuses | 0 |
| AST decode/instrumentation failures | 0 |

By recorded event label:

| Label | Events | Certified |
| --- | ---: | ---: |
| Analyzer (stage-level) | 46 | 0 |
| Optimizer (stage-level) | 18 | 0 |
| InferFiltersFromConstraints | 11 | 0 |
| CollapseProject | 10 | 0 |
| PushDownPredicates | 3 | 0 |
| ColumnPruning | 1 | 0 |
| DeduplicateRelations | 1 | 0 |
| EliminateOuterJoin | 1 | 0 |
| **Total** | **91** | **0** |

A strict secondary denominator excludes `Analyzer` and `Optimizer`
stage-labelled events: **0/27 named-rule events** certified. Stage-labelled
events are part of the prior instrumentation's captured event universe but
must not be rhetorically described as 64 individual rewrite rules.

## Interpretation

This falsifies any suggestion that QueryProof's five developed theorem
families **already** achieve broad coverage of a PostgreSQL-derived join
regression sample. No SQL row error was found. A 0% coverage observation
is an abstention, **not** 91 optimizer bugs. In-scope theorem soundness
is not refuted merely by low coverage; the generalization narrative is
strongly constrained.

This sample includes `USING`, `NATURAL`, `CROSS`, non-equi, and outer join
structures that can produce nontrivial plan shapes. It is still not a
representative production sample and the SQL source is Spark's own port of
PostgreSQL tests, not a wholly separate SQL engine's native workload.

## Preserved scientific boundary

The baseline measurements above were scored before any analysis of unknown
shape diagnostics. Subsequent use of this exact 18-query corpus is
**retrospective development**, even if we only add logging. Any new theorem
inspired by this sample requires a *new, frozen, unseen* validation cohort
for prospective evidence.

The next planned research step is sanitized post-hoc shape analysis of
`InferFiltersFromConstraints`, `CollapseProject` and
`PushDownPredicates` (24/27 named-rule events) to identify a smallest
sound compositional extension. It must preserve exact bag semantics
(including outer joins, duplicate multiplicities and NULL) and guard
against expression changes, not merely assert structural similarity.
