# QueryProof: external PostgreSQL-derived join workload (frozen design)

**Date:** 2026-10-19. **Study:** out-of-design generalization of unchanged QueryProof exact certificates.

## Why this is a genuinely different test

Our five prior suites were written around candidate proof families. This cohort uses
18 **unchanged** SQL SELECT statements from [Apache Spark's PostgreSQL join
regression input](https://github.com/apache/spark/blob/b2c6f51268d7b94b0f82096262fc5381cd9cf0f9/sql/core/src/test/resources/sql-tests/inputs/postgreSQL/join.sql),
originally derived from the PostgreSQL regression suite. We pin the upstream
source commit and copy the two original test tables' exact 11 and 9 inserted
rows, including NULLs and duplicate records.

The selection was locked **before any QueryProof execution** and without
examining optimizer ASTs. Among SELECTs between `CORRELATION NAMES` and
`More complicated constructs`, take the first 18 usable statements, in order.
Only skip source SQL requiring positional column-list relation aliases or
known-ambiguous unqualified references across the two tables. Exact 1-based
source SELECT ordinals are in `CASES.json`; SQL itself is not tailored to
the five certificate families. This is independent of our **development query
design**, but not independent of Apache Spark as a software test source.

## Primary metric

`certified_union / all_captured_optimizer_rule_transitions`.

For each event, call all five **unchanged** checkers and count a certified
event once if any checker reports `EQUIVALENT_IN_FRAGMENT`. Other transitions
are `UNKNOWN`. Report overlaps and raw per-family counts separately; do not
sum families to produce coverage. A counterexample from the narrow predicate
checker is not proof of reachable wrong Spark results.

The SQLite oracle compares the **multiset**, not order, of all query results
against released PySpark 4.2.0, with per-query hash/counts. SQLite agreement
is a finite consistency check; universal exactness still rests entirely on
each accepted fragment certificate's own mathematics.

## Source and verification controls

- Frozen verifier: `35314636b8d0ce04d922a3df0f2fa72c0974325c`.
- Python/Java/Spark runner lives only in `tools/queryproof_external_postgresql_join_20261019`.
- The source material, eligibility criteria, and 18 selected query ordinals
  are committed **before** running the instrumented Spark optimizer.
- Fail CI on missing/errored queries, row-oracle disagreements, incomplete
  traces, or mismatched event denominators; **do not** fail merely for zero
  certified events.
- No raw Catalyst AST or raw Java stderr goes into CI logs/artifacts; they
  are deleted with a temporary directory. Only shape-only diagnostics and
  salted-free per-case hashes of synthetic benchmark output are archived.
- No changes to the five theorem implementation modules, their 42 tests, or
  existing five prospective/development suites.

## Interpretation rules

High coverage would show generalization *to this narrow join regression
sample*, not arbitrary Spark correctness. Low coverage is valuable: it
identifies exactly which rules/shapes the certifier conservatively refuses.
All join regression SQL may differ sharply from real production queries.
The next source should therefore cover an unrelated engine's SQL corpus,
with aggregation/window/subquery cases and a fully specified cross-dialect
eligibility filter. Do not tune the checker on this frozen cohort while
describing later measurements as independently prospective.
