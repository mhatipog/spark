# QueryProof cross-engine three-valued-logic investigation (18 October 2026)

## Finding, correction of novelty, provenance

This investigation began from existing frozen Spark bug case B01 (SPARK-58384), not an unexplored SQL query. It re-established a semantic oracle using Python SQL three-valued logic and SQLite, found a surprising DuckDB disagreement, and isolated the responsible DuckDB optimizer stage through an exhaustive individual-rule ablation.

**The DuckDB bug is NOT new.** It was previously reported as duckdb/duckdb issue #23685 on 8 July 2026 and fixed in the main development branch by Feng Wu's commits bc320735 (20 July) and cc71908 (20 July). The published DuckDB Python releases 1.3.2, 1.4.1, 1.4.5, and 1.5.6 still exhibit it in our October 18 executable tests. This is a version/backport gap, not grounds for a duplicate new-bug issue.

- Frozen protocol: FROZEN_DUCKDB_JOIN_PROTOCOL.json
- Cross-version execution: https://github.com/mhatipog/spark/actions/runs/39102536396
- Frozen pass-isolation protocol: FROZEN_DUCKDB_RULE_ISOLATION.json
- Independent 33-pass one-rule-at-a-time validation: https://github.com/mhatipog/spark/actions/runs/39102730092
- DuckDB known issue: https://github.com/duckdb/duckdb/issues/23685
- DuckDB main fix: https://github.com/duckdb/duckdb/commit/bc32073585c733c96150c868610caaa6922853e2 and https://github.com/duckdb/duckdb/commit/cc71908bf28e10d8e12e253d8052678d4ba36b05
- Exact affected released DuckDB source: https://github.com/duckdb/duckdb/blob/v1.5.6/src/optimizer/rule/equal_or_null_simplification.cpp

All four tested DuckDB published versions returned four rows when optimizer enabled versus one when disabled. Independent Python and SQLite calculations agree on one. The DuckDB projection of the predicate over a Cartesian product ALSO differs from correct SQL three-valued semantics under optimization. In DuckDB 1.5.6, disabling ONLY expression_rewriter fixed both projected truth values and join membership; disabling each of the other 32 named optimizer passes individually did not.

### Formal reason

Let P(a,b) = (a=b) OR (a IS NULL AND b IS NULL). Let Q(a,b) = a IS NOT DISTINCT FROM b.

For a=0 and b=NULL:
- P = NULL under strong Kleene three-valued logic.
- Q = FALSE, even though P and Q accept the same input pairs when used as bare filter predicates.
- NOT P = NULL, while NOT Q = TRUE.

Thus P and Q are observationally equivalent under a *truth-only filter observer*, but not value-equivalent as Boolean expressions, and certainly not equivalent under NOT or IS NULL.

Two distinct obligations must be represented:
- T-SET preservation: all rows for which predicate is TRUE are preserved (sufficient for a direct WHERE/HAVING/ON predicate).
- FULL-T3 preservation: the complete TRUE/FALSE/UNKNOWN mapping must be preserved for scalar projections and all null-observing contexts.

The observer context is a semantic effect. Under a root positive AND/OR context, T-SET is sufficient; under NOT, IS NULL, scalar value positions, nontrivial comparisons and arbitrary functions, FULL-T3 is needed. This is a standard semantic distinction, NOT a newly invented theorem. What might be useful is an automatic cross-optimizer contract/effect analyzer with abstention rather than unsafe blanket substitution.

DuckDB main fix limits EqualOrNullSimplification to proper predicate-root contexts; it already instantiates the same principle. Apache Spark's known SPARK-58384 was fixed similarly (Spark 4.3.0 target), with an explicit AND/OR-only traversal in OptimizeJoinCondition. The old released Spark 4.0.1/4.2.0 wheels still reproduce it. Therefore no standalone new DuckDB bug, new Spark bug, or original mathematics can be claimed here.

### Still-open Spark opportunities (verified 18 October 2026)

- SPARK-58442: mandatory RewritePredicateSubquery collapses IN-subquery UNKNOWN to FALSE in observable contexts. Open upstream issue, existing PR https://github.com/apache/spark/pull/58186 (open as of check). Its last description notes limitations for correlated subqueries and value positions. We must inspect rather than duplicate the work.
- SPARK-58429: uncorrelated IN selected beside a global aggregate over empty input incorrectly loses a scalar result through First(exists). Existing open PR https://github.com/apache/spark/pull/57817. This is a separate boundary/cardinality bug, not fixed by a Boolean context guard.
- Our Spark upstream PR #59445 is separate (correlated projection LIMIT/OFFSET) and remains untouched.

## Strict research interpretation

The two already-fixed-in-main but unfixed-in-release expression bugs corroborate a useful general concern: local rewrites require context-sensitive semantic contracts. This contributes *motivation and evidence*, not scientific novelty. The independent-family benchmark showed 0/3 detection by the earlier order/TopK contracts and 1/3 by our configuration differential; these results remain fixed.

The highest-value next milestone is to construct a generalized T3 observer-effect extraction that detects soundness risks in different SQL optimizer rewrite families, then evaluate on **new, unseen issues** rather than optimize around known cases. Compare directly with SQLancer DQP, NoREC/TLP/CODDTest and report measured cost/unknown rate. Do NOT open duplicate bug reports or public claims of discovering #23685/SPARK-58384.

Note on the harness: the SQLite version of the one-pair VALUES alias query uses unsupported SQL syntax and its reported one_pair_null_mismatch_no_rows=false should be read as an unverified ancillary check, not a semantic counterexample; the full SQLite six-pair JOIN and truth table controls passed.
