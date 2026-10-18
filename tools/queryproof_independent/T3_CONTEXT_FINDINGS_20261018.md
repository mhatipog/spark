# QueryProof: SQL three-valued observer-effect model (18 October 2026)

**Result:** A small runnable conservative finite-context model and 7 standard-library tests were committed and verified in [GitHub Actions 39103287053](https://github.com/mhatipog/spark/actions/runs/39103287053). Code: [t3_context.py](t3_context.py) and [test_t3_context.py](test_t3_context.py).

This is an **in-sample demonstration of established semantics** from the prior SPARK-58384 / DuckDB #23685 known issue family, **not a new theorem, new issue or proof of Spark optimizer soundness**. No original upstream code changed.

## Tested contract

The problematic local substitution is:

    P(a,b) = (a=b) OR (a IS NULL AND b IS NULL)
    Q(a,b) = a IS NOT DISTINCT FROM b

For a=0 and b=NULL, P=NULL and Q=FALSE. Thus:

- Under a direct WHERE/ON predicate observer, both select precisely the same rows: equivalence of the TRUE-set.
- Under a projected Boolean value observer, they are **not** equal: SQL must expose NULL vs FALSE.
- Under NOT, the same inputs yield NOT(P)=NULL vs NOT(Q)=TRUE: the rewritten expression now causes an extra row to pass.
- Under IS NULL, P IS NULL=TRUE versus Q IS NULL=FALSE.

A parent operator induces an *observation contract*: TRUE_SET when only truth of the enclosing positive predicate matters; FULL_3VL when the expression's exact three-valued result matters. Once FULL_3VL is needed, every descendant must honor it. A positive AND/OR can preserve the weaker TRUE_SET obligation for a TRUE-only outer observer; NOT, IS NULL, arbitrary UDF/scalar context must upgrade to FULL_3VL.

The finite test domain `{0,1,NULL}` is complete **only for this equality/nullness rewrite**, because the distinctions are equal, unequal and NULL; it is not a general bounded-model theorem for arbitrary SQL operators.

## Independent controls and measured observations

- 7 unit tests passed: proper null propagation under NOT/IS NULL; filter true-set equivalence; full-value counterexamples; exhaustive nine pair values × three sibling truths for positive AND/OR; safe vs unsafe observer paths; fail-closed unsupported operators.
- Four-version DuckDB control: [39102536396](https://github.com/mhatipog/spark/actions/runs/39102536396).
- Exhaustive 33 named optimizer-pass ablations: [39102730092](https://github.com/mhatipog/spark/actions/runs/39102730092): only disabling `expression_rewriter` restores correct truth values and join result.
- The original DuckDB issue [#23685](https://github.com/duckdb/duckdb/issues/23685) was closed July 20, 2026; main-branch fixes are [bc320735](https://github.com/duckdb/duckdb/commit/bc32073585c733c96150c868610caaa6922853e2) and [cc71908](https://github.com/duckdb/duckdb/commit/cc71908bf28e10d8e12e253d8052678d4ba36b05). Correct in-source fix exists; released 1.5.6 still lacks it. Do not file a duplicate new-bug report.
- Spark's SPARK-58384 bug and upstream patch already address the analogous issue, so this model adds no newly reported defect.

## Literature boundary

Existing database-testing literature includes SQLancer NoREC, TLP and DQP, and the 2026 ICDE paper **A Set-Theoretic Approach to Detecting Logic Bugs in DBMS Inner Join Optimizations** (JoinEquiv, Ce Lyu et al.), which explicitly benchmarks equivalent relational transforms against TLP and DQP. 2025 PLDI **Optimization-Directed Compiler Fuzzing for Continuous Translation Validation** (Optimuzz) addresses compiler translation-validation bugs with directed fuzzing. The recent 2026 CIDR work by Narasayya and Chaudhuri leverages DBMS optimizer capabilities for rewrite equivalence. We have NOT executed these systems or obtained quantitative superiority evidence.

Relevant external resources:
- https://sqlancer.github.io/impact/papers/paper_doi_10_1109_icde65706_2026_00224/
- https://doi.org/10.1145/3729275
- https://www.vldb.org/cidrdb/2026/leveraging-query-optimizers-to-verify-the-soundness-of-llm-based-query-rewrites-for-real-world-workloads.html

Our plausible distinct direction, if measurable, is **automatic provenance-aware semantic obligation extraction across Catalyst optimizer rewrite and AQE boundaries**, providing concrete small counterexamples with explicit UNKNOWN when outside scope. This is a hypothesis requiring an actual automated extractor, not something proven by the current standalone model.

## Next publication go/no-go

1. Freeze a *new*, non-overlapping collection of Spark query-correctness issues that has not been used in developing this model. Make issue selection and excluded training families public before executing.
2. Extract Boolean observer paths directly from analyzed Catalyst expression trees and from a sampled rewrite trace. Do not infer effects from strings or hand-entered SQL. Support three-valued predicate contexts, tuple/null subquery semantics and empty aggregate cardinality; unsupported expressions abstain.
3. Match each witnessed wrong value to an independent finite-domain source-rows evaluation **without** asking the optimizer to rewrite an equivalent SQL oracle, where this is feasible.
4. Run actual comparator tools (SQLancer TLP/DQP where supported) or accurately scope any independent reimplementation, using the **same bugs and budget**, and include test execution and runtime overhead.
5. Primary metric: distinct, independently reproduced issue families detected with no verified false accusations and an explicit UNKNOWN denominator, not repeated execution count. A meaningful gain over a fair baseline or a new upstream-relevant defect is essential to claim a publishable scientific contribution.

### Unresolved issues

The earlier untouched automatic ORDER BY and top-k extractors detected 0/3 retrospectively reproduced independent wrong-result cases. The separate settings-differential test detected 1/3, and the independent DuckDB engine confirmed two of three original expected answers while also exposing its own *already documented* expression rewrite bug. The 0/3 remains our honest generalization baseline.
