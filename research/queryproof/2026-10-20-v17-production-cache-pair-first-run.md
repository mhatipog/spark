# QueryProof V17 — exact cross-batch source-level cache pairing (first run, 2026-10-20)

## Frozen original result

- Branch: research/queryproof-phase-local-independent-20261020
- Source: tools/queryproof_posthoc_v17_20261020/QueryProofNativeV17StockOptimizerCacheJoin.scala
- Workflow: .github/workflows/queryproof-native-v17-stock-optimizer-cache-pair.yml
- First run: https://github.com/mhatipog/spark/actions/runs/39482283878
- Job 118833636329: SUCCESS, first attempt.
- Official Apache Spark 4.2.0 PySpark distribution with the exact source-pinned disposable V10/V11/V14 event observer overlays. Stock Optimizer.defaultBatches and stock PropagateEmptyRelation production rule. Not uninstrumented upstream binary. Catalog-free plan; Optimizer(null) {}.

## Executable asserted observations

Plan: Repartition(2, shuffle=true, LocalRelation(Nil)); initially no ROOT_REPARTITION tag.

- QUERYPROOF_V17_EXACT_BATCH_ATTRIBUTED_TAG_SEQUENCE=SET,UNSET|SET
- QUERYPROOF_V17_EXACT_ROOT_RULE_CACHE_PAIR_COUNT=1
- QUERYPROOF_V17_MATCHED_RULE_ID=112
- QUERYPROOF_V17_MATCHED_ROOT_JVM_ID=2
- QUERYPROOF_V17_IN_ONE_UNMODIFIED_BATCH_SCHEDULE=true
- QUERYPROOF_V17_IDENTITY_AND_AST_UNCHANGED_BUT_TAG_PRESENT
- QUERYPROOF_V17_NO_SQL_BAG_DIFFERENCE_OR_SPARK_BUG_CLAIM

Unlike V16's global MARK/SKIP counts, V17 parses actual V11 RuleExecutor begin/end markers for the production PropagateEmptyRelation invocation in each of the two default batches. It insists the early span contains a ROOT_REPARTITION SET then UNSET on Repartition with transitions false->true->false; the later span must contain SET false->true with no UNSET; and an actual TreeNode V14 ineffective-rule MARK inside the early span pairs to a SKIP inside the late span using exact (rule ID, JVM node identity). It asserts final identical JVM root, identical structural JSON, and persistent ROOT_REPARTITION tag.

Scope: This is strong, precise, executable evidence that unchanged Catalyst structural JSON is not a complete description of optimizer transition state. The rule-cache and tag mutation are source-observed, not hypothesized or test-induced. Rule ID and node ID are JVM/run-local. The witness is a benign empty-relation repartition plan and establishes no SQL bag mismatch, no unbounded semantics theorem, and no incorrect upstream Spark optimization. Missing other state paths still report UNKNOWN, so neither Y nor V17 validates original W32 X guard. Frozen W32 16/32 structural and X 0/16 strict statuses must not change.

## Implications for proof design

Source-state claims need (1) a declared cutpoint, (2) per-invocation source-level effect coverage, (3) read-dependence closure for the remaining rule suffix, (4) a proof that each enabled state-dependent transformation preserves observable bag semantics, and (5) an untrusted-certificate checker or Coq theorem about the replay relation. A hash of structural JSON and a partial tag write log alone are insufficient.

## Related published work: disjoint scope, avoid novelty overclaims

- SQLSolver, Ding et al., Proving Query Equivalence Using Linear Integer Arithmetic, Proc. ACM Manag. Data / SIGMOD 2023: https://www.cs.yale.edu/homes/piskac/papers/2024SIGMOD.pdf — unbounded algebraic equivalence of query pairs, 346/359 on a different Calcite/Spark-derived benchmark; not a stock Spark optimizer internal-state trace benchmark.
- VeriEQL, He et al., OOPSLA 2024: https://pinhanzhao.com/papers/oopsla24.pdf — SMT-bounded equivalence, rich SQL features and constraints, not soundness of the actual Spark cache/tag instrumentation.
- Alive2, Lopes et al., PLDI 2021: https://users.cs.utah.edu/~regehr/alive2-pldi21.pdf — translation validation of LLVM optimizations; foundational idea, not an invention of QueryProof.
- RuleScript, An Extensible and Verifiable Language for Query Rewrite Rules, preprint May 2026: https://arxiv.org/html/2605.05536v1 — verifies engine-agnostic declarative rewrite rules; its adapter/interpreter is explicitly a trusted component, a distinct implementation gap that QueryProof may seek to close for existing Spark executions.
- Null SQL formalization with Coq: https://link.springer.com/article/10.1007/s10817-022-09632-4

No apples-to-apples end-to-end performance or coverage claim can presently be made against any of these systems.

## Next

Formalize a minimal effect-indexed two-batch machine with tagged node and per-node rule pruning, then derive and mechanize the paired MARK->SKIP transition. Separately implement read-side instrumentation; write-only traces are not a semantic frame certificate. Revisit 16 originally unsupported W32 shapes on a separately preregistered, matched-SQL evaluation protocol.
