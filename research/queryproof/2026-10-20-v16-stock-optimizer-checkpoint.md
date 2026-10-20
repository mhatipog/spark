# QueryProof V16 — stock default-optimizer hidden-state witness (first run, 2026-10-20)

## Frozen first run

- Workflow: .github/workflows/queryproof-native-v16-stock-optimizer.yml
- Native source: tools/queryproof_posthoc_v16_20261020/QueryProofNativeV16StockOptimizerWitness.scala
- First CI run: https://github.com/mhatipog/spark/actions/runs/39481356949
- Job: 118830455916
- Conclusion: success on first attempt.
- Spark runtime: pip-distributed official Apache Spark 4.2.0; disposable source instrumentation from the exact pinned official TreeNode.scala SHA256 e82e6a30b9bba41e637b27cbab43f26d9d23ff9b, V11 rule scopes and V14 rule pruning observer. This is NOT an unmodified Spark runtime. Original optimizer and PropagateEmptyRelation rule behavior were not replaced.

## Test and actual native outputs

Input was one user-specified Repartition(2, shuffle=true, LocalRelation(Nil)).
Optimizer was new Optimizer(null) {} on this catalog-free input. The class's stock defaultBatches schedule was executed with optimizer.execute(root), not a hand-built two-rule replay. A null CatalogManager limits generality beyond this catalog-free plan.

- QUERYPROOF_V16_STOCK_DEFAULT_OPTIMIZER_TWO_PRODUCTION_BATCHES
- QUERYPROOF_V16_REAL_PRODUCTION_RULE_INVOCATIONS=2
- QUERYPROOF_V16_REAL_PRODUCTION_RULE_BATCHES=LocalRelation early|LocalRelation
- QUERYPROOF_V16_SOURCE_TAG_UNKNOWN_SET=2
- QUERYPROOF_V16_SOURCE_TAG_UNKNOWN_UNSET=1
- QUERYPROOF_V16_REAL_CACHE_MARKS=12
- QUERYPROOF_V16_REAL_CACHE_SKIPS=8
- QUERYPROOF_V16_ROOT_IDENTITY_PRESERVED=true
- QUERYPROOF_V16_ROOT_STRUCTURAL_JSON_PRESERVED=true
- QUERYPROOF_V16_ROOT_HIDDEN_TAG_AFTER=true
- QUERYPROOF_V16_STOCK_OPTIMIZER_EXECUTION_COMPLETE
- QUERYPROOF_V16_NO_ROW_BAG_OR_SPARK_BUG_CLAIM

The source-level tag is PropagateEmptyRelation.ROOT_REPARTITION (TreeNodeTag[Unit]); production addTagForRootRepartition sets it, production applyInternal may unset it for an empty child, and a real TreeNode rule pruning cache may cause later application to skip the same immutable-appearing root. The source observer reports this tag as UNKNOWN because V11 was intentionally scoped to FAILED_TO_EVALUATE. Counts (SET 2, UNSET 1) and final containsTag(true) were recorded in real native logs.

## Interpretation and scope

This is a genuine stock default optimizer execution producing distinct hidden state at the same unchanged structural JSON, with two real invocations of PropagateEmptyRelation in separate optimizer batches. It supersedes the artificial two-call schedule limitation of V15, but only as an observational state witness. It does NOT establish different output SQL row bags, incorrect query results, or an upstream Spark bug. The observed cache MARK/SKIP counts are across all rules/nodes; V16 did not demonstrate that every individual cache event belongs to PropagateEmptyRelation. No formal Spark-to-Coq soundness proof was obtained.

V16 also does not identify which, if any, original frozen W32 cases actually need this hidden state for query equivalence. W32's original 16/32 structural certificates, X's original 0/16 strict guard, and all prespecified statuses remain unchanged. The Y phase-local relaxed guard must not be promoted retrospectively.

## Next falsifiable test

Run a matched stock optimizer cold/fresh versus warm/reused input experiment with full per-rule and per-node cache logs. Capture both final tags and plans, explicit state-sensitive follow-on behavior, and investigate whether ROOT_REPARTITION survives a second full pass. A semantic difference must be proven with real row bags before any claims of incorrect Spark behavior.

## Authoritative source

Apache Spark 4.2.0:
- https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PropagateEmptyRelation.scala
- https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala
- https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala
