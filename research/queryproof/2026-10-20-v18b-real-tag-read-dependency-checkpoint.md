# QueryProof V18B — native source-level tag-read dependence, corrected retry (2026-10-20)

## Immutable first-run history

- V18 ORIGINAL first attempt https://github.com/mhatipog/spark/actions/runs/39483141760 FAILED solely from a mismatched Scala object/launcher name, before native experiment execution; archived as 2026-10-20-v18-first-run-infrastructure-failure.md. Do not relabel it.
- V18B corrected code commit c4bd6e55d674ea29b9350a7901d1fd50dd1a618f (one object-name fix).
- V18B first actual native experimental execution https://github.com/mhatipog/spark/actions/runs/39483370459, job 118837404507: SUCCESS.
- Same pinned Spark 4.2.0 source, V10/V11/V14 disposable overlays, and additional V18 getTagValue read observer; normal stock Optimizer.defaultBatches and PropagateEmptyRelation. No upstream code changed in the permanent Git worktree.

## V18B asserted native trace

- QUERYPROOF_V18_OBSERVER_ISOLATED_SOURCE_READ_HOOK_READY
- QUERYPROOF_V18_EXACT_BATCH_ATTRIBUTED_TAG_SEQUENCE=SET,UNSET|SET
- QUERYPROOF_V18_EXACT_ROOT_RULE_CACHE_PAIR_COUNT=1
- QUERYPROOF_V18_MATCHED_RULE_ID=112
- QUERYPROOF_V18_MATCHED_ROOT_JVM_ID=1
- QUERYPROOF_V18_REAL_ROOT_READS_EARLY=1
- QUERYPROOF_V18_REAL_ROOT_READS_LATE=0
- QUERYPROOF_V18_READ_NODE_ID_EQUALS_CACHE_PAIR=1
- QUERYPROOF_V18_OBSERVER_INDUCED_READS_MUTED=true
- QUERYPROOF_V18_IN_ONE_UNMODIFIED_BATCH_SCHEDULE=true
- QUERYPROOF_V18_IDENTITY_AND_AST_UNCHANGED_BUT_TAG_PRESENT
- QUERYPROOF_V18_NO_SQL_BAG_DIFFERENCE_OR_SPARK_BUG_CLAIM

The source-level getTagValue hook catches the single production read of ROOT_REPARTITION (present=true) on the same root in the early PropagateEmptyRelation invocation, which also unsets it. In the later batch, the production rule sets it again but the exact same node and rule ID are rejected by the real cached ineffective-rule pruning bit, preventing the read (0 observed). The root instance and structural JSON remain identical and its final hidden tag is present. This is a stronger causally ordered source trace than V17: WRITE, READ, UNSET, MARK; then WRITE, SKIP, with exact source-batch boundaries.

Observer design explicitly isolates synthetic reads introduced by V10's own setTagValue/unsetTagValue before/after-state bookkeeping. Those artificial containsTag reads are muted with a ThreadLocal sentinel. Without this isolation a naive instrumented read count would not reliably describe real source-rule behavior. The V18 read observer tracks only ROOT_REPARTITION via getTagValue on TreeNode, and does not prove coverage of every hidden-read site, config, cache, metadata path, or Spark version.

## Boundaries not crossed

- NO incorrect SQL row-bag result demonstrated.
- NO upstream Spark bug, security issue, or broken RuleId contract demonstrated.
- NO matched W32 result, sound full state guard, or Catalyst-to-Coq soundness bridge established.
- The input is a deliberately small catalog-free Repartition(LocalRelation(Nil)), executed with Optimizer(null) {} to run actual stock default batch logic. This is a source instrumented packaged Spark 4.2.0 runtime, not a full SparkSession SQL job.
- Local rule/cache identity numbers have meaning only within this CI JVM.
- W32 historical first 16/32 structural and X first 0/16 strict remain immutable.

## Suggested next round

Generate an exhaustive, fail-closed read/write/support inventory for the actual W32 suffix rule families, beginning with ColumnPruning and ConstantFolding. Prove a sound, kernel-checkable effect-indexed semantic congruence rather than either bytewise state equality or write-only observation. Prove actual runtime-to-formal AST mapping or classify UNKNOWN. Freeze a new prospective test set before counting any fresh certification increase.

Primary official source: https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PropagateEmptyRelation.scala, https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala.
