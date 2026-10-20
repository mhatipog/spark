# QueryProof V15: source-pinned, production-only ROOT_REPARTITION/cache interaction

Date: 2026-10-20. Research owner Metin Hatipogullari.
Posthoc branch research/queryproof-phase-local-independent-20261020.
**First implementation/workflow SHA:** ad1625ad078cce7a16f5d416a67650699954c03f.
**FIRST NATIVE CI RUN: [39480583070](https://github.com/mhatipog/spark/actions/runs/39480583070)**,
job 118827766348, PASS on first run. Preserve this provenance.

## What is different from V13/V14

No test-only tag injection, clearing or expression relocation.
Only Apache Spark 4.2.0 production PropagateEmptyRelation rule operates
on a root user-specified Repartition(2, true, LocalRelation(Nil)),
via genuine RuleExecutor invocations on the same plan instance.

Official code:
[PropagateEmptyRelation.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PropagateEmptyRelation.scala)
sets ROOT_REPARTITION in addTagForRootRepartition before
transformUpWithPruning, then unsets the tag for an empty child if
the root was already tagged. Production source:
[TreeNode.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala)
records rule-ineffective bits and skips a previously ineffective rule.

## Native method and observed sequence

1. Official TreeNode.scala source Git blob pinned to
   e82e6a30b9bba41e637b27cbab43f26d9d23ff9b.
   Disposable source patching through V10, V11 and V14; build
   actual Spark 4.2.0 catalyst plus RuleExecutor overlays in CI.
2. The genuine PropagateEmptyRelation production rule is called
   twice via fresh RuleExecutor instances, each with a single
   Once batch. The same exact plan object is deliberately reused.
3. First call: source observer records tag SET and UNSET, an
   ineffective-rule MARK on Repartition, and the root retains
   NO ROOT_REPARTITION tag. Original V11 observer correctly emits
   UNKNOWN_UNTRACKED_TAG_NAME on unsupported ROOT_REPARTITION.
4. Second call: production rule sets the tag, and its root
   transform sees the cached ineffective-rule entry. V14 source
   observer emits SKIP on the identical Repartition JVM node
   identity and rule ID. The clearing branch is not executed:
   ROOT_REPARTITION remains PRESENT.
5. Spark TreeNode.toJSON on that root is EXACTLY unchanged across
   the whole test. RuleExecutor returns a structurally unchanged
   root in both invocations.

Native CI markers:
- QUERYPROOF_V15_PRODUCTION_TAG_SET_UNSET_FIRST_PASS
- QUERYPROOF_V15_PRODUCTION_REPARTITION_RULE_CACHE_MARK
- QUERYPROOF_V15_PRODUCTION_SAME_ROOT_RULE_MARK_THEN_SKIP
- QUERYPROOF_V15_IDENTICAL_AST_DIFFERENT_HIDDEN_TAG_AFTER_REPLAY
- QUERYPROOF_V15_ORIGINAL_V11_DOMAIN_CORRECTLY_REPORTS_UNKNOWN
- QUERYPROOF_V15_NOT_ROW_BAG_MISMATCH_NOT_SPARK_BUG_CLAIM

## Why it matters and why not overstate

This is actual optimizer code setting/unsetting an actual tag
and interacting with actual rule cache; unlike V13B/V14 it does
not depend on test-only mutation or relocating an expression.
An identical structural plan can therefore have distinct
hidden state after different execution histories.

The test deliberately REUSES a plan in two single-rule optimizer
executions. That does not by itself prove a production application's
full optimizer follows this same path or returns wrong query rows.
ROOT_REPARTITION can deliberately remain set to safeguard a user's
partitioning request. No wrong-answer bug, bag inequivalence,
user-visible regression or security issue has been established.
Source proves only that the default Optimizer lists
PropagateEmptyRelation in both a "LocalRelation early" and a later
"LocalRelation" fixed-point batch; end-to-end occurrence needs
a separately run full-optimizer witness.

QueryProof V11 correctly marks ROOT_REPARTITION tag effects UNKNOWN.
This is a newly concrete reason to preserve fail-closed semantics
and explicitly account for causal internal state in certification.

## Frozen historical status

W32 CI 39435756545: 16/32 prospective structural suffix
certificates, prespecified 16 targets succeeded, 32/32 finite
Spark/SQLite row-bag agreement. Strict X CI 39436715938:
**0/16** accepted by original global hidden-state guard.
V15 does not reclassify those runs or relax any guard.

## Additive files

- tools/queryproof_posthoc_v15_20261020/QueryProofNativeV15ProductionRepartitionWitness.scala
- .github/workflows/queryproof-native-v15-production-repartition.yml

Next: run the stock default optimizer or an explicitly faithful
batch schedule, independently capture rule events and prune-state
transitions; build formal context-indexed effect/step semantics.
