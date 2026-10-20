# QueryProof V14: native source-level rule-cache explanation of failed V13 replay

Date: 2026-10-20. Branch:
research/queryproof-phase-local-independent-20261020.
Owner: Metin Hatipogullari.

**First V14 source implementation commit:** 1ca637922f7de78a6190c94567931dd9fc911581.
**Preserved first native CI run:** [39479932112](https://github.com/mhatipog/spark/actions/runs/39479932112);
job 118825530826, success. All source pinning, Scala compilation,
RuleExecutor overlay and native source-effect assertions succeeded.

## Source and question

Apache Spark 4.2.0 TreeNode.scala has a private mutable
_ineffectiveRules BitSet for each node. markRuleAsIneffective
writes it, while isRuleIneffective can suppress subsequent
tree traversal, even when serializations of the plan remain
unchanged. Compare:
[Apache Spark v4.2.0 TreeNode.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala).

V13 [first failed native run 39476441071](https://github.com/mhatipog/spark/actions/runs/39476441071)
generated a tag via actual ConstantFolding but failed its
hypothesis that unsetting the tag on the *reused Project*
would cause a fresh failure. The cause had not been observed
directly before V14.

## Native methods

1. Pin upstream Spark 4.2.0 TreeNode.scala Git blob SHA
   e82e6a30b9bba41e637b27cbab43f26d9d23ff9b, then patch
   only an ephemeral source copy: V10, V11, V14.
2. V14 injects a separate source-site observer into actual
   TreeNode.markRuleAsIneffective and isRuleIneffective. Logs
   only real MARK and true SKIP events with exact JVM-local
   node identity, real rule ID, node class, query ID.
3. Native ConstantFolding on a conditional bad ANSI Cast
   legitimately sets FAILED_TO_EVALUATE; create an
   unconditional Project containing that same tagged Cast.
   The test-constructed move is not a normal Spark rewrite.
4. First invocation of ConstantFolding on this Project
   succeeds and source observer records ineffective-rule MARK
   on that Project. Clear the Cast tag *outside* the rule
   scope; V11 correctly reports UNKNOWN, while toJSON remains
   identical.
5. Run a new RuleExecutor instance over that same Project.
   Source observer records SKIP of the same actual Project
   for the same rule ID; optimizer does not try the invalid
   unconditional ANSI Cast.
6. Construct a **fresh Project and Cast** with exactly
   matching toJSON and Alias ExprId but no cached rule bit.
   The optimizer attempts unconditional ANSI Cast evaluation
   and throws, as the separate V13B [run 39476960456](https://github.com/mhatipog/spark/actions/runs/39476960456)
   had independently found.

## Actual first run success markers

- QUERYPROOF_V14_PRODUCTION_FAILED_TAG_GENERATED
- QUERYPROOF_V14_REAL_PROJECT_RULE_CACHE_MARK_OBSERVED
- QUERYPROOF_V14_TAG_UNSET_INVISIBLE_TO_STRUCTURE_UNKNOWN_EVENT
- QUERYPROOF_V14_SAME_PROJECT_SAME_RULE_CACHE_MARK_THEN_SKIP
- QUERYPROOF_V14_FRESH_IDENTICAL_AST_NOT_PRUNED_THREW
- QUERYPROOF_V14_V13_FIRST_RUN_CACHE_CONFOUND_EXPLAINED
- QUERYPROOF_V14_TEST_ONLY_CONTEXT_RELOCATION_NO_SPARK_WRONG_ANSWER_CLAIM

The exact identity-and-rule matching is independently asserted
inside a native Scala witness against actual source-site events.
Thus V13's failure was explained, not silently removed, and
a second hidden-state source was verified.

## Scope, caution, next

The source cache is actual production Spark logic, but
moving the Cast out of its conditional context and unsetting
its failure tag are test-only interventions. V14 does NOT
demonstrate any real optimizer wrong-answer bug or row-bag
inequivalence. It verifies optimizer control-flow dependence
on mutable state invisible to structural JSON.

Existing QueryProof strict X still has 0/16 accepted W32
structural candidates. W32's prospective first-run remains
16/32 structural completions and 32/32 finite row-bag
matches. V14 is a posthoc evidence-boundary finding.

Next: seek actual production rules that set and unset their
own tags, replay them across optimizer invocations, and test
cache/tag interactions without synthetic changes. Build a
unified semantic effect model for tags and pruning caches,
then an independently checked Spark AST-to-Coq bridge.

## Additive files

- tools/queryproof_posthoc_v14_20261020/instrument_rule_pruning.py
- tools/queryproof_posthoc_v14_20261020/QueryProofNativeV14RulePruningCacheWitness.scala
- .github/workflows/queryproof-native-v14-source-rule-cache.yml
