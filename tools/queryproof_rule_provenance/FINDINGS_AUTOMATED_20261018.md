# QueryProof automated optimizer provenance — 18 October 2026

**Status: successful in-sample fault localization, not a general SQL correctness verifier.**

The first source-dependent SQL correctness benchmark, frozen October 16, found three independently reproduced wrong-result bugs B01/B02/B03; QueryProof's then-current order/TopK necessary-condition detectors detected **0/3**. That result is preserved unchanged.

We subsequently added separate source-derived interpreters for three-valued joins and nullable IN, an exact five-partition Boolean observer lattice, and rule-level trace instrumentation. An end-to-end pipeline has now been validated from synthetic SQL input to causal intervention evidence:

[Full pipeline GitHub Actions run 39106224959](https://github.com/mhatipog/spark/actions/runs/39106224959) — completed success with official PySpark 4.2.0 and a disposable compiled RuleExecutor overlay.

Stages:
1. Extract the analyzed Catalyst expression observation demands, such as FALSE_ONLY, NULL_ONLY or FULL_3VL.
2. Capture before/after full Catalyst JSON AST for every effective rule using the existing QueryProof RuleExecutor tracer.
3. Generate candidates by demand-aware structural losses **without reading expected answers or passing hardcoded rule names** to the intervention phase.
4. Take exactly one top candidate per case and run baseline, Spark SQLConf exclusion, and test-only forced rule-skip arms.
5. In a *separate last scoring step*, compare each outcome to the original frozen reference answers.

| Historical case | Auto-ranked first rule | Baseline | Force-skip result | Control |
| --- | --- | --- | --- | --- |
| B01 SPARK-58384 | OptimizeJoinCondition | wrong (4 rows) | correct (1 row) | N01 remains correct |
| B02 SPARK-58442 | RewritePredicateSubquery | wrong (0 rows) | correct (1 row) | N02 remains correct |
| B03 SPARK-58429 | RewritePredicateSubquery | wrong (NULL-valued scalar) | correct (TRUE) | N03 remains correct |

All three bad results were restored when suppressing the automatically implicated rule, and the three safe control outputs remained correct. For B01, standard SQLConf rule exclusion also sufficed; for B02 and B03, the rule was not effectively excluded by SQLConf, while the forced test-only rule skip did execute and the plan changed.

**Critical qualifier:** all six queries, including the safe controls, contained a structural candidate transition. Thus identifying the rule is NOT sufficient to accuse it of unsoundness. The causal test and independent result contract are essential; disabling a rule globally is NOT a production remedy.

The AST trace uses only synthetic SQL and keeps the raw complete AST dump inside the disposable runner rather than uploading it to the repository or public artifacts. A handful of candidate events and results are uploaded as JSON. No Apache Spark PR or upstream code was changed.

**Scientific status:** Causal *in-sample* fault localization achieved; truly unseen issue-family detection and measured robustness still absent. Independent correctness oracles presently have narrow grammar/immutable-small-fixture restrictions. Meaningful novelty requires a general, effect-aware mapping from each RuleExecutor pre/post transformation to a sound, checkable obligation plus a prospective independent bug evaluation.
