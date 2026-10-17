# QueryProof — older frozen cross-family correctness benchmark (2026-10-17)

## Purpose
The September/October QueryProof optimizer-risk heuristic previously failed to detect **all three reproduced unrelated correctness bugs** in the 2026-10-16 historical benchmark. This experiment separately scores the new automatic analyzed-plan final-result ordering checker on exactly those earlier frozen issues and controls. It **does not use expected rows during key extraction or detection**; oracle scoring is applied after each Spark execution.

The test manifest at `tools/queryproof_prospective/manifest.json` was frozen on 2026-10-16, before this extractor. **Nevertheless**, the issues had been investigated in the research program, so these are independent bug *families relative to the ordering proof-of-concept*, not a pristine never-before-seen scientific holdout.

## Confirmed Spark 4.2.0 result
[GitHub Actions 39068283833](https://github.com/mhatipog/spark/actions/runs/39068283833) (the Spark 4.2.0 job completed successfully):

- **B01 SPARK-58384 (null join under NOT):** 2/2 wrong row bags; 0 ordering violations (returned rows were order-compatible).
- **B02 SPARK-58442 (subquery IN / three-valued logic):** 2/2 wrong results; UNKNOWN, because no supported output ORDER BY.
- **B03 SPARK-58429 (empty input global aggregate with subquery):** 2/2 wrong results; UNKNOWN.
- **B04 SPARK-59126 (AQE ordering):** 2/2 baseline correct in this configuration, therefore not a reproduced fault and not counted as a detected bug.
- **B05 SPARK-55038 (array aggregate):** 2/2 baseline correct in this configuration; UNKNOWN.
- **N01..N04:** all 8/8 negative-control executions correct.
- **0/6** independently oracle-wrong executions witnessed by the final-output ordering checker.
- **0** false accusations on the remaining 12 oracle-correct executions.
- Explanations: 10 NO_VIOLATION_OBSERVED and 8 UNKNOWN across all 18 tested executions.

This is the **expected limitation**, not a surprise defect in the ordering checker: ordering is not a sufficiently informative property to catch wrong row bags, SQL NULL predicates, or aggregate correctness.

Spark 4.0.1 has a separate matrix job in the workflow; this memo records 4.2.0 independently of whether that job completes.

## What should change in the research direction

Focus on automatic **property-specific** semantic witnesses, not a single magic general heuristic. Consider:
1. Null semantics: restricted, independently verified three-valued predicate identities in filter/join contexts, comparing returned row bags across proven-equivalent formulations.
2. Empty-input aggregate contracts: e.g. global count(*) on an empty input must be zero, and scalar expressions not based on the input count must remain intact.
3. Semantics of correlated expressions across LIMIT/OFFSET (already demonstrated with independent source-stage TopK membership evidence).
4. Selection of genuinely unseen defects, method/bug partitioning, transparent UNKNOWN accounting, and external baselines (SQLancer/DQP, DistSQL).

A repeated violation of one bug family is not a new bug; simple ordering and TopK identities are not novel mathematics. A publishable contribution would require a generally applicable safe **contract extraction and composition** mechanism across relevant Catalyst/AQE rules, with rigorous validation and competitive comparison.
