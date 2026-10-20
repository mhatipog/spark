# QueryProof W32 — preregistered first-run full optimizer suffix holdout

Date: 2026-10-20. **This file and the entire manifest are created before any W SQL execution.**

## Research question
Can source-pinned v5 (developed after the independent R32 *local-step* cohort)
strictly certify a **complete contiguous changed post-`Finish Analysis` Catalyst
optimizer suffix** for previously unseen join data and withheld SQL variants?
This measures a structural logical proof, not physical Spark correctness.

## Frozen experiment
- Official PySpark **4.2.0**, Java 17, independent SQLite finite unordered
  rowbag oracle; 32 new cases W01–W32, eight families, four cases each.
- Random generator LCG seed **2026102057**; 35 left and 29 right rows per
  case, independent case draws; duplicate keys, duplicate complete rows,
  negative integers, both-side NULLs and absent join keys guaranteed.
- 16 targets: INNER/LEFT equijoins, duplicate GROUP BY attribute, COUNT/SUM.
  Two predetermined SQL variants per family (e.g., COUNT(*) vs COUNT(r.j),
  SUM(r.extra) vs SUM(r.j)). 16 challenges: non-equi, OR predicate, DISTINCT,
  HAVING (two variants each). Expected UNKNOWN is valid scientific output.
- Primary: exactly complete v5 changed suffixes *after* Finish Analysis,
  counted by target/challenge family, with no posthoc rule grammar edits.
  Secondary: local Q join-step events, full independent state/frame/RDD
  input/all-rule/tag necessary gate statuses and counts. Report as
  aggregated necessary checks, without inventing same-case intersections.
- The **15 exact Git blob hashes** in `PROTOCOL.json` include the generated
  W manifest, v5 ledger, filter/grouping/join local checkers and all
  independent witness code. The workflow checks these before first SQL.
- No code/manifest/protocol edits, score thresholds, family exclusions or
  scorer reruns may modify the historical **first-run** W outcome. Later
  corrections require a separate named developmental replay.

## Scientific controls
1. JVM-executed SQL must be complete, no errors and no Spark/SQLite unordered
   bag discrepancies.
2. Source guard outcomes must all be complete. Raw ASTs, RDD paths, SQL
   literal values, and hidden-source details must never appear in reports.
3. Every admitted transition must match an exact restricted grammar; unknown
   rule families and context changes abstain. The previously exercised 32
   join/filter adversarial AST mutants and 2 algebraically unsound cases
   remain development tests, not W evidence.
4. R32 first run was 16/32 **local join step** certificates and 0/32 under
   the old v2 **full suffix** checker. W results will never overwrite it.
5. New data alone do not imply broad generalization: SQL skeletons partly
   reuse R, and all cases are purposive, synthetic, family-correlated.

## Frozen acceptance contract
An observed changed suffix with a v5 proof may be called **structurally
certified under defined logical row-bag semantics**. A counterexample,
unknown or incomplete witness is a failure or an abstention as appropriate.
No claim of soundness for Spark runtime errors, order, non-determinism,
UDFs, effects, physical plans or the whole SQL language.

## Independent source anchors
- Spark v4.2.0 `AttributeReference` source (qualifier used for naming,
  `sameRef` checks `exprId`, canonical form drops qualifier):
  https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala
- Spark v4.2.0 binding references by `exprId`:
  https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/BoundAttribute.scala
- These are source-backed semantics, not proof that every Spark optimization
  is correct. Bag/null lemmas are compared to finite countermodel tests.
