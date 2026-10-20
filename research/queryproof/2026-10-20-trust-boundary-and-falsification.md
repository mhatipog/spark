# QueryProof — trust boundaries and falsification agenda (2026-10-20)

## What the successful W32 result actually establishes

A source-pinned, explicit verifier admitted 16/32 *observed actual
post-FinishAnalysis changed Catalyst plan transitions*, composing each
recognized rule with checked syntactic frames to a complete logical optimizer
suffix, and all 32 output bags agreed with independent SQLite on their finite
datasets. It is encouraging **restricted proof-chain automation**, not a
general Spark query correctness theorem or a discovered new SQL algebra law.

## Which components remain trusted / observational

1. **Capture completeness:** `RuleExecutor` modifications emit nested scope
   boundaries and effective events. The all-rule call audit independently
   cross-checks event counts, but both hooks are inserted in the same
   experimental Spark runtime. This does not exclude transformations outside
   the covered hook sites, and a common-mode instrumentation bug remains.
2. **Plan representation:** Spark `TreeNode.toJSON` omits potential live
   source/runtime semantics. Same serial AST is necessary for our restricted
   row-bag proof, but not sufficient for arbitrary attribute metadata,
   expression tags, mutable JVM objects or exceptions. Extra witness layers
   detect only inspected categories and their hash encodings can collide.
3. **Mathematical soundness:** the join/project/filter/grouping lemmas are
   implemented by conservative grammar checkers and reasoned algebraically,
   but not yet translated into a kernel-checked proof assistant. The
   independent 2,916 guard and 531,441 projection multiset tests detect
   selected mistakes; bounded testing cannot prove the quantified laws.
4. **Data oracle:** SQLite independently verifies Spark's final unordered
   results on 32 synthetic bags. It is neither a full SQL semantic model nor
   an execution-error/non-determinism/ordering oracle. Shared wrong
   assumptions can cause both engines to agree incorrectly.
5. **Sample selection:** W deliberately enriched exact equal-key joins and
   repeated GROUP BY keys that v5 supports. 16/16 target success measures
   *this admitted fragment*, not average production Spark proof coverage.
   The 16 challenge UNKNOWNs are principled abstentions, not Spark faults.

## Source-backed narrow semantic anchors

- Spark v4.2.0 `namedExpressions.scala`: `AttributeReference.sameRef`
  checks exprId, `qualifier` is optional naming context and canonicalization
  discards it; ordinary Scala equality nevertheless compares qualifier.
  https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala
- Spark v4.2.0 `BoundAttribute.scala`: binding a resolved reference
  selects input slot by exprId, not qualifier.
  https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/BoundAttribute.scala
- SQL NULL and bag mechanization already exists (Ricciotti/Cheney 2022);
  SQLSolver and VeriEQL already establish stronger SQL equivalence results
  on other semantics/fragments. QueryProof must position observed rule-chain
  provenance as an unproven differentiator and benchmark with them, not
  claim first-ever proven SQL rewriting.
  https://doi.org/10.1007/s10817-022-09632-4
  https://github.com/SJTU-IPADS/SQLSolver
  https://github.com/VeriEQL/VeriEQL

## High-value adversarial experiments (not yet performed)

- Instrumentation common-mode: fabricate an effective rule change omitted
  from only one observer stream; require full suffix / all-rule accounting to
  abort. Then fabricate nested scope misordering; require fail-closed.
- Disguised semantic input: keep serialized LogicalRDD shape unchanged
  while switching underlying RDD reference; input observer must quarantine.
  Restore reference before final snapshot to explore blind spots.
- Adversarial metadata: mutate an `AttributeReference` exprId while
  preserving qualifier/type/name; fail closed. Change only qualifier on
  resolved identical exprId and verify both semantic equivalence and
  admissibility, while rejecting qualifier interpreted as a binding key.
- SQL three-valued counterexamples: left-preserved-side IsNotNull and
  EqualNullSafe join must be demonstrably unsound for corresponding inferred
  filters; store minimal NULL witness bags in tests.
- Mathematical kernel: encode the four local inference laws over finite bags
  with explicit SQL TRUE/FALSE/UNKNOWN and NULL padding, then discharge via
  SMT with checkable certificates or a Coq/Lean proof; retain a separate
  implementation-to-model conformance test for Catalyst AST serialization.

## Research programme acceptance bar

The next major claim should require at least one *structurally new* withheld
Spark plan family, full case-ID intersection of five necessary independent
observations, a short mechanically checked proof of the local semantics, and
negative controls that break it. Until then the result remains a promising
synthetic prototype with strong internal discipline, not a safety-critical
verification product or a general-purpose optimizer proof.
