# QueryProof — Independent Y / Z / V6 and Coq handoff (20 October 2026)

**Status:** DEVELOPMENT, not a revised prospective W32 outcome.  
**Repository:** `mhatipog/spark`  
**Historical branch:** `research/queryproof-adversarial-tag-frame-20261020`  
**New isolated development branch:** `research/queryproof-phase-local-independent-20261020`  
**Baseline frozen W first-run SHA:** `1d56bb97e2327ce9f8dd0e5aebccb243140c3ee8`  
**Baseline X replay SHA:** `522cbdce600c0bc512748b0830991ee3cd99a751`

## Immutable evidence (do not re-label)

- W32 first prospective result: run `39435756545`, artifact `12273160063`, ZIP digest
  `sha256:024da3af1ea90616b1364cbc5f2ba0d75e10202e38003ba4837a0373bdf3ac01`.
  Original 15-file source/data blob pin preflight passed BEFORE any W SQL.
  **16/32 complete v5 structural post-Finish-Analysis suffixes; 16/16 target,
  0/16 challenge; 32/32 finite Spark/SQLite rowbags agree.**
- X independent, explicitly posthoc replay: run `39436715938`, artifact
  `12272901343`, digest
  `sha256:fc75ad3c06954e45f5d18464128c1291851ca303dc5fc4857f73619a56ce81f8`.
  **0/16 strict global hidden-state acceptance**, although all other necessary
  guard sets equalled the 16 v5 candidates.
- Preserve older M/O/R/L results and verifier pins separately. No W first-run
  source, manifest, protocol, verdict or CI artifact was changed here.

## First-principles Y hypothesis and independent result

`state_guard.py` considers tags, metadata and opaque collections observed at
ANY phase. An optimizer-suffix theorem starts after `Finish Analysis`.
`phase_guard.py` restricts the opaque-collection check to the cut, post-cut
events/batches and exit, and delegates tag/metadata stability to
`frame_guard.py`. This distinction matters: pre-cut transient opacity cannot
itself refute a post-cut suffix property.

New run `39440559109` on the development branch passed seven independent
synthetic phase/trace tests and replayed unchanged W32: 16 v5 candidates,
**16/16 phase-local observational acceptance**, but strict original gate still
**0/16**. A pre-cut opaque-count mutant preserved W01 acceptance; a post-cut
changed-rule opaque-count mutant rejected W01 and retained 15 other cases.
The result is explicitly posthoc and only observational.

**FUNDAMENTAL NEGATIVE:** Spark tag fingerprinting is NOT injective before
SHA-256: the maps `{qpA: "one|qpB:two"}` and
`{qpA: "one", qpB: "two"}` produce the same delimiter-joined string and hash.
The repository contains a genuine Spark Scala collision fixture
`QueryProofTagFrameCollision.scala`; our independent Python test matches
the exact problematic encoding. No snapshot-only equality of this digest
proves real mutable tag-map equality. A native post-cut tag write with the
same encoding, or any mutation between snapshots, can elude the guard.
Thus Y recovers useful observation coverage but **does not solve soundness**.

## Round Z: identify every unsupported W case

Run `39441946120` succeeded. Another redacted shape-discovery Z replay
`39442740147` succeeded. 48 observed post-cut changed-rule steps were
classified in W17–W32; all sixteen remained UNKNOWN in frozen v5:

| Cases | Family | Exact v5 blockers per case |
| --- | --- | --- |
| W17–W20 | INNER_LT | ColumnPruning, InferFiltersFromConstraints, PushDownPredicates (all condition requires direct EqualTo, found LessThan) |
| W21–W24 | LEFT_OR | ColumnPruning only (join condition is two direct EqualTo branches under Or) |
| W25–W28 | DISTINCT_PAIR | ReplaceDistinctWithAggregate has no v5 checker; ColumnPruning lacks new aggregate/projection frame |
| W29–W32 | HAVING_COUNT | ColumnPruning aggregate frame, ConstantFolding absent, InferFiltersFromConstraints and PushDownPredicates top frame unsupported |

Trace-derived redacted shapes: `LessThan(AttributeReference,AttributeReference)`
and `Or(EqualTo(AttributeReference,AttributeReference),
EqualTo(AttributeReference,AttributeReference))`, each operator carrying
`num-children:2, left:0,right:1`. Literal values and source ASTs were NOT
exported. Source `run_z.py` retains per-family rejection reasons only.

## Round V6: a new *posthoc* checker, not a new prospective claim

Run `39443657623` succeeded under official Spark 4.2.0, reexecuting W32
only as development. `join_project_v6.py` derives from frozen
`join_project_v1.py` Git blob `0440ff7841573ce5fd17c8dbb64de91b3a59a49b`.
`scope_ledger_v6.py` derives from frozen v5 blob
`76e4e65cd934607ca62abbbcd596d19b65cea2ce`.
Both originals remain untouched.

The new checker accepts only an unchanged two-branch direct-integral
`Or(EqualTo(L,R),EqualTo(L,R))` predicate and a dependency-preserving
identity ColumnPruning context. It verifies all referenced keys are retained
and source-consistent. It **does not** infer non-NULL guards under OR.

**V6 result: 20/32 complete structural optimizer suffixes** (W01–W16
retained, W21–W24 newly certified); zero regression. Each of four new
positive local events survived three real-trace mutants? NO: the 12 mutants
(OR→AND, EqualTo→EqualNullSafe, foreign ExprId) all returned UNKNOWN.
The source frame, RDD input, all-rule and tag-type necessary gates each
retained 20 candidates. Y's phase-local observer retained all 20.
**The unchanged strict global state guard retained 0/20.** This is not an
unconditional proof and must not be reported as revised W32 first-run
performance. It is a candidate theorem/grammar development result.

## Mathematical mechanization

- `QueryProofNarrowJoin.v`: official Ubuntu Coq 8.18 compiled in run
  `39441610840`; extended inequality and OR negative control compiled in
  `39442134157`. Finite-list multiplicities, standard SQL-equality
  TRUE-only NULL semantics, LEFT OUTER padding, safe right-side filtering,
  inner-side filtering, filter-map commutation, duplicate grouping-key
  partition equivalence, wrong preserved-side filter and null-safe equality
  counterexamples. Inequality null-filter laws and an OR per-disjunct-guard
  counterexample added. Not Spark implementation theorems.
- `QueryProofParametricFrame.v`: official Coq 8.18 kernel accepted complete
  predicate-parametric INNER and LEFT OUTER projection-frame laws in
  run `39444180008` (after a previously preserved compilation failure in
  run `39443997728`). The assumption is a pure, total predicate factoring
  through projections. The predicate itself can include equality, inequality,
  OR and NULL semantics subject to that factorization condition.
- Older first compilation failures are deliberately retained:
  `39441166021` induction rewriting order; `39441418086` escaped Coq
  conjunction lexer; fixed before success. No `Admitted` or axiomatic
  shortcuts; the workflow checks this. None of this establishes correctness
  of Catalyst AST parsing, JVM state, or SQL aggregates/overflow/errors.

## External comparison

Pinned SQLSolver source `dcc2a91d8971a4c4d30b055f99d7d8428a1b754b`
compiled, run `39442264903`. Three preregistered mini SQL pairs yielded
`NEQ,EQ,EQ` as expected. This is a tool smoke/control comparison, NOT a
full W32 head-to-head; SQLSolver's original schema uses a primary key.

VeriEQL first comparator was preregistered separately in
`tools/queryproof_verieql_controls_20261020` before first execution:
official source commit `493cbb81000205e33b0623cfd1c39106fa035fae`,
input blob pinned in `PROTOCOL.json`, five SQL pairs (literal mismatch,
identity projection, duplicate grouping, LEFT_OR projected dependency, bad
OR-side IS NOT NULL), no PK uniqueness, max relation bound 3, explicit
UNKNOWN/unsupported policy. Development run `39444540912` is pending at
the time of this note; inspect actual result, do not guess success.

Published comparison: SQLSolver (SIGMOD 2024) proves stronger SQL-level
equivalence in its fragment; VeriEQL (OOPSLA 2024) has richer bounded SQL,
three-valued logic, bags, constraints, joins and aggregation; Ricciotti &
Cheney (JAR 2022) mechanize broad SQL with NULLs in Coq. QueryProof is
not original in SQL algebra or first in machine-checked SQL equivalence.
Its **candidate differentiator** is observed, provenance-linked proof
carrying over *actual optimizer rewrite chains*. Novelty is unestablished.

## Scientific conclusion / current trust boundary

- W32 remains **16/32 prospectively structural**.
- X remains **0/16 under strict global hidden-state acceptance**.
- Y is **16/16 posthoc phase-local observational**.
- V6 is **20/32 posthoc structural and observational**, but **0/20 under
  the still-existing strict guard**.
- Coq proves only the explicit list/NULL/frame model. It is NOT yet
  connected by a verified parser or semantics-preservation theorem to
  Spark's real Catalyst plans.
- All finite Spark/SQLite matches are testing evidence, not universal
  SQL equivalence proofs. No Spark correctness bug discovered here.

## Next research that can actually falsify the design

1. **Prove the bridge**: verified parsing of exactly recognized Catalyst
   operator/expression serialization into typed Coq semantic terms, with
   sound ExprId binding and output-field identity. Reject or reveal lossy
   metadata and tag domains at every boundary.
2. **Eliminate lossy state equality as a basis for soundness**: instrument
   mutation sites (including alias/shared mutable tag values), or prove
   a narrow static noninterference/effect discipline for the only supported
   Catalyst rule/attribute/tag families. Snapshot hash agreement alone is
   NOT an acceptable premise for an unconditional theorem.
3. **Independent prospective V7**: freeze new checker and data before
   first execution; withhold fresh multi-key OR/inequality and nested frames.
   Predeclare targets/UNKNOWN, exact whole-suffix and all necessary SAME-CASE
   intersections, mutation controls and source pins.
4. For `INNER_LT`, mechanized NULL-intolerance is ready, but actual
   ColumnPruning/InferFilters/PushDownPredicates must each have sound
   source-level AST checks. For `DISTINCT_PAIR`, prove
   `ReplaceDistinctWithAggregate` and adjust frames; `HAVING_COUNT`
   is hardest because four rule families interact.
5. Compare SQLSolver/VeriEQL on genuinely matched SQL pairs and semantics
   assumptions. Report unsupported/timeouts and do not compare incomparable
   raw percentages. Use published Coq SQL mechanizations as the semantic
   standard against which the mini-model should be measured.
6. Preserve all CI attempts, negative fixtures, source SHA pins and
   original first-run reports; no retroactive relabelling and no premature
   upstream Spark PR.

**Promising proposed design (not yet established novelty):**
*effect-indexed, proof-carrying Catalyst trace*: a rule event exports a
typed semantic dependency footprint, actual source identity and a local
kernel-checkable rewrite obligation; composition checks every contiguous
batch and optimizer boundary while an explicit effect/noninterference
policy handles mutable tags, metadata and unobserved state. The essential
research claim is the trustworthy *bridge from real execution to proof
objects*, not the rediscovery of equivalent SQL rewrites.
