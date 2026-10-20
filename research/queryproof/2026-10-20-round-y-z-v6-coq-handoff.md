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

**FUNDAMENTAL NEGATIVE 1:** Spark tag fingerprinting is NOT injective before
SHA-256: the maps `{qpA: "one|qpB:two"}` and
`{qpA: "one", qpB: "two"}` produce the same delimiter-joined string and hash.
The repository contains a genuine Spark Scala collision fixture
`QueryProofTagFrameCollision.scala`; our independent Python test matches
the exact problematic encoding. No snapshot-only equality of this digest
proves real mutable tag-map equality. A native post-cut tag write with the
same encoding, or any mutation between snapshots, can elude the guard.
Thus Y recovers useful observation coverage but **does not solve soundness**.

**FUNDAMENTAL NEGATIVE 2 (independently tested later):** the actual
`instrument_frame.py` observer records tags as a sorted *multiset* of
`node.getClass.getSimpleName + SHA256(tags)`, with NO node identity.
Swapping distinct tag values between two different `AttributeReference`
instances therefore produces exactly the same frame, EVEN WITH an injective
tag serialization. `test_frame_observer_aliasing.py` checks this exact
source encoding, the delimiter collision, a different-class detectable
control, and a transient-mutated-then-restored snapshot. Source-pin
`instrument_frame.py` blob
`3d2f64d6b91e5a09830a43f066f6690af91abdbb`. CI
`39446088363` passed **4/4**; test committed at
`15eaa3ade87fb3de5cfc473fe9f18666cb9c91c2` and independent workflow at
`1fa7916ec84633edaf1cfb37214b01791d19e439`. This is an
**observer noninjectivity / insufficiency counterexample**, not evidence of
wrong Spark query answers. A separate full Y replay `39446056607` again
passed, with original 16/16 observational and strict 0/16, pre-cut
positive W01 and post-cut negative W01.

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
retained, W21–W24 newly certified); zero regression. For four new positive local events, twelve independent real-trace mutants
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

## Effect-indexed trace calculus and sharper proof boundary

`research/queryproof/proofs/QueryProofEffectIndexedTrace.v` was added in
commit `19bb5788b483477faebfe15e465edb8b1dac1d3a` and **kernel-checked
in Coq 8.18** without any admitted/axiomatic shortcuts, CI
`39446469409`. It explicitly separates (i) preservation of the visible
plan rewrite at the starting hidden state, from (ii) semantic
**noninterference** of the hidden-state change at the resulting plan. A
finite certified suffix composes by equality transitivity with an arbitrary
pre-cut analyzer state: there is no need to prove the prefix for a
post-Finish-Analysis theorem.

Crucial generalization: hidden observation need not be byte-for-byte
injective; it must be a **semantic congruence** for all supported resulting
plans. Coq proves sufficiency of this premise and gives an explicit Boolean
countermodel to the unsound rule “equal observer + equal plan => equal
meaning.” This is a **generic formal calculus**, not a verified Catalyst
extractor or checked effect analysis. Proving that any concrete Spark
rule/attribute/tag domain meets the premises is still the central task.

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
UNKNOWN/unsupported policy. The **first** development comparator run `39444540912` (artifact
`12274859459`, SHA256
`7b7b9bb63e5e474eca95d06956c33d58f83dbd1d270e4f78bac0828bca15efc6`)
recorded **five OTE infrastructure failures**, not five semantic verdicts:
`And(..., ctx=...)`, `Or(..., ctx=...)`, `Sum(..., ctx=...)` rejected
by installed ordinary `z3-solver==5.3.1.0`. The official VeriEQL
source's README explicitly requires copying its custom `z3py_libs/*.py`;
source-pinned `z3.py` blob `c64e6584ee24df2b83a701d2e6f017530162653f`
contains the distinct signatures `And(*args, ctx)`, `Or(*args, ctx)`,
`Sum(*args, ctx=None)`. This is a missing documented setup step in our
first harness, not a VeriEQL equivalence failure.

A separately labelled **posthoc author-Z3 environment repair** workflow
`.github/workflows/queryproof-verieql-author-z3-repair.yml` pins
`z3-solver==4.12.2.0`, copies the unchanged official 11 custom Z3 files,
checks their signatures, and uses the SAME five previously pinned SQL pairs
and 1–3 row bound. Run `39445532031`, commit `bf883dcf31839005a910abd0abc87a62095e16e7`, passed:
`S1=NEQ, S2=EQU/EQU/EQU, S3=EQU/EQU/EQU,
S4=EQU/EQU/EQU, S5=NEQ` — **5/5 predicted classifications**.
These are bounded, five-case development controls, not unbounded proof or a
matched 32-trace comparison. The initial five OTEs remain immutable.

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
2. **Prove a source-level hidden-effect *semantic congruence*, not merely
   state-hash equality**: enumerate effectful tag/metadata reads/writes in
   the supported Spark rule families, establish exactly which effects are
   semantically irrelevant to each resulting plan, and encode these as
   checked obligations. If relying on observation, repair both string
   delimiters AND node-identity aliasing and instrument intra-rule writes
   (a reverted transient mutation defeats any boundary-only snapshot).
   Snapshot hash agreement alone is NOT an acceptable premise for proof.
3. **Independent prospective V7**: freeze new checker and data before
   first execution; withhold fresh multi-key OR/inequality and nested frames.
   Predeclare targets/UNKNOWN, exact whole-suffix and all necessary SAME-CASE
   intersections, mutation controls and source pins.
4. For `INNER_LT`, mechanized NULL-intolerance is ready, but actual
   ColumnPruning/InferFilters/PushDownPredicates must each have sound
   source-level AST checks. For `DISTINCT_PAIR`, prove
   `ReplaceDistinctWithAggregate` and adjust frames; `HAVING_COUNT`
   is hardest because four rule families interact.
5. Expand the now-working five-pair VeriEQL comparator to genuinely
   matched SQL pairs and semantics assumptions, and repeat each run under
   its own prospectively pinned protocol. Preserve original five OTEs and
   the posthoc 5/5 repair independently; report UNKNOWN/timeouts as such. Use published Coq SQL mechanizations as the semantic
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

## End-of-round acceptance gates

A researcher may describe W32 as **prospectively 16/32 structural** and X
as **0/16 under strict hidden-state guard**. They may describe Y as
**posthoc 16/16 observational**, V6 as **posthoc 20/32 structural** with
**0/20 legacy strict acceptance**, Coq as **kernel-proven conditional laws**,
and the first/repaired VeriEQL studies as **5 OTE** and **5/5 bounded expected
outputs**, respectively. They must NOT describe any QueryProof structural
candidate as an unconditional Spark transformation proof until: (a) the
Catalyst AST-to-Coq meaning-preserving bridge is mechanically checked,
(b) each step carries actual rule-source identity and sound obligations,
(c) hidden-state congruence/noninterference is source-proven, (d) the whole
post-cut chain composes, and (e) adversarial negative cases fail closed.

Next proposed first independent V7 study: freeze checker, trace sources,
new SQL, mutation inventory and prospective scoring gates BEFORE the first
fresh run; target both INNER_LT and two-branch LEFT_OR without reusing
W32 cases for prospective headline metrics. Prefer one sound, completely
proven family over higher observational percentage bought with assumptions.
