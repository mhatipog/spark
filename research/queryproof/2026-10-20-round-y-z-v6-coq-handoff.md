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

## Subsequent Round V7: strict effective-rule completeness and real hidden effects

**V7 first diagnostic run:** `39451278579` on the SAME W32 cases, with
`tools/queryproof_posthoc_v7_20261020/scope_ledger_v7_all_steps.py`,
`run_v7.py`, and five pre-replay synthetic tests (5/5 passed).
The strict auditor was cloned from V6 blob
`9ca27603cdc36de30a779e307eac87d131a61d24`; it disallows the
legacy shortcut `if start == end: return True` whenever a batch,
executor or suffix encloses effective rule steps that lack proof. A synthetic
net-zero intermediate rule cycle clearly demonstrated the OLD ledger could
accept identity while hiding two uncertified events. The strict ledger
correctly rejected it. **Actual W32 result: 20/32 remained event-complete,
0 regressions; 0 observed post-cut identity cycles hiding unsupported
steps; 12 unsupported cases remain UNKNOWN.** This is an important
failed falsification of the W32 data, NOT a repair to the hidden-state guard
and NOT a new prospective W run. Source labels in the audit say exactly
what is and is not proved. Original strict hidden-state X remains 0.

**Independent production-engine source audit:** official `apache/spark`
`v4.2.0` `TreeNode.scala` blob
`e82e6a30b9bba41e637b27cbab43f26d9d23ff9b` defines mutable private
`_tags`, `setTagValue`/`unsetTagValue`, and `fastEquals` based on
reference or structural equality. Official `RuleExecutor.scala` blob
`b5a35608a54e688c798796f81b949e2998a6f546` labels a rule
effective only when `!result.fastEquals(plan)`. QueryProof's disposable
`QueryProofAstTrace.capture` is INSIDE that `if (effective)` block.
Therefore in-place hidden-only mutations are not necessarily accompanied
by a `QUERYPROOF_AST` event.

Native CI `39451725225`, code
`tools/queryproof_posthoc_v7_20261020/QueryProofNativeInPlaceTagWitness.scala`
(commit `923b386ec8ed24c4a20ddf7c32e4e581ca0af82b`) compiled real
Spark 4.2.0 JVM code with the instrumented RuleExecutor. A deliberately
crafted rule set a tag on the same plan instance; tag changed but plan JSON
and effective-rule AST event remained unchanged. A second crafted rule
set/unset a tag within one invocation; snapshots before/after were equal.
The real structural rewrite positive control emitted its AST event. This
is **instrumentation incompleteness**, NOT a Spark miscompilation.

**Independent private-rule-cache result:** `TreeNode.scala` also contains
private mutable `_ineffectiveRules: BitSet`, which
`transformDownWithPruning` consults and updates. This field is not exposed
by the current `stateCounts` product/metadata/tag census or
`queryProofFrame` hashing. Native CI `39452186486`, code
`QueryProofNativeRuleCacheWitness.scala` (commit
`7635cecd03ecf47b35d7b8b633905698a7d727af`), proved with fresh-node
control that the cache suppresses the second rule traversal on an otherwise
unchanged `OneRowRelation`, while tags and JSON stay unchanged. Spark's
official `RuleIdCollection.scala` blob
`a890d43f0672c39d70fdccc348ca17d84b70936a` documents the intended
PURE/external-state-independent RuleId contract. This is correct Spark
behavior under its contract, not a correctness defect.

**Crucial production-rule rather than toy-only confirmation:** official
`v4.2.0` `optimizer/expressions.scala` blob
`929ffdef2b52ff1aabdc6f38501dc131e7938fd2` has
`ConstantFolding.FAILED_TO_EVALUATE: TreeNodeTag[Unit]`. Its
`tryFold` catches an evaluation error inside a conditional branch and
sets this tag on the existing Expression; a subsequent `containsTag`
branch changes future folding control flow. Native CI `39452930918`
compiled `QueryProofNativeConstantFoldingTagWitness.scala`
(commit `567dacd688efe3fe523150d9e970ff5f4fdb1321`) against official
Spark 4.2.0: an invalid ANSI cast in a conditional branch caused the REAL
`ConstantFolding` rule to mutate `FAILED_TO_EVALUATE`, yet plan JSON was
identical and no `QUERYPROOF_AST` event appeared. A separate successful
constant-folding control DID emit the expected AST event. This establishes
a genuine production-rule observation gap, but says NOTHING about a Spark
wrong-answer bug or whether this occurred within W32's 20 successes.

**Math/semantics refinement:** Coq file
`QueryProofRuleCacheEffect.v` (commit
`708d6cb46c3b877dea72e3afb875a4494b131b31`,
kernel CI `39452554191`) distinguishes exact-plan cache correctness
(requiring valid `rule(p)=p` when skipping) from *semantic* correctness.
A skipped rule can be semantically harmless even when its optimized AST
would differ, PROVIDED all possible executions of that rule preserve
denotation. A Boolean countermodel demonstrates why external-state-dependent
rules can invalidate the exact replay premise. Theorem is an ABSTRACT Coq
model, NOT verified Spark implementation semantics.

**Synthesis:** strict changed-AST event tracing is necessary but does not
observe all in-place optimizer effects. A sound candidate system must add
either (i) explicit, correctly scoped effect witnesses at mutation sites
(also transients), OR (ii) mechanically justified semantic noninterference
for hidden state affected by each supported rule family. The second is
potentially substantially more efficient but requires a verified source
bridge. For example, `FAILED_TO_EVALUATE` controls whether a fold is
reattempted, while the result expression is unchanged in the caught-error
path; its global semantic irrelevance has NOT been proved here.

**Related compiler-verification frontier:** LLVM's Alive2 already performs
translation validation of ACTUAL compiler optimization pipelines; CompCert
has long supplied mechanized compiler-correctness proofs. Therefore
actual-execution-linked validation is NOT a generic first. QueryProof's
candidate contribution is an effect-aware translation validator specialized
to the subtleties of Catalyst / SQL bag and NULL semantics, contingent on
a trustworthy Spark/semantics bridge and comparable benchmarks.

### Next falsifiable source-specific round

1. Source-audit Spark 4.2.0's exact five V6-certified rule families for every
   `TreeNodeTag`, `RuleId`, private memo, metadata, stateful expression,
   evaluation-error and query-configuration dependency. Preserve source
   blob hashes and build a machine-readable effect inventory.
2. Exercise a **production** `ConstantFolding` silent-effect counterexample
   in a controlled multi-rule chain, comparing post-cut tag/cache states and
   resulting plans. Test a second rule that reads that state. Do not claim
   wrong answers from a mere planning-path difference.
3. Define a typed, complete semantic projection for the *exact* emitted
   Catalyst AST fragment, with a proof that the parser rejects every
   unrecognized value-bearing field rather than treating it as hidden junk.
4. State source-proven per-rule semantic noninterference obligations, and
   connect them to the already kernel-checked effect-indexed trace calculus.
5. Freeze a new prospective corpus with unexposed, adversarial target
   families and negative mutators before the first V8 execution; report
   an EXACT same-case intersection of whole-suffix proof and all independent
   guards. The original W32 remains 16/32 prospective forever.



## Follow-up: causal production ConstantFolding tag decision and narrow Coq law

**Native causal result:** new `QueryProofNativeConstantFoldingDecisionWitness.scala`
introduced in commit `e8944ef7df05947701c0d909367c91c7e2096597`.
Independent workflow `.github/workflows/queryproof-native-constantfolding-decision.yml`
introduced in commit `d3027284346c8239ac5bcc4da5b376bafbfb8267`.
Pinned pip `pyspark==4.2.0`, Scala 2.13.18, Java 17, genuine
production `ConstantFolding`, instrumented disposable `RuleExecutor`.
**First native run `39457131853`, job `118746827610`, succeeded.**
Logs print initial failed compile-time evaluation attempts `1`;
subsequent invocation on a FRESH Project tree with the SAME tagged expression
retained `1`; clearing exactly `FAILED_TO_EVALUATE` and invoking again on
another fresh Project produced `2`; unrelated untagged expression separately
confirmed another attempt. Every resulting serialized AST remained unchanged,
and every execution emitted NO `QUERYPROOF_AST` structural event.

The `CountingFailure` expression is DELIBERATELY test-only and is not a
normal Spark SQL expression. The mutating, reading, and conditional folding
logic is the actual production rule. This is a causal demonstration that the
production `FAILED_TO_EVALUATE` tag affects a later optimizer decision,
not a wrong-result case. Fresh root plans explicitly control for the
separate private `_ineffectiveRules` memoization effect.

**Independent upstream intended-behavior confirmation:** official Apache
Spark v4.2.0 source `optimizer/expressions.scala` pinned blob
`929ffdef2b52ff1aabdc6f38501dc131e7938fd2` writes the tag in
`tryFold` and reads it before the `e.foldable` branch. Official
`ConstantFoldingSuite.scala` blob
`0489308a1ab24f26a4124529b00fea6a5fde6525` includes
`SPARK-39106: Correct conditional expression constant folding`: it
asserts the failure tag exists for nonfoldable conditional inputs with
arithmetic errors under ANSI, and that optimized versus analyzed plans
compare equal. This supports **intentional planning behavior**, not any
Spark defect or occurrence within the W32 suffixes.

**New conditional mechanization:** `QueryProofConstantFoldingFailureTag.v`
introduced at `23d6ae4d76134c51dc16ffe804428f9c0f7ac04f`.
Initial Coq CI `39457747597` FAILED because `fold_step` was not
explicitly reduced; correction `94e61bbcc1f42eb3950b59a17a5ccc458363a0a1`
ran as `39457953332` and FAILED on equality direction; final correction
`e9a310231d9ec3683bc1d90752c77b2f4a3c9976` passed **official
Coq 8.18 kernel**, CI `39458103376` (no admitted/axiomatic shortcuts).
The proved model distinguishes SQL NULL (`None` runtime value) from
compile-time failure (`None` outer optional result), and proves
failure-tag denotational inertness, folding preservation under an explicit
`statically_sound` assumption, sound double application, and a
reproducible tag-driven decision difference. It contains a counterexample
to an unsound compile-time evaluator and a separate hidden-sensitive
runtime-evaluator counterexample. It intentionally excludes runtime
exceptions, SQL joins/aggregation, environment changes, and verification
that actual Scala code refines the Coq model. Its premises must NOT be
mistaken for discharged obligations about arbitrary Catalyst expressions.

**New refined hypothesis:** A real optimizer tag may be **control-relevant**
without being **value-relevant**. Enforcing bytewise identity of the
complete mutable state is therefore unnecessarily restrictive for some
local rewriting claims. Conversely, silently omitting mutable state is
UNSOUND absent source-linked proof that the relevant expression evaluator
and all possible later rewrite paths preserve denotation regardless of
that state. An adequate certificate must have a typed effect classification
and per-rule source-backed semantic noninterference proof; a plan JSON
checksum or restored tag snapshot supplies neither.

**Historical scoring UNCHANGED:** frozen prospective W=16/32 structural,
X=0/16 strict; Y=16/16 phase-local observational, V6/V7 posthoc=20/32
structural and 0/20 strict. The new causal native test and Coq theorem
add no W32 structural certifications, no newly accepted strict cases,
and no proven SQL answer discrepancy.

**Next decisive experiment:** build a source-pinned, fail-closed effect
dependency inventory (per rule: emitted expression classes, external SQLConf,
tag/memo/metadata writes, later readers and runtime evaluator reads);
try to instantiate the effect-indexed Coq trace theorem with an actual
Spark 4.2.0 expression-extractor soundness bridge, or report UNKNOWN.
Then return to the 12 unsupported W32 cases with a separate prospective
corpus and blind negative controls. Preserve all failed CI runs.


## Follow-up: Spark JSON serialization collision, impossibility theorem, and source-side repair

**Major new source-level falsifier:** official Spark v4.2.0
`sql/catalyst/.../trees/TreeNode.scala`, pinned blob
`e82e6a30b9bba41e637b27cbab43f26d9d23ff9b`, serializes
`Metadata` through `case m: Metadata => Metadata.empty.jsonValue` in
`parseToJson`. The source comment cites Spark issue SPARK-17356:
unbounded ML metadata can trigger out-of-memory conditions. The serializer
also drops generic maps and has intentionally truncated sequences; neither
`toJSON` nor `prettyJson` is a complete, injective Catalyst object
representation. This is standard intentional Spark behavior, not a bug.

The official v4.2.0 `namedExpressions.scala`, blob
`c7a2165480854905999e22be6125d76eb3a31db0`, makes
`AttributeReference.equals` depend on `metadata` and defines `ExprId`
equality using BOTH numeric `id` and `jvmId`. The same numeric
`ExprId` alone is NOT a sufficient binding identity.
`BoundAttribute.scala` blob
`9a0bab32061fa0d12af3cefb8055dc06a4c5942c` shows
`BindReferences.bindReference` locates slots by full `exprId`;
binding must happen after column-pruning layout changes.

**First native serializer experiment:** source
`tools/queryproof_posthoc_v7_20261020/QueryProofNativeJsonLossWitness.scala`,
commit `a64c8126e40dccb6a60ca7c742bc7d2f5d4723bc`, workflow
`.github/workflows/queryproof-native-json-loss.yml`, commit
`a84c617248ab35aae62c95ed4afc111cb462b3dc`.
GitHub Actions **`39461415673`**, job `118761549608`, SUCCESS,
actual unmodified Spark 4.2.0 JVM:

- Two `AttributeReference` nodes with the SAME name, type, nullability
  and full ExprId but distinct actual metadata
  `{"queryproof_provenance":"secret-A"}` and `secret-B` were unequal
  by Spark `equals/fastEquals`, but `toJSON` strings were EXACTLY equal
  and printed `"metadata":{}` for both.
- The collision survives wrapping in a genuine unequal
  `LocalRelation` logical plan: again IDENTICAL `toJSON`.
- Positive controls showed Spark's JSON *does* retain distinct ExprId
  JVM UUIDs even when numeric ids collide; data type and nullability
  changes also produced distinct JSON. This is a specific encoding gap.
- The source contains the exact printed Spark JSON and both true raw
  metadata values in the CI log. Do NOT conflate real Catalyst equality
  with its lossy JSON image.

**Coq lower-bound theorem:** `QueryProofJsonImpossibility.v` first
introduced in `061a10956a0275e7a7dffb1f858889f2445498d9`.
Initial Coq compile `39461873748` FAILED on equality rewrite
orientation; fix `9493ab3f297fc7f10f63cd83247f1180e0756168` compiled
under official Coq 8.18, CI **`39462049077` SUCCESS**. The theorem
proves a colliding serializer cannot have a total left inverse that
reconstructs EVERY distinct source object. A semantic-decoder
impossibility requires the ADDITIONAL premise that the colliding objects
have different meanings; no Spark wrong-answer or actual metadata
denotational difference is established. A strictly restricted source
domain with empty metadata can have an injective simplified representation,
but `TreeNode.toJSON` does NOT prove actual metadata was empty.

**Constructive narrow source-side repair:** Scala
`QueryProofNativeSourceBoundFrameWitness.scala`,
commit `6a07bdcf39078ca67db9b32360262aa3d2e35a39`;
workflow `queryproof-native-source-bound-frame.yml`,
commit `283443dddb4d8f060d0c8e4828709d6dbe44d9e3`.
GitHub Actions **`39462504637`**, job `118765311862`, SUCCESS.
For the SINGLE tiny admitted grammar `LocalRelation` with an EMPTY,
NON-STREAMING input and only `AttributeReference` output slots,
a separate native source frame captures full `a.metadata.json`,
`exprId.id`, `exprId.jvmId`, name, dtype, nullability,
non-truncated qualifier values, and `/output/INDEX` paths; metadata
and qualifier lengths have explicit upper limits. A two-column metadata
SWAP that leaves Spark's `toJSON` unchanged is DETECTED by the new
position-indexed frame. An unrelated `Project` plan is explicitly
rejected as `UNKNOWN`. This repairs one source-observation collision
in one tiny scope; it does NOT capture intra-rule transient mutation,
opaque private cache state, arbitrary Catalyst expressions, runtime SQL
semantics, or prove the native extractor matches a Coq interpretation.

A second, explicitly POSTHOC Coq development commit
`736e0e0c0cbaf7bdd9b247838e4cd306cb6ec571` extends the
impossibility file with an abstract ordered source-frame injectivity
and metadata-swap countermodel. Independent Coq CI **\`39462918891\` SUCCEEDED** under official
Coq 8.18, without `Admitted` or axioms. This kernel-checks a narrow
ordered-source-frame injectivity lemma and a metadata-swap model, not
the actual Scala emitter.

**Exact impact on old scores: NONE.** W32 prospective first run stays
16/32 structural; X stays 0/16 strict; V6/V7 posthoc stay 20/32
structural and 0/20 strict. This is new evidence about the reliability
of existing serialization as a proof bridge, not a new W case that
changed results. No historical first-run file or scoring definition changed.

**Critical next obligations:**

1. Treat raw Catalyst `toJSON` as a lossy *diagnostic*, not as a
   mathematical AST witness; reject unverified metadata or other erased
   fields rather than treating absence from JSON as evidence of absence
   from the native plan.
2. Make the source-bound frame complete for EACH recognized operator,
   including exact ordered field identities and actual metadata,
   qualifier, dtype, nullability, TreeNodeTag effects and rule caches,
   or discharge explicitly source-proven semantic noninterference.
3. Formalize and validate the specific Scala source-frame to typed
   Coq-interpreter mapping. Coq kernel correctness of an abstract
   record-based frame does not verify a Scala extractor.
4. Add mutation-site event witnesses or rigorously prove effects can
   be ignored for row bags; snapshots at rule boundaries cannot reveal
   state transitions that are reverted within one rule.
5. Only THEN freeze a new independent prospective corpus and evaluate
   whole-suffix proof+every hidden gate on the EXACT same cases, with
   predeclared UNKNOWN controls. Keep SQLSolver/VeriEQL comparisons
   within their official semantics and first-run provenance.

**Scientific label:** source-observer counterexample + conditional
impossibility proof + narrow source-bound prototype. NOT production
Spark correctness verification, no query-answer flaw, no upgraded
W32 headline.
