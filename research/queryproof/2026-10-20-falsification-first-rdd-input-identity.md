# QueryProof falsification-first checkpoint: actual input-identity counterexample

Date: 2026-10-20. Owner: Metin Hatipogullari. Repo: mhatipog/spark.
Research branch: research/queryproof-adversarial-tag-frame-20261020.
Parent frozen checkpoint: research/queryproof-generalized-liveness-20261020
at ac5cd4131d393e3801dac1831d2f982a6543f444.

**Executive verdict:** QueryProof's current unrestricted equality-of-Catalyst-JSON
premise is false as a statement about relational meaning, not merely about
auxiliary metadata. A deliberately invalid input-RDD substitution can be
accepted as both an exact scope identity and a local projection-liveness
certificate. Stop claiming soundness for arbitrary captured transitions until
input relation identity is attested independently of TreeNode.toJSON.

## Experiments actually executed

| Experiment | Free CI | Verified finding |
|---|---|---|
| Actual tag-map serialization collision | 39395989539 | Two distinct TreeNode tag maps, one tagged node each, same string digest |
| Spark native integrity vs local proof checkers | 39396623433 | A literal 1->2 value change preserving schema/ExprId passes native integrity; four local checkers return UNKNOWN. Schema type mutation is rejected by native. |
| Strong whole-trace tag quarantine, H retrospective | 39397090688 | 48/48 H executable; 30 formerly conditional certificates become 0; tags found in 48 cases |
| Strict post-FinishAnalysis tag quarantine, H retrospective | 39398426958 | 48 normalization cuts; tagged suffix states in all 48; again 30->0, no rowbag mismatches |
| Pinned SQLSolver (commit dcc2a91...) | 39397432925 | Three preregistered SQL pairs: NEQ, EQ, EQ (all expected) |
| Real LogicalRDD input collision and false local certificate | 39398930735 | Two actual Spark LogicalRDDs with identical toJSON and output IDs but differing backing RDD row bags; scope _chain_proof reports EXACT_IDENTITY; existing generalized projection liveness certifier accepts an adversarial Aggregate/Project event built with the real leaf JSON |
| Emergency opaque-leaf guard | 39399304940 | Same negative input substitution now UNKNOWN |
| JVM reference-token proof obligation prototype | 39399684412 | Minted distinct tokens for differing RDDs force UNKNOWN; same backing RDD token preserves positive local certificate |

All successful runs used free GitHub-hosted workers or official local Spark 4.2.0.
The first attempted real LogicalRDD control had a pre-execution Scala API type
error (classic SparkSession required), repaired before the first valid JVM
collision execution. The first refined suffix audit had an executor-name
assumption error, repaired by recognizing the actual Finish Analysis cut.
Neither harness failure is semantic evidence.

## The real false-certificate mechanism

Spark's `LogicalRDD` stores an `RDD[InternalRow]` backing object as a
constructor field. Spark `TreeNode.parseToJson` serializes unrecognized
objects as `JNull`. Therefore two leaves with identical output attributes,
but different backing RDDs and different row bags, have equal Catalyst JSON.

The existing `scope_ledger._chain_proof` accepts equal entry/exit JSONs as
`EXACT_IDENTITY` without additional leaf identity obligations.
`generalized_projection_liveness_certificate._certify` compares the base
subtrees using that serialized representation, then trusts their equality.
The injected test uses a known-positive Aggregate/Project restructuring, but
changes its hidden backing RDD simultaneously. The local checker returned
`EQUIVALENT_IN_FRAGMENT`, although Spark actually collected different values
from the two RDDs.

The test combines **real Spark-constructed leaf JSON** with an intentionally
synthetic Aggregate/Project context; it is not a production-rule execution
and not an unmodified Spark optimizer defect. It falsifies a checker premise
for adversarial transitions. No upstream Spark bug is alleged.

The separate tag collision comes from concatenating
`key.name + ":" + value.toString` and joining by `|` before hashing.
For example `{qpA: "one|qpB:two"}` and
`{qpA: "one", qpB: "two"}` produce identical strings. This is a
**serialization collision**, not a SHA-256 cryptographic collision.
It affects claims of full hidden JVM-state identity even without RDD drift.

## Precise status of old results

- H and K each recorded 30/48 changed optimizer suffixes with conditional
  certificates after Finish Analysis.
- The prospectively frozen L cohort remains 12/48, on 474 changed rule events,
  no finite Spark/SQLite rowbag mismatches, and no observed fingerprint drift.
- Those numbers may be retained as **historical, conditional certificates in
  a serialized observational fragment**, not as unconditional semantic proofs
  over arbitrary JVM states.
- The separate strict H test shows no changed suffix survives a conservative
  no-tag proof gate. This is loss of proof authority, not evidence the real
  Spark rules were wrong.
- The RDD negative control proves the original unrestricted local checker
  can return a false positive on an explicitly invalid transformation.
  It does not prove the 30 H/K or 12 L actual transitions were wrong.

## Trusted computing base and gaps

1. Actual rule/scope snapshots must be complete and contiguous. Existing
   logging, fastEquals event selection, missing-event handling and nested
   executor boundaries remain trusted assumptions.
2. JSON equality is **not** a semantic base-relation identity theorem.
   `RDD` references, tags, metadata and other constructor fields can be
   elided or lossy. Arbitrary opaque relation leaves cannot be admitted based
   solely on their output schema and JSON class.
3. The emergency `guarded_leaf_identity.py` rejects all non-intrinsic
   relation leaves unless a stronger model is developed. It is purposely
   coverage-destroying but blocks this false certificate.
4. `attested_leaf_identity.py` demonstrates a better **mathematical
   obligation**: unchanged backing-input identity, with tokens minted by a
   JVM `IdentityHashMap` allocator. The prototype uses tokens from a
   controlled standalone Scala experiment. It is **not yet** wired to live
   RuleExecutor events, so do not treat the prototype as recovered H/K/L
   proof coverage. Token authenticity and snapshot/event context binding
   remain future TCB obligations.
5. Constant logical leaves such as OneRowRelation can be directly modeled.
   LocalRelation, RDD, file/source relations, mutable external datasets and
   hidden expression state need explicit source semantics or opaque bindings.
6. The denotation is unordered logical row-bag semantics under a stable
   abstract input relation, deterministic total expressions and narrow
   schemas; it excludes physical order, errors, UDF effects, time-dependent
   sources, Analyzer, FinishAnalysis and arbitrary optimizer rules.
7. Finite Spark/SQLite matching tests do not prove unbounded equivalence.

## Existing tools and novelty comparison

Spark native `validateOptimizedPlan` checks structural integrity and schema
obligations; the executed 1->2 control is not meant to be detected by it.
SQLSolver (SIGMOD 2024) already handles Spark-derived SQL rewrites, 114/127 in
the publication and 118/127 in the later official artifact. Our pinned
three-pair run correctly returned NEQ/EQ/EQ. Its documented interface takes
SQL strings and a schema; the paper manually converted some logical plans
to SQL. This is strong prior work, not evidence QueryProof dominates.

HoTTSQL/Cosette, U-semiring, VeriEQL and Alive2 respectively provide other
formal denotational, algebraic, bounded-SQL, and compiler translation-validation
precedents. The still-plausible distinctive contribution is complete,
fail-closed **authentic in-process Catalyst trace provenance** linked to
mathematically justified local relation-preservation obligations. This gap
has not yet been convincingly benchmarked end-to-end.

## Decision and immediate next experiment

Do **not** expand grouping-expression grammar or freeze cohort M yet.
The next high-information experiment is a versioned, disposable,
real-RuleExecutor input-state observer:

1. Mint unique stable JVM identity tokens for supported opaque backing objects
   (first `LogicalRDD.rdd`) through an IdentityHashMap, not toString or
   identityHashCode alone.
2. Emit an independently bound input witness with each scope and rule-event
   old/new state, require complete counts, type/schema alignment and context.
   On unknown leaf, missing event, changed token or malformed marker, UNKNOWN.
3. Inject a deliberately invalid RDD-substitution rule in disposable Spark CI.
   Show the integrated checker rejects it while preserving a legitimate
   same-RDD local certificate.
4. Evaluate true post-FinishAnalysis H/K/L traces **retrospectively** to see
   what coverage survives. Do not relabel past runs prospective.
5. Review tag noninterference as a separate relational-semantics theorem or
   keep the strict quarantine. Do not equate hash stability with noninterference.
6. Only after these obligations are fixed, freeze an independently authored M
   corpus and compare complete event coverage, overhead and abstentions against
   native plan validation, SQLSolver and finite execution.

**Assessment:** the experimental methodology became stronger because it found
and repaired a real flaw in its own evidence. The present checker became less
credible as a broad validator, but the research question is more precisely
posed. A publishable advance now requires a trusted binding from genuine JVM
inputs to local proof obligations; raw syntactic coverage gains are premature.

Historical files, branch, manifest pins and previous prospective results were
not modified. No upstream PR was opened.

## Live RuleExecutor follow-up (completed 2026-10-20)

The next experiment proposed above **has now been executed** on the same
versioned research branch. All links below point to free GitHub Actions runs.

- Actual disposable Spark 4.2.0 source overlay, built from pinned scope/state/frame
  tracers and `instrument_input_identity.py` emitting a fourth `QUERYPROOF_INPUT`
  marker. Tokens are minted by a synchronized JVM `IdentityHashMap` over the
  actual `LogicalRDD.rdd` references.
- Actual `RuleExecutor` negative rule substitutes independent RDDs behind
  identical Catalyst JSON; the observer **rejects** the transition with
  `INPUT_REFERENCE_CHANGED`. Actual same-RDD project simplification **passes**
  the input witness `SAME_INPUT_SEQUENCE`. The authentic rule compiler,
  execution and fail-closed parser all passed in
  https://github.com/mhatipog/spark/actions/runs/39407217801 .
  Precisely: 2 changed rule records and 8 complete scope snapshots.
- A second run replayed frozen H01-H48 **retrospectively** with the same live
  observer: 438 changed rule records, 5,856 scope markers, 120 post-FinishAnalysis
  rule records. All 120 suffix rule records preserved actual input identity;
  30/30 earlier conditional certificates pass this *additional necessary*
  identity obligation. Across the entire trace, 48 rule records and 768 scope
  snapshots abstained on unsupported relation semantics, all before the proof
  suffix window. CI https://github.com/mhatipog/spark/actions/runs/39407609164 .
- A separate adversarial protocol suite exercises 10 malformed/reordered/
  incomplete witness cases; CI
  https://github.com/mhatipog/spark/actions/runs/39407833953 .

**Combined verdict:** Actual RDD identity integration repairs the demonstrated
false premise on the tested JVM input type and preserves all historical H
candidate suffixes at that particular gate. It does not restore *unconditional*
certificates: strict post-FinishAnalysis tag quarantine still excludes all 30.
The observer does not authenticate arbitrary external log tampering, does not
model non-RDD source leaves, and does not by itself validate any rewrite.
The positive test was an input-identity witness success, not a general proof
that arbitrary project rewrites are sound.

**Next highest-value falsification:** test changes to tags/hidden state during
rule invocations that return `fastEquals=true`. The current AST logger only
records *effective* changes and could silently omit in-place tag mutations;
a pre-rule/post-rule audit of **all** invocations is needed before trying to
relax the tag quarantine. Then separately derive (or refute) a formal
noninterference lemma for tags within the narrow logical row-bag fragment.

### Additional all-rule soundness counterexample

Actual Spark source injects a rule that mutates a plan's private tag in place
and returns the same JVM object. Thus `result.fastEquals(plan)` is true, and
the original QueryProof AST observer (effective changes only) emits no rule
event at all. The instrumented before/after observer, inserted at the actual
`RuleExecutor` invocation boundary, **detects the hidden mutation**. An
identical benign no-op has no false alarm. Successful free CI:
https://github.com/mhatipog/spark/actions/runs/39408827014 .

This is a second distinct blind spot in the *verifier's trace-completeness
assumption*, not a Spark optimizer bug. The all-rule observer is observational
and still uses a lossy tag-frame digest. Its absence-of-drift result, even if
confirmed over H, cannot prove semantic noninterference of tags or exclude
transient mutation followed by restoration inside a single rule call.

### All-rule H measurement: 26,112 actual calls, 0 observed silent deltas

The disposable pre/post rule observer has also been replayed on frozen H01-H48
in free CI https://github.com/mhatipog/spark/actions/runs/39408853888 .

- **26,112** total actual logical-plan `RuleExecutor` invocations, independently
  counted before the existing effective-rule event emitter.
- **438** effective rule calls, equal to all 438 existing captured AST events.
- **25,674** `fastEquals` no-op calls previously omitted from the AST trace.
- **0** observational pre/post hidden-state mismatches on those no-op calls,
  and **0** observed silent backing-input reference substitutions.
- In the **post-FinishAnalysis** window specifically: 11,904 rule calls,
  120 effective changes, 11,784 no-ops, **0 observed silent drift**.
- All 48 normalization cuts accounted for. The original 30/48 conditional
  suffix candidates survive this additional **necessary** trace-completeness
  check, independently of the earlier 30/30 live input gate.

The separately executed deliberately mutating no-op is *detected* by this
same observer, ruling out the trivial explanation that it never notices any
hidden changes. The outcome improves empirical credibility of the historical
H traces but **does not** make zero-drift a mathematical theorem. The tag-frame
observer is still noninjective (documented collision), and pre/post snapshots
cannot see transient intra-rule mutations that restore state before return.
The strict tag quarantine therefore remains 30 -> 0 for unconditional
certificates. Future work must separate denotational tag noninterference
from observational trace fidelity.


### H tag-key census, adversarial semantics controls and shape gate

New evidence collected after the all-rule H audit, pinned to **official Apache Spark
v4.2.0** (not a generic claim about all Catalyst versions):

1. A disposable RuleExecutor tag-key/type census was installed *after* scope,
   state, frame and input observers. A complete frozen H01-H48 retrospective
   execution recorded **438 changed-rule records**, **5,856 complete scope
   markers** and **48 post-FinishAnalysis suffixes**. All observed suffix tags
   were the exact name **dataset_id**, backed by JVM
   `scala.collection.mutable.HashSet`; there were **zero unknown tag-key
   names**. **8,064** tag occurrences count repeated captured snapshots, not
   independent tag instances. First complete audit:
   https://github.com/mhatipog/spark/actions/runs/39412775975 .
2. Real **independent positive and negative Spark JVM perturbation controls**
   were compiled against Spark 4.2.0. Two otherwise identical plans sharing
   the exact LogicalRDD backing reference, but holding different
   `dataset_id` sets, yielded the same optimized tag-erased AST and same
   *actually collected* row bag. Conversely, identical `Join` ASTs with
   vs. without a matching `processedJoinCondition` tag produced different
   outcomes under Spark's actual `PushExtraPredicateThroughJoin` rule
   (extra pushed `Filter` vs skipped), proving **blanket tag erasure unsafe**.
   These are *control cases*, not exhaustive proof over all plans:
   https://github.com/mhatipog/spark/actions/runs/39413743147 .
3. Strengthened the JVM observer to classify `dataset_id` collection
   **element type without printing its values**. Frozen H01-H48 replay found
   all **8,064** post-FinishAnalysis tag appearances were
   `dataset_id:HashSet[Long]` as observed at captured states. New
   fail-closed phase-local gate refuses unknown tag names,
   `processedJoinCondition`, malformed HashSets and non-Long members;
   its adversarial unit falsifiers passed. All **30/30** historical
   conditional H candidates pass this *additional necessary tag-shape
   obligation*, **0 rejected**, but no unconditional general soundness is
   asserted. Successful CI:
   https://github.com/mhatipog/spark/actions/runs/39414066476 .
4. Source audit via exact v4.2.0 implementation found
   `Dataset.DATASET_ID_TAG` defined/set in classic `Dataset.scala` and
   read by `DetectAmbiguousSelfJoin.scala`, an **analyzer** rule.
   GitHub's full-repo code search returns just three files for the token
   `DATASET_ID_TAG` (those two and a test), no optimizer consumer.
   `PushExtraPredicateThroughJoin.scala`, `PropagateEmptyRelation.scala`
   and `ConstantFolding` are counterexamples for treating *other* tags
   as inert. This is a **source-pinned bounded consumer search**, not a
   machine-checked closed-world call graph.
   Reference sources:
   https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala ;
   https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/analysis/DetectAmbiguousSelfJoin.scala ;
   https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PushExtraPredicateThroughJoin.scala .

**Narrow mathematical obligation.** Let `erase_D` remove only the
`dataset_id` annotation, preserving the exact full supported logical AST,
expression identities, schema/metadata, leaf relations and live backing-RDD
reference identities. For any supported, well-typed resolved relational plan
`P` with total deterministic expressions, assume the abstract logical
denotation interprets only those constructor fields and never `dataset_id`.
Then `[[P]]_rho = [[erase_D(P)]]_rho` follows by structural induction on
the admitted plan grammar. This is a **conditional model lemma**, not a
universal theorem about Spark's runtime. To lift the lemma to actual captured
rewrites, one must *also* establish source-version phase noninterference of
`dataset_id` for **every invoked optimizer rule**, complete and untampered
rule-boundary observations, stable underlying inputs, exhaustive supported
constructor semantics and valid local certificates. The phase restriction
matters: future Analyzer use and ambiguous self-join checking are excluded.
A rule's intermediate behavior may depend on other tags even if its ultimate
row-bag semantics is preserved.

**Status:** We have substantially reduced the tag-based obstacle from
uninterpreted metadata in every H suffix to one precisely recognized,
element-validated tag class, while proving a broad relaxation invalid.
The 30/30 number is **retrospective conditional eligibility**, not a newly
prospective 30/48 theorem and not yet an upstream-worthy validation claim.
Next: independently validate optimizer consumer coverage (including dynamic
tag access and generic mutable state), then freeze a fresh adversarial cohort
M that stresses input identity, plan annotation semantics and all-rule trace
completeness. Only then evaluate end-to-end proof coverage and comparisons.
No paid infrastructure or upstream PR was used.


### End-to-end live shape-gate falsifiers (completed)

The in-parser synthetic negative controls were insufficient by themselves:
they could not show that the JVM emitter, base64 record binding and parser
agree when a live plan actually contains a misleading or malformed tag.
A separately constructed real Spark 4.2.0 experiment addressed this
with four `RuleExecutor` identity-rule plans, each with a different tag
condition. The scope/context-bound observer classified all **16 expected
JVM scope snapshots** without printing actual tag values. Exact outcomes:

- `dataset_id:HashSet[Long]` **accepted**.
- Same tag name but forged `HashSet[String]` **rejected**.
- Recognized but not eligible `processedJoinCondition` **rejected**.
- Completely unknown key name **rejected** (key name remains redacted).

All 4 live fixtures executed, with no missing or crossed scope/observer
records and with source-pinned type/shape matching. Free CI:
https://github.com/mhatipog/spark/actions/runs/39414926580 .
This is an integrated negative gate test, not a certificate for the rule's
semantic behavior or exhaustive proof of tag discovery.

The targeted source query
`DATASET_ID_TAG repo:apache/spark` currently returns only the
`Dataset.scala` setter/definition, `DetectAmbiguousSelfJoin.scala`
consumer and one test. In the Spark Catalyst optimizer source path,
`dataset_id` has **zero** direct-string hits, while `getTagValue`
has a hit for `PushExtraPredicateThroughJoin` and `TreeNodeTag` has
three rule-file hits. This is useful narrowing evidence but **not an
exhaustive whole-program proof**: external/extended optimizer rules,
dynamic tags, inherited helpers, framework callbacks, alternative
planner paths and subsequent Analyzer calls still require boundaries.

We now have a plausible, version-pinned denotational tag-erasure exception
rather than a generic annotation assumption. Keep H figures labeled
**retrospective conditional** until a fresh prospective cohort and a
source-reviewed supported phase give independent evidence.
