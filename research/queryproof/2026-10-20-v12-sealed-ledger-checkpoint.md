# QueryProof V12 — sealed source-event ledger: first-run checkpoint

Date: 2026-10-20. Researcher: Metin Hatipogullari.
Branch: research/queryproof-phase-local-independent-20261020.
V12 first source implementation commit: 044f26c56935a4966ca2eef7434543702815e94b.
**FIRST NATIVE RUN — DO NOT REPLACE:** [GitHub Actions run 39475275862](https://github.com/mhatipog/spark/actions/runs/39475275862);
job 118809515572; success on first run. The Actions artifact
queryproof-v12-native-sealed-stream (ID 12288373077) contains
native stdout including the Base64-encoded exact event transcript.

## Frozen baselines (never changed by V12)

- W32 prospective first run [39435756545](https://github.com/mhatipog/spark/actions/runs/39435756545),
  SHA 1d56bb97e2327ce9f8dd0e5aebccb243140c3ee8: 16/32 complete
  structural suffixes, all 16 prespecified targets met, 32/32 finite
  Spark-vs-SQLite row-bag agreements.
- Strict X first run [39436715938](https://github.com/mhatipog/spark/actions/runs/39436715938):
  **0/16** pass original strict global hidden-state guard.
- Posthoc Y/V6/V7 can produce 20/32 structural candidates, **0/20**
  satisfying the original strict guard. Nothing in V12 upgrades
  structural certificates to sound full optimizer correctness proofs.

## Question and methods

Can an independent checker distinguish an intact actual Spark source-site
tag-effect stream from streams with deleted, altered, duplicated, reordered,
malformed or truncated messages without confusing integrity and authenticity?

1. Official Spark 4.2.0, Scala 2.13.18, Java 17. Upstream
   TreeNode.scala Git blob e82e6a30b9bba41e637b27cbab43f26d9d23ff9b
   pinned before disposable source patching (V10 > V11 > V12).
   V9/V11 RuleExecutor overlay also compiled in disposable CI.
2. Native Spark executes production ConstantFolding with an ANSI
   Cast failure (one FAILED_TO_EVALUATE SET); separate nested
   OuterRule/InnerRule on one Cast writes SET/UNSET/SET/UNSET.
   Three RuleExecutor invocations and five TreeNode tag mutations.
3. V12 source-hook logger serializes each V11-channel marker and
   Base64 UTF-8 fields, updates SHA-256 over exact line plus LF,
   counts emitted and UNKNOWN events, then writes a final seal
   (query, event count, last mutation serial, scope serial,
   unknown count, active scope depth and checksum).
   The seal does **not** cover separate V9 diagnostic records
   or all Catalyst state.
4. Separate Python verifier independently checks exact line encoding,
   checksum, count, contiguous sequences, nested LIFO, active rule
   mutation attribution, supported tag/op transitions and fixed
   first native witness expected rule identities.
5. Adversaries mutate the actual captured native transcript, not
   a Python-simulated substitute. First run and artifacts preserved.

## FIRST-RUN RESULTS

- Official Spark TreeNode source Git blob confirmed and native Scala
  RuleExecutor plus TreeNode overlays compiled.
- Exact trace: **11 events; 3 nested/serial invocations; 5 mutations**.
  Source-stream SHA-256:
  **9ac4825ad4928d598508c8c15d088702fb5d89ea3b95692565005c772bad346a**.
- Independent verifier emitted QUERYPROOF_V12_INDEPENDENT_TRACE_INTEGRITY_VERIFIED.
- **12/12 damage controls rejected**: drop, duplicate, reorder,
  wrong operation, wrong active scope, injected UNKNOWN, missing seal,
  append after seal, wrong digest, malformed Base64, truncation,
  altered closing marker. Since some fail checksum first, do not
  claim all independently exercised deeper semantic constraints.
- **Critical negative:** changed a production mutation's Cast
  identity and recomputed the unkeyed checksum/seal. The verifier
  **accepted** the coherently forged transcript:
  QUERYPROOF_V12_RESEALED_FORGERY_ACCEPTED_BY_UNKEYED_VERIFIER.
  This is a falsification of any stronger authentication claim.
- CI run remained green *because* it was designed to demonstrate and
  record this forgery acceptance.

## Scope of inference: deliberately narrow

- SHA-256 checksum guarantees only internal consistency relative
  to the supplied seal, **not trace authenticity**. Whoever controls
  both the stream and seal can forge both.
- No proof of complete instrumentation for all Catalyst effects:
  bulk tag changes, all other mutable objects, uninstrumented state,
  concurrency, earlier/later analysis phases and missed source hooks
  remain possible. Events omitted at emission are not recovered
  by a checksum of what was emitted.
- Observing real Spark tag writes is NOT proving noninterference or
  SQL bag/NULL equivalence. Source-to-Coq verified interpretation
  does not yet exist. The original X guard remains a failure.
- All code is posthoc on a separate development branch; historical
  W32 first-run data never relabelled as prospectively proved.

## Additive files

- tools/queryproof_posthoc_v12_20261020/instrument_seal.py
- tools/queryproof_posthoc_v12_20261020/QueryProofNativeV12SealedStreamWitness.scala
- tools/queryproof_posthoc_v12_20261020/verify_v12_stream.py
- .github/workflows/queryproof-native-v12-sealed-stream.yml

## Next

1. Disentangle event integrity, emitter authenticity, source
   completeness and semantic soundness as separate proof obligations.
2. Mechanize typed, nullable Spark AST to Coq bag-meaning mapping;
   demand tested proof premises, not tag-observer heuristics.
3. Falsify the observation boundary by suppressing a source event,
   using bulk copy/merge, mutating after seal, altering other
   optimizer fields and using concurrent threads.
4. Audit W17–20 INNER_LT, W25–28 DISTINCT_PAIR and W29–32
   HAVING_COUNT with a sound finite-model oracle and matched
   SQLSolver/VeriEQL benchmarks.
5. Preserve strict-X original 0/16 as the acceptance target unless
   a formally justified new property is explicitly separate.

Any later corrections require a distinct commit and CI run and must
never overwrite this first-run provenance.
