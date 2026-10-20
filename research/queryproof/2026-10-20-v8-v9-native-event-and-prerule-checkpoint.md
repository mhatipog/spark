# QueryProof — V8/V9 native optimizer-state continuation

20 October 2026. POSTHOC DEVELOPMENT, not a revised prospective W32 result.

Research repository: [mhatipog/spark](https://github.com/mhatipog/spark).
Frozen branch: research/queryproof-adversarial-tag-frame-20261020.
Isolated development branch: research/queryproof-phase-local-independent-20261020.

## Immutable baselines

- W32 prospective first run [39435756545](https://github.com/mhatipog/spark/actions/runs/39435756545), pinned commit 1d56bb97e2327ce9f8dd0e5aebccb243140c3ee8: **16/32 structurally complete optimizer suffixes**; all 16 prespecified targets; 32/32 Spark/SQLite finite row bags agree.
- X strict hidden-state replay [39436715938](https://github.com/mhatipog/spark/actions/runs/39436715938): **0/16 accepted** by original global hidden-state guard.
- Posthoc V6/V7: **20/32** structural same-case candidates, but **0/20** pass unchanged original strict guard. W17–20 (INNER_LT), W25–28 (DISTINCT_PAIR) and W29–32 (HAVING_COUNT) remain unsupported.
- Conditional Coq NULL/bag/frame/effect theorems are kernel-checked, but NOT mechanically connected to real Spark's AST semantics.
- The comprehensive predecessor checkpoint is [here](2026-10-20-round-y-z-v6-coq-handoff.md).

All V8/V9 sources and workflows are new files on the independent development branch. Original frozen W, X, tracer source, fixtures, and first-run results were not edited. Compilation patches only disposable GitHub Actions checkouts.

## Prior source-level falsification

Official Spark TreeNode.scala pinned blob e82e6a30b9bba41e637b27cbab43f26d9d23ff9b deliberately maps Metadata to empty JSON. Yet real Spark AttributeReference equality considers metadata. Native [39463269639](https://github.com/mhatipog/spark/actions/runs/39463269639) therefore demonstrated a genuinely effective metadata-only RuleExecutor event whose captured before/after JSON fields were IDENTICAL. This establishes lossy **observer transport**, not a wrong Spark query result. A separate path-indexed source frame precursor passed [39462504637](https://github.com/mhatipog/spark/actions/runs/39462504637).

## V8: attach a source-bound frame directly to the effective event

First run [39467630367](https://github.com/mhatipog/spark/actions/runs/39467630367): **SUCCESS**; commit e5805d33ab8578f5959e6c06c2777e0606ff44d9; pinned Spark 4.2.0 and Scala 2.13.18.

- [Ephemeral V8 instrumenter](../../tools/queryproof_posthoc_v8_20261020/instrument_v8_trace.py)
- [Native adversarial witness](../../tools/queryproof_posthoc_v8_20261020/QueryProofNativeV8EventWitness.scala)
- [Native CI workflow](../../.github/workflows/queryproof-native-v8-source-bound-event.yml)

Original seven-field event remains unchanged, proving the legacy before/after JSON collision. A new nine-field QUERYPROOF_AST_V8 event contains distinct metadata in source-bound before/after frames, complete positional output path and JVM ExprId, from real Spark objects. Exactly one production-rule-executor test-only metadata replacement succeeds. A genuinely effective unsupported Project rewrite emits QUERYPROOF_AST_V8_UNKNOWN instead of a complete V8 event.

Supported observational frame is deliberately tiny: empty, non-streaming LocalRelation with 1–64 AttributeReference outputs, source metadata, output paths, full ExprId, name, datatype, nullability and qualifier; size guards reject oversized fields. **This is not a general complete Catalyst source state or a semantic certificate.** Frames are sampled after the rule returns; mutable in-place pre-state might already be lost.

## V9: read the source BEFORE the actual production rule

Official Spark 4.2.0 ConstantFolding source is expressions.scala pinned blob 929ffdef2b52ff1aabdc6f38501dc131e7938fd2. The real rule's FAILED_TO_EVALUATE tag is set when conditional folding fails and later read to avoid repeated evaluation. It is intended optimizer state, not a Spark bug.

First run [39468400233](https://github.com/mhatipog/spark/actions/runs/39468400233): **SUCCESS**; commit 4c24603e76b31324218591415e56374360639d47.

- [Ephemeral pre-rule V9 instrumenter](../../tools/queryproof_posthoc_v9_20261020/instrument_v9_prerule_tag.py)
- [Native actual-rule tag witness](../../tools/queryproof_posthoc_v9_20261020/QueryProofNativeV9PreRuleProductionTagWitness.scala)
- [Native CI workflow](../../.github/workflows/queryproof-native-v9-real-prerule-tag.yml)

A production ConstantFolding invocation attempts an invalid ANSI integer cast in a conditional expression. On the actual Spark Cast object, FAILED_TO_EVALUATE changes **false -> true**. The structural logical plan and original toJSON stay unchanged; the original tracer and V8 emit **no effective-rule events**. V9 attaches a before-invocation observation and records a matching after-invocation observation even when fastEquals returns unchanged. Thus it logs that actual tag change. An effective Project with no Cast is explicitly UNKNOWN, not accepted.

V9 observes one source Cast tag in a Project with exactly one Cast. It cannot certify other tags, sources, mutable caches, other Spark rules, semantic noninterference or transient effects.

## V9 negative control: even the improved snapshots are insufficient

First run [39468822236](https://github.com/mhatipog/spark/actions/runs/39468822236): **SUCCESS**, commit 45ec2036378386720403084c53d107e5897e1da1.

- [Transient-tag falsifier](../../tools/queryproof_posthoc_v9_20261020/QueryProofNativeV9TransientTagCounterexample.scala)
- [Native CI workflow](../../.github/workflows/queryproof-native-v9-transient-tag-falsifier.yml)

An explicitly TEST-ONLY rule calls real Spark setTagValue and then unsetTagValue on the tracked FAILED_TO_EVALUATE tag, verifies BOTH operations, and returns the identical plan. The tracked V9 before/after tag frames are identical and original/V8 structural events are absent. This refutes **equal boundary snapshots imply no intervening hidden mutation**, even for the exact tag V9 is tracking. It does NOT show a difference in SQL result semantics or an actual Spark production rule performing this transient behavior.

## Three distinct evidence failures

| Mechanism | Native evidence | Necessary design response |
| --- | --- | --- |
| Metadata is erased by Catalyst JSON | Pre-V8 metadata-collision effective event | Bind frames directly to full supported source fields, or UNKNOWN |
| Structural effectiveness filters hidden tag mutations | V9 real production ConstantFolding | Log relevant observations around **all invocations**, including ineffective ones |
| Pre/post snapshots miss set-then-clear effects | V9 test-only transient witness | Source-proven noninterference or trusted effect-write hooks, not snapshot equality alone |

V8/V9 complete events denote narrowly scoped **observations only**. No global kernel proof checker consumes those new events yet. UNKNOWN emission by the producer is NOT, by itself, a checked whole-trace certification algorithm. Do not count either as a revised W32 success or assert that they prove Spark equivalence.

## Most useful next experiment

Create a new isolated, source-pinned effect log from actual Spark tag-write/clear sites for ONE known key, with a tamper-evident invocation sequence and explicit gaps/UNKNOWN. Establish source-level correspondence between intercepted writes and the logged events; exercise real-rule positive, test-only transient, missing-hook, and interleaved-identity negative controls. Then mechanize the corresponding effect-noninterference obligation and AST-to-Coq translation for a narrowly supported optimizer rule before expanding to inequality, DISTINCT or HAVING shapes. Preserve all first runs and compare with SQLSolver/VeriEQL only on aligned held-out SQL and assumptions. Source-verified proof composition is the potential scientific novelty; a higher posthoc structural acceptance fraction is not.

No Spark wrong-answer bug has been found. The most uncomfortable result, X 0/16 strict, remains fully operative.
