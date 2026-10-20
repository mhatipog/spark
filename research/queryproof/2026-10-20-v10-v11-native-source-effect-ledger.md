# QueryProof V10/V11 — Native source-site effect ledger checkpoint
20 October 2026 · independent posthoc development · preserving frozen W32/X

## Non-negotiable baselines

Historical branch: research/queryproof-adversarial-tag-frame-20261020.
Development branch: research/queryproof-phase-local-independent-20261020.
W32 first prospective run [39435756545](https://github.com/mhatipog/spark/actions/runs/39435756545): **16/32 structural suffix candidates**, 16/16 prespecified targets and 32/32 finite Spark/SQLite rowbag matches.
X strict replay [39436715938](https://github.com/mhatipog/spark/actions/runs/39436715938): **0/16 original strict hidden-state guard acceptance**.
Posthoc V6/V7: 20/32 structural SAME-W32 candidates; 0/20 original strict guard; twelve unresolved W17–20, W25–32.
Coq proves conditional NULL/bag/frame/tag/effect theorems but NO mechanically verified Catalyst AST-to-Coq semantic bridge. This project has found NO Spark SQL wrong-answer bug.

Previous V8/V9 [checkpoint](2026-10-20-v8-v9-native-event-and-prerule-checkpoint.md) describes (a) metadata erased by TreeNode.toJSON, (b) invisible production ConstantFolding.FAILED_TO_EVALUATE write in ineffective rules, (c) a test-only transient set/unset missed even by pre/post snapshots.

All V10/V11 sources, tests and workflows are additive on the isolated branch. Source patches apply to disposable GitHub runners only. Spark is installed from official pinned PySpark 4.2.0; Scala 2.13.18 and Java 17. No frozen first-run source/manifest/test was modified.

## V10 — instrument actual Spark tag mutation source sites

Exact original upstream Spark TreeNode.scala blob e82e6a30b9bba41e637b27cbab43f26d9d23ff9b was freshly downloaded and its Git blob SHA verified BEFORE the instrumentation script could edit it. That official source contains:
- mutable tag map initialization (starts empty);
- copyTagsFrom and mergeTagsFrom direct bulk assignments;
- setTagValue direct map update;
- unsetTagValue direct map removal.

The temporary patch records supported direct tag SET/UNSET with JVM-local identity, monotone event sequence, tag name and Boolean old/new presence. It classifies tag bulk copying/merging and untracked tag keys as UNKNOWN; it does NOT capture source values, arbitrary rule caches, or other optimizer effects. There is a 2,500-event cap that emits UNKNOWN on exhaustion. It never stores unredacted metadata values in events.

Code:
- [V10 Spark source patcher](../../tools/queryproof_posthoc_v10_20261020/instrument_official_treenode.py)
- [V10 native production/transient/bulk witness](../../tools/queryproof_posthoc_v10_20261020/QueryProofNativeV10TreeNodeMutationWitness.scala)
- [V10 CI](../../.github/workflows/queryproof-native-v10-treenode-tag-effects.yml)

**First run [39471627017](https://github.com/mhatipog/spark/actions/runs/39471627017) SUCCESS**, head commit 47140f6265d9ce52a6eacce22830ed99cb00e112:
1. An actual production ConstantFolding failed ANSI conditional cast writes FAILED_TO_EVALUATE, structurally ineffective. V10 captures the underlying real source-site SET. Independent V9 pre/post tag evidence agrees. No tag bulk UNKNOWN occurred in the tested production path.
2. A TEST-ONLY transient rule sets and removes the tracked tag. V10 captures **two** events, in source order, with the SAME native object identity. V9 pre/post tag frames remain equal. This repairs exactly V9's transient-event observation loss in this restricted experiment.
3. A real copyTagsFrom bulk transfer from one native Cast to another emits UNKNOWN, and the target demonstrably receives the tag. This is a fail-closed supported-domain boundary.
No semantic-equivalence assertion follows from a write being observed.

## V11 — bind each source write to the RuleExecutor invocation

V10's mutation log did not assign causal rule identity. V11 adds a source-level ThreadLocal LIFO invocation scope around actual RuleExecutor rule(plan) calls, with enter/exit in try/finally. Mutations carry a source-specific invocation ID, distinct from a node ID or plan equality, and protocol markers have their own V11 name and field count; there is no silent reuse of V10's old wire schema. Direct tag writes outside a scope become UNKNOWN. Nested rule invocations return to the outer scope on exit. Unsupported bulk and untracked tags still return UNKNOWN.

Code:
- [V11 scoped source patcher](../../tools/queryproof_posthoc_v11_20261020/instrument_rule_scope.py)
- [V11 native actual rule, nested, and out-of-rule controls](../../tools/queryproof_posthoc_v11_20261020/QueryProofNativeV11RuleScopedTagWitness.scala)
- [V11 CI](../../.github/workflows/queryproof-native-v11-rule-scoped-effects.yml)

**First run [39472533560](https://github.com/mhatipog/spark/actions/runs/39472533560) SUCCESS**, head commit 77500235e8c75b49b89cfe3d0c9ec4aad263129f:
1. An actual ConstantFolding failure-tag write references its single matching RuleExecutor invocation ID. Its logged before/after agrees with independent V9; zero V11 UNKNOWN events in this tiny production example.
2. Deliberately nested inner/outer RuleExecutors yield distinct invocation IDs; real tag writes map OUTER, INNER, INNER, OUTER on the SAME Cast object. Scope end order is INNER before OUTER, verifying LIFO provenance and context restoration.
3. A direct native tag write outside any RuleExecutor scope is UNKNOWN; it never produces an accepted V11 direct-write event.

## Precise epistemic boundary

These are demonstrated native **observations**, not machine-checkable SQL correctness certificates. Even correctly logged writes are not proof that tag changes are semantically irrelevant. Current protocol neither:
- guarantees logs cannot be truncated/dropped/fabricated;
- has a sealed final event count or external independent verifier;
- proves the source hooks cover all optimizer state beyond the explicitly audited TreeNode tag methods;
- handles cross-thread state, reflective mutation, all tags/metadata, rule caches, or all non-rule-state effects;
- mechanically relates source Spark plans and effect logs to the Coq NULL/bag SQL semantics.

The existing Coq QueryProofConstantFoldingFailureTag.v model proves a narrowly **conditional** failure-tag denotation-inertness result, but does NOT establish a verified source-to-model link. The strict X 0/16 result is still an operative negative finding.

## Next research experiment

V12 should add an **unforgeable-by-accidental-truncation event envelope** for this exact scoped one-tag model: source-side terminal SEAL with total mutation/scope counters and a rolling digest over each event, plus an independent fail-closed verifier. Negative tests must delete, duplicate, reorder, mutate, misattribute, inject UNKNOWN and truncate real logged events, all yielding REJECT/UNKNOWN. No cryptographic tamper resistance against code with the process's logging authority should be claimed. Only after that should a separately verified Spark/Coq interpretation and source-derived noninterference proof be attempted; keep unsupported W32 families UNKNOWN until source obligations are discharged.

A publishable differentiator may be an actual **effect-indexed proof-carrying Catalyst execution trace** with checkable provenance, exact source bounds and negative controls. It is not rediscovery of SQL equivalence or a claim of stronger present coverage than SQLSolver, VeriEQL or the Coq SQL formalization literature.
