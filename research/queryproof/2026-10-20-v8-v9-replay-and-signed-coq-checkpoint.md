# QueryProof — V8/V9 verified experimental checkpoint (20 October 2026)

## Purpose and immutability

All work on `research/queryproof-phase-local-independent-20261020`. Frozen W32
first-run is **16/32 prospective structural**, frozen strict X is **0/16**
hidden-state accepted. These numbers are not superseded by the posthoc replays.

## V7 CI defect, reproducible fix

Initial V7 run [39487376479](https://github.com/mhatipog/spark/actions/runs/39487376479)
failed before collecting tests: a generated Python header contained literal
backslash-n characters. The development ledger was fixed and
[39487946029](https://github.com/mhatipog/spark/actions/runs/39487946029)
passed five negative/positive structural tests. Do not count a commit as
experimental evidence until the matching CI run and logs are inspected.

V7 `_chain_proof(P,P, steps)` now rejects uncertified or disconnected
intermediate steps; identity is accepted only with **zero recorded** events.
Nested executor identity cannot override an uncertified batch.

## V8 independently replayed W32 against original V6

Run [39488462714](https://github.com/mhatipog/spark/actions/runs/39488462714)
PASS; paired run [39489194954](https://github.com/mhatipog/spark/actions/runs/39489194954)
PASS with enforced `COMPLETE_ALL_RULE_AUDIT` and case-attributed blockers.

- 32/32 SQL workloads executed; Spark/SQLite finite row oracle matched.
- 308 effective changed-rule AST events recorded, with global 2,500
  capture overflow instrumentation compiled and no overflow marker detected.
- 1,716 event-free batch scope identities; 76 batch certified chains;
  96 uncertified batch chains (includes scope repetitions);
  64 executors include uncertified batches.
- V6 20/32 posthoc structural; V8 stricter record-chain 20/32;
  0 lost, 0 newly gained. Exact cases W01–W16, W21–W24.
- 12 failing suffixes due to UNKNOWN_RULE, rather than trace inconsistency.
- Hidden-state *semantic* soundness is **not established** by an
  observational hash, even if all recorded rule events match their accounting.

Note: `queryproof.ast.enabled` records rules when Spark's
`!result.fastEquals(plan)`. The all-rule audit covers fastEquals no-op
invocations and observational drift, but still cannot rule out a temporary
in-place mutation, observer collision or unmodelled SQLConf/effects.

## V9 new narrow posthoc inequality fragment

Run [39489786733](https://github.com/mhatipog/spark/actions/runs/39489786733)
PASS.

- Extends *only direct cross-input integral* `LessThan(AttributeReference,
  AttributeReference)` alongside prior direct `EqualTo` within the
  recognized exact join/pruning/filter contexts.
- Crucial: inference/pushdown use `_condition_v1`, **not** the V6 OR
  recognizer: using an individual OR branch to justify an IsNotNull guard
  would be unsound.
- Structural W32 replay **24/32**, new W17–W20, no regressions.
- >=12 strict-inequality local changed-rule mutant controls tested:
  replacing strict LessThan by EqualNullSafe on both snapshots must return
  UNKNOWN; every control satisfied this requirement.
- Remaining W25–W28 DISTINCT_PAIR: `ReplaceDistinctWithAggregate` plus
  `ColumnPruning` new aggregate frame.
- Remaining W29–W32 HAVING_COUNT: `ConstantFolding`,
  `ColumnPruning`, `InferFiltersFromConstraints`, `PushDownPredicates`.
- This was **development on the already seen W32**, not independent holdout
  performance. Never report 24/32 as a prospective success probability or
  fully verified optimizer correctness.

## New signed nullable comparison theorem

`research/queryproof/proofs/QueryProofSignedLessThanBag.v`, Coq workflow
[39490443383](https://github.com/mhatipog/spark/actions/runs/39490443383)
PASS. Coq 8.18 kernel accepted lemmas for signed `Z.ltb` values, SQL NULL
strictness, left/right non-NULL guards, INNER/LEFT OUTER bag join preservation,
the left-preserved-side counterexample, the nullsafe-equality counterexample,
and exact duplicate multiplicities. No axioms or admits.

This is an **abstract semantic law**, not a verified translation of
Catalyst JSON to typed Coq, Spark `LessThan` code, runtime SQLConf,
physical execution, or state-effect noninterference.

## Next decisive work (priority, not an unsupported score prediction)

1. Construct a small **independent proof-object checker** for a narrow
   signed-nullable join fragment. Explicitly reject unrecognized JSON
   keys, type conversions, non-pure expressions and configuration.
2. Source-pin Spark 4.2.0 `ColumnPruning`,
   `InferFiltersFromConstraints` and `PushDownPredicates` subcases;
   extract or check AST-to-typed-semantics bridge obligations. A theorem
   proven in Coq is not a proof of actual Spark execution until this bridge.
3. Qualify per-step hidden-state effects / conditional semantic congruence,
   not hash equality. In particular certify no relevant in-place tag/metadata
   effect that `fastEquals` would miss.
4. Freeze **new unseen holdout** before considering V9 prospective coverage;
   run mutation campaigns that include nullsafe, OR inference, side-switch,
   exprId collision, duplicate bags, and error/overflow boundary cases.
5. Only then choose DISTINCT/HAVING as next coverage targets: they require
   genuinely different algebra and must not be admitted as ad-hoc whitelist.

## Artifact and workflows

- `tools/queryproof_v7_strict_20261020/`: strict ledger,
  overflow instrumentation, unit tests, V8 paired scorer.
- `tools/queryproof_v9_inequality_20261020/`: isolated signed LessThan
  structural candidate, exact filter checks, mutation runner.
- `.github/workflows/queryproof-v8-w32-strict-replay.yml`
- `.github/workflows/queryproof-v9-inequality-replay.yml`
- `.github/workflows/queryproof-coq-local-laws.yml`

## Research verdict

The novelty is growing in adversarial, independently accounted *structural
coverage* and formal component semantics. The major unsolved publication
claim remains genuine **proof-carrying Catalyst optimizer soundness**, because
the AST/Coq interpretation and hidden-state transition proof have not yet
been established. Preserve this distinction in every handoff.
