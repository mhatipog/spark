# QueryProof — V10/V11: live Catalyst guard witnesses checked by Coq (21 October 2026)

## Executive scientific result

This is a first **conditional source-to-proof pipeline** for one actual
production Spark Catalyst optimizer rule shape. The fixed retrospective W32
cohort produced **four** `InferFiltersFromConstraints` guard-insertion
events (W17–W20) that were reflected into concrete Coq certificates.
The Coq 8.18 kernel checked each emitted witness against a universally
quantified, executable proof-checker soundness theorem for SQL-style
nullable SIGNED integer key bags with duplicates.

The first independent parser cross-check V11 **passed** on the same four
actual events. It does not import the V9/V10 syntactic recognizer.

**The milestone is NOT a proof of correctness for arbitrary Spark or all
24 accepted suffixes.** The reflection of Catalyst data and the source-native
hidden-state bridge remain outside the Coq kernel and must be audited.

## Immutable historical scores

| Evidence | Result | Status |
| --- | --- | --- |
| W32 first prospective frozen experiment, CI 39435756545 | 16/32 structural suffix certificates | Immutable |
| X first strict hidden-state guard, CI 39436715938 | **0/16 accepted** | Immutable |
| V9 posthoc independent structural replay, CI 39489786733 | **24/32** (W01–W20, W21–W24) | Retrospective only |
| V10 Coq universal guard-certificate checker, CI 39548890793 | Kernel accepted | **Only formal key-fragment semantics** |
| V10 real W32 witnesses to Coq, CI 39549154501 | **4 real events, 4/4 kernel-checked instances**, 32/32 row oracles | Retrospective, Python reflection |
| V11 independent Catalyst binding, CI 39549845950 | **4/4 independent binder agreement; 15/15 synthetic tests; 12/12 live-event mutants rejected; Coq PASS** | Retrospective, incomplete source trust |

A single Coq proof instance may correspond to one changed rule event, not an
entire optimizer suffix. Never add four to the 24/32 structural count, or
convert those counts into a measured probability of semantic soundness.

## Concrete files

- `research/queryproof/proofs/QueryProofGuardCertificate.v`:
  Coq executable `check` accepts only strict nullable equality or strict
  integer inequality; for LEFT OUTER, it forbids a new filter on the preserved
  LEFT side. `checked_certificate_sound` proves equal finite output bags
  (indeed equal canonical output lists) for every nullable signed Z-key input
  bag, including duplicates and correct outer-padding. Null-safe comparison
  and preserved-side outer-join filters are rejected.
- `tools/queryproof_v10_guard_kernel_20261021/run_v10.py`:
  W32 replay, narrow source-event candidate production, SHA256 binding to
  both encoded plan snapshots, ephemeral generated Coq instances, no raw AST
  or SQL literal exported in the new certificate artifact.
- `tools/queryproof_v11_independent_20261021/guard_binding_v11.py`:
  separate fail-closed TreeNode preorder decoder, exact unchanged
  Sort/Aggregate/Project/Join frame matching, direct strict predicate and
  cross-side integral ExprId binding, exact IsNotNull-filter insertion,
  LEFT OUTER preserved-side rejection, identity Project/leaf checks.
- `tools/queryproof_v11_independent_20261021/test_guard_binding_v11.py`:
  15 independent positive/negative tests.
- `tools/queryproof_v11_independent_20261021/run_v11.py`:
  checks independent reflection agrees *field for field* with V10,
  then rejects NULLSAFE substitutions, OR operator substitutions and
  foreign ExprId guards on each captured real positive event.
- `.github/workflows/queryproof-v10-coq-live-guard.yml` and
  `.github/workflows/queryproof-v11-independent-coq-binding.yml`.
- CI artifact in V11 contains a redacted JSON score and disposable Coq
  live-instance source (case IDs, structural assumptions, encoded-plan-pair
  SHA256; no original AST).

## Why this is more than finite test agreement

The checker is a *computed condition*, not an `Admitted` lemma or a trusted
Python status. Coq proves

`check(c)=true -> execute(c, guardedInputs)=execute(c, originalInputs)`

for arbitrary finite nullable signed integer key bags. It proves both
guard-side implications for strict comparison, equality of INNER bag
multiplicities and preservation of LEFT OUTER unmatched padding. CI
mechanically validates certificates derived from real optimizer events.
A bad guard-side choice or null-safe predicate cannot satisfy the checker.

V11 separately reconstructs source-emitted structural facts and rejects
multiple adversarial variants on actual event snapshots; this reduces
the chance of a mistaken Python field interpretation while keeping the
original V9 checker unchanged.

## Precise threat model and unsolved trust boundary

1. **Noninjective native serialization.** In actual Spark 4.2.0,
   `TreeNode.toJSON` erases relevant native `Metadata` in some cases.
   Earlier native test CI 39463269639 exhibited an effective metadata-only
   rule event with equal JSON. Equal JSON does NOT entail equal Spark state;
   even V11's two parsers cannot restore erased information.
2. **No Coq-verified Catalyst decoder.** The two Python translators are
   independently implemented but NOT formal proofs. A correlated incorrect
   interpretation or a misleading source serialization remains possible.
3. **Restricted value semantics.** Coq models signed nullable integer
   join-key values, SQL true/false matching and bag multiplicity.
   It does not model all projected payload columns, aggregation/HAVING,
   SQLConf/ANSI errors, casts, timezones, UDFs, physical ordering or RDD
   execution. The unchanged frame must be separately justified.
4. **Mutable optimizer effects.** Spark may write tag/cache state even
   when `fastEquals` sees no rule change; native production witnesses
   and transient tag-write falsifiers exist. Neither this Coq checker nor
   V11 proves source-state noninterference for the complete optimizer suffix.
   Frozen X **0/16** cannot be upgraded by these tests.
5. **Already-seen holdout.** W32 had been examined in earlier rounds:
   this is developmental validation, not new prospective predictive
   performance or a statistically independent sample.

## Most valuable next hypotheses

**H1 — native event binding**: attach a path-indexed SOURCE-side complete
attribute/frame observation (including metadata) directly before and after
a real W17–W20 `InferFiltersFromConstraints` invocation. An unsupported
attribute type, native-state drift, dropped event, or absent source
attestation must yield UNKNOWN. Test metadata-only collisions and transient
tag mutations, not only structural JSON substitutions. This narrows
the remaining source-observation uncertainty without pretending a hash is
injective.

**H2 — a verified symbolic plan pair**: have Coq check a typed *before and
after plan relation* and not just two booleans claiming guards. A Python
translator may construct the terms, but the kernel must verify the entire
restricted rewrite's structural relation and semantic theorem. The
source-to-Coq term mapping remains an explicitly unproved boundary.

**H3 — unseen prospective cases**: freeze new randomized NULL/duplicate
join inputs, eligible and ineligible source shapes, negative mutation family
and acceptance protocol BEFORE running Catalyst. Keep old W32 as development
data. Do not conflate per-rule correctness with end-to-end suffix correctness.

## Status

**Scientific improvement, not a solved Spark correctness claim.**
The formal model, event reflection, and independent adversarial validation
form a promising small proof-carrying component. Soundness of the full
`Catalyst source -> observations -> Coq semantics -> full suffix` chain is
an open research objective. The 24/32 posthoc structural score and 0/16
strict legacy-state score must both remain visible.
