# QueryProof R32 — independent prospective join-step holdout, FIRST RUN

**Date:** 2026-10-20  
**Owner:** Metin Hatipogullari  
**Branch:** `research/queryproof-adversarial-tag-frame-20261020`  
**Scientific status:** prospectively frozen synthetic *local rule-event* holdout, NOT full optimizer-suffix proof, NOT general Spark query-equivalence proof, and no upstream PR.

## Summary with immutable measurement distinctions

The strict local two-sided join-pruning checker was developed **posthoc** on genuine previously seen M29/M33 `ColumnPruning` transitions (both certified; 20/20 mutation negatives rejected) in [Q run 39428615179](https://github.com/mhatipog/spark/actions/runs/39428615179). That development was NOT a holdout. Its theoretical basis was an explicit finite bag, SQL NULL, join/outer join lemma independently checked on 288,800 small relational models in [P run 39423911030](https://github.com/mhatipog/spark/actions/runs/39423911030). The algebra is a known type of relational identity, not a claimed newly invented database theorem.

**R32 then independently passed its *first* fresh, prenumbered and source-pinned test:** 32/32 Spark 4.2.0 cases executed, zero execution/report errors, zero finite Spark–SQLite unordered row-multiset mismatches; **16/32 changed `ColumnPruning` rule events** received an exact local join-projection certificate. This was **16/16** deliberately target-family instances and **0/16** intentionally unsupported challenges. Every recorded case had exactly one changed `ColumnPruning` event; the score denominator is therefore neither padded by cases without events nor based on manually sampled rule changes.

**However, full changed post-FinishAnalysis optimizer-suffix certificates remained 0/32 using the pre-existing v2 suffix scorer.** The Q checker was deliberately not substituted into v2. Local proof success must not be silently promoted to complete chain success. The all-rule, JVM state, frame, RDD-input, and tag audits completed, but the conservative full-suffix candidate baseline was zero, so each separately eligible gate count was zero. They are necessary observers, not additional local-event proofs.

## Freeze and reproducibility

- Frozen corpus: `tools/queryproof_prospective_r_20261020/CASES.json`, source of truth, new seed `2026102031` with unsigned-32 LCG `1664525*state+1013904223 mod 2^32`, 32 distinct synthetic table instances each with **27 left and 19 right rows**, including deliberate duplicate and NULL rows and shared join keys. This is not reused M/N/O data.
- Frozen protocol: `tools/queryproof_prospective_r_20261020/PROTOCOL.json`, 11 exact verifier/observer Git blob SHA pins, with `frozen_verifier_commit=49a4da96cfbef9dd649ed81e8d1b7c3ecf95d6c8`. One frozen Q checker source `tools/queryproof_join_pruning_q_dev_20261020/join_project_v1.py` has Git blob SHA `0440ff7841573ce5fd17c8dbb64de91b3a59a49b`. Runner SHA `2c135238b2e69924ee0e9f5c8601e336170fe9cc`.
- Frozen design: `tools/queryproof_prospective_r_20261020/DESIGN.md`. Eight SQL families, four distinct synthetic inputs per *same SQL template*. 16 JOIN_TARGET, 16 JOIN_CHALLENGE. Zero proofs valid. No post-first-execution tuning.
- The workflow `.github/workflows/queryproof-prospective-r32.yml` performed pre-execution validation of all protocol keys required by the generic report writer, all blob hashes, Python syntax, 32 unique cases, source ID/ordinal uniqueness, exact stratum/family size. Thus the N32 administrative first-run loss was specifically guarded against.
- **First and only executed R run:** [GitHub Actions 39429538147](https://github.com/mhatipog/spark/actions/runs/39429538147), run HEAD `459958738190c5e1b606578dd3b0e11582ebf9ba`, **SUCCESS**.
- Sanitized first-run [artifact 12266599281](https://github.com/mhatipog/spark/actions/runs/39429538147/artifacts/12266599281): `queryproof-prospective-r32-first-run`, 30-day Actions retention; upload SHA-256 `7cf875c77ef12298a473ed311a282c94ea689a678ac0aeb5652c28986dc673f7`. Preserve externally before expiry if needed.

## Exact R first-run table

| Family | Observed changed pruning rule events | Locally certified | Notes |
|---|---:|---:|---|
| INNER_COUNT | 4 | **4** | Exact two-sided inner equijoin + COUNT |
| LEFT_COUNT | 4 | **4** | Left-outer NULL extension, COUNT(right nullable input) |
| INNER_SUM | 4 | **4** | Exact two-sided inner equijoin + SUM |
| LEFT_SUM | 4 | **4** | Left-outer NULL extension + SUM |
| INNER_LT | 4 | **0** | Deliberate non-equality predicate: UNKNOWN |
| LEFT_OR | 4 | **0** | Deliberate compound predicate: UNKNOWN |
| DISTINCT_PAIR | 4 | **0** | No admitted aggregate/join frame: UNKNOWN |
| HAVING_COUNT | 4 | **0** | Changed/unrecognized frame: UNKNOWN |
| **Total** | **32** | **16** | Designed purposive family distribution |

The 16 exact reasons for deliberate challenge rejections: INNER_LT `JOIN_CONDITION_NOT_DIRECT_EQUAL_TO` (4), LEFT_OR `JOIN_CONDITION_NOT_BINARY_EQUATION` (4), DISTINCT_PAIR `INPUT_NOT_BINARY_JOIN` (4), HAVING_COUNT `AGGREGATE_FRAME_CHANGED` (4). A rejection is **not** evidence Spark's output is wrong; it is faithful limitation of an incomplete proof grammar.

Every target SQL/data run was independent of the M29/M33 development data. All target case events were genuinely emitted by the disposable instrumented official Spark optimizer; the source-pinned strict checker was not rewritten following first execution. The checker compares exact typed exprIds, source leaf project attributes (including metadata), join condition, join type, both input projections, final projection, live aggregate references and unchanged outer frames. It abstains on violations.

## Separate scopes (do not blend them)

| Evidence level | Result | Proper interpretation |
|---|---|---|
| Round P bag/null relational micro-model | 288,800/288,800 finite model matches; unsafe mutants found | Checks algebra, does not certify Spark |
| Round Q **posthoc** observed M join events | 2/2; 20/20 negative AST mutations rejected | Development only |
| Round R **prospective** observed join pruning events | **16/32**; targeted 16/16, challenges 0/16 | Fresh evidence of local proof applicability to actual Catalyst events |
| Round R old v2 complete optimizer suffix | **0/32** | Other optimizer rule events remain unknown |
| Round O old v2 targeted aggregate full suffix | **16/32** (different O cohort, different metric) | Historical independent full-suffix result; unchanged |

The R structural-suffix baseline was zero. The independent state, frame, backing-RDD identity, all-rule accounting and tag census mechanisms all reported complete execution statuses, with **0** cases eligible for each suffix gate. No claim of a positive combined gate set or proof of input continuity for a certified R local event is warranted solely from these aggregate counts.

## Published-work perspective and strongest next test

This law sits alongside established formal bag/NULL SQL semantics and equivalence checking, including the Coq formalization *A Formalization of SQL with Nulls*, SPES's symbolic bag proofs, SQLSolver's linear-integer-arithmetic prover, and SMT-based VeriEQL. Claiming originality of bag projection/join algebra would be incorrect. Our differentiated **hypothesis** is that a source-pinned, real-Catalyst **step-by-step observer + conservative local proof + independent hidden-state witness** can offer useful trustworthy certificates precisely where SQL-text provers or normal optimizer test suites don't attest the actual running JVM transition. Novelty, correctness and benefit remain to be substantiated comparatively.

The next rational action is **posthoc development of a complete optimizer-suffix join chain**, not retuning R. Start with an alternative v3 ledger that imports the Q join certificate but leaves all other v2 checks untouched. Measure remaining unsupported `InferFiltersFromConstraints`, `PushDownPredicates`, and related rule transitions on known M29/M33 development traces. Only widen semantics where a genuinely source-specific local theorem and falsification controls justify it. If/when a sound full chain emerges, freeze a *different* prospective corpus before claiming full-suffix generalization. Preserve old R scoring forever.

**Scientific decision:** pursue joins; independent evidence supports a real narrow result, and the remaining unknown full chain is the important unsolved part.
