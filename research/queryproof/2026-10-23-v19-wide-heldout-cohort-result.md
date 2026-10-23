# QueryProof — V19 wider grammar and unified join law: frozen held-out result (23 October 2026)

## What changed (all before the freeze)

`research/queryproof/proofs/QueryProofDeepWide.v` is self-contained (V15/V16
stay frozen), and `check3_sound` is **closed under the global context**.

- **Grammar.** Adds `IsNull`, `IN` over literal lists, and width-bounded integer
  `Add`, where overflow is an evaluation error (Spark's default ANSI mode).
  New TreeNode JSON finding: `Add` serializes `"evalContext": null`, so its
  eval mode is erased. ANSI is therefore a trusted assumption, and as for every
  error, the model treats it as "not TRUE" where Spark would fail the query.
- **One unified INNER-join law** replaces the V15 guard and V16 pushdown rules.
  The conjuncts of (WHERE above, ON, existing input filters) may be
  redistributed into (new input filters, new ON), under three conditions:
  - every old conjunct is kept;
  - each added conjunct is an `IsNotNull(x)` with x null-rejected by an old
    conjunct (null rejection now flows through `Add` and `IN`);
  - each input-side conjunct reads only that input's columns, and right-side
    conjuncts are disjoint from the left output.

  The same law works through an unchanged attribute-only Project. There, only
  the conjuncts *added* to the inner filter (computed in Coq as `rest_of`) must
  be projected. This covers IFFC guards, WHERE pushdown, **ON-clause pushdown**
  and **fused push-and-combine**.
- **LEFT OUTER.** The same law applies on the null-supplying (right) side of ON.
  Pushdown into the preserved side is rejected, and V16's WHERE-to-left-input
  push is kept.
- 14 kernel-evaluated controls.

The printer, differential, validator and scorer are V19 copies
(`tools/queryproof_v19_wide_20261022/`). The driver gains `--printer`.

## Freeze and run

- Seed 20261023, 150 queries: 146 distinct, 94 with one-side ON conjuncts, and
  6 SQL strings shared with V17/V18 over different data.
- First freeze `1ca33bf25b`. Run 39722510440 was rejected by GitHub as an
  invalid workflow (an unquoted colon in `name:`) and **started no job**. The
  name was quoted and the freeze re-recorded at `e9440aba80` before any execution.
- Run: CI 39959977113 at `c5040788ee`. The log shows
  `QUERYPROOF_V19_FREEZE_VERIFIED 17` before execution.

## Frozen verdicts

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| H0a: no accepted event differs in SQLite | 394 / 394 agree | passed |
| H0b: ≥ 80% of accepted events evaluable | 1.00 | passed |
| **H1 (primary)**: no invalid mutant accepted | **0 / 587** (385 VALIDATED_WRONG + 202 ILL_SCOPED) | **passed** |
| H1b: ≥ 150 VALIDATED_WRONG | 385 | passed |
| H2: IFFC coverage ≥ 0.75 | 113 / 144 = **0.785** (31 rejected, **0 UNKNOWN**) | passed |
| H3: PDP coverage ≥ 0.65 | 280 / 295 = **0.949** (10 rejected, 5 UNKNOWN: `Cast`) | passed |
| H5: Spark = SQLite query results | 150 / 150 | passed |

Reported without prediction:

- 1 `BooleanSimplification` event accepted. It falls inside conjunct-set
  equivalence and is covered by `check3_sound`.
- 42 accepted mutants are NOT_SHOWN_WRONG, i.e. equivalent rewrites that were
  legitimately accepted.

Coverage across the three prospective runs (different seeds, same templates;
V19 adds ON templates):

| | V17 (`check2`) | V18 (`check2`) | V19 (`check3`) |
| --- | --- | --- | --- |
| IFFC | 0.647 | 0.656 | **0.785** |
| PushDownPredicates | 0.571 | 0.528 | **0.949** |
| UNKNOWN target events | 100 | 108 | **5** |

## Remaining gaps (post-hoc inspection of the 31 rejected IFFC events)

- **Transitive constraint propagation through equi-join keys** (largest group):
  e.g. `ta.b >= 1 ∧ ta.b = tb.a ⇒ tb.a >= 1`, and comparisons copied across
  `ta.c = tb.c`. These are valid, but the inferred atoms are not `IsNotNull`.
- Contradictions inferred by transitivity (`x < x`; 5 events).
- Guards in OR or null-safe ON contexts (8 events).
- PushDownPredicates: 10 rejected and 5 `Cast` UNKNOWN.

## Proof status

1. Kernel-proved: `check3_sound` (axiom-free).
2. Kernel-decided, prospectively: this frozen cohort.
3. Independently cross-checked: SQLite differential (H0, 394 events) and
   query oracle (H5).
4. Trusted: the printer; ANSI-mode `Add` (erased from JSON); errors-as-not-TRUE;
   opaque-descriptor identity; JSON fidelity.
5. Open: hidden optimizer state, composition across the optimizer run, and
   transitive constraint inference.
