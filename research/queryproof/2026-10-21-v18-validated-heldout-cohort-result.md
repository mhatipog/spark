# QueryProof — V18 frozen held-out cohort with validated mutants: result (21 October 2026)

Second prospective evaluation of the kernel-decided checker `check2`. It is
unchanged since V16: the Coq blob SHAs are identical to those frozen in V17.

## What changed from V17 (all frozen before execution)

- **Independent mutant validation.**
  `tools/queryproof_v18_validated_20261021/differential.py` translates the
  *exact kernel terms* to SQL and runs both sides in SQLite on 40 random NULL-
  and duplicate-rich databases. Opaque operator `k` is a projection plus a
  k-dependent, NULL-preserving row filter, one admissible interpretation
  under `check2_sound`. Mutants are classified VALIDATED_WRONG, ILL_SCOPED,
  NOT_SHOWN_WRONG or OUTSIDE. Only the first two count for H1.
- **Retargeted mutants.** Join-kind and OR mutants act on the join whose input
  received the new guard, and LEFT OUTER applies only when that guard is on the
  preserved side. The OR mutant is now `c OR (a = a)`: V17's `c OR c` is
  identical to `c` in 3VL and could never be wrong.
- **New cross-check H0.** Every ACCEPTED real event must agree in SQLite on every
  random database.
- **Other-rule acceptances are reported**, not predicted.

Post hoc on V17's kernel file: all 207 accepted V17 events agree, and **all 25
accepted V17 mutants are NOT_SHOWN_WRONG**. This confirms the V17 H1 failure was
a test-design error. The V17 frozen verdict stays as recorded.

## Freeze and run

- Seed 20261022, 150 queries, fresh tables (not equal to V17's). 134 distinct SQL strings;
  8 strings coincide with V17 over different data.
- Freeze `b6c89207cd` (17 components). The pipeline was dry-run only on W32 and
  the V16 development probes, and no V18 query was executed before the freeze.
- Run: CI 39650986099 at `10a3a3fd91` (run-request marker only). The log shows
  `QUERYPROOF_V18_FREEZE_VERIFIED 17` before execution.

## Frozen verdicts

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| H0a: no accepted event differs in SQLite | 188 / 188 agree | passed |
| H0b: ≥ 80% of accepted events evaluable | 188 / 188 = 1.00 | passed |
| **H1 (primary)**: no invalid mutant accepted | **0 / 376** (254 VALIDATED_WRONG + 122 ILL_SCOPED) | **passed** |
| H1b: ≥ 150 VALIDATED_WRONG mutants (power) | 254 | passed |
| H2: IFFC coverage ≥ 0.60 | 86 / 131 = **0.656** (8 rejected, 37 UNKNOWN) | passed |
| H3: PDP coverage ≥ 0.50 | 102 / 193 = **0.528** (20 rejected, 71 UNKNOWN) | passed |
| H5: Spark = SQLite query results | 150 / 150 | passed |

Reported without prediction: 0 events of other rules accepted; 0 mutants of
any class accepted (none of the 135 NOT_SHOWN_WRONG were accepted either).

VALIDATED_WRONG by family: unimplied_conjunct 50, or_join_condition 49,
left_outer_preserved_guard 41, null_safe_join 28, drop_pushed_filter 28,
wrong_filter_attribute 28, frame_edit 19, wrong_guard_attribute 11.
ILL_SCOPED: wrong_guard_attribute 40, swapped_guard_sides 33,
unimplied_conjunct 25, wrong_filter_attribute 24.

## What this establishes, and what it does not

- **Established (prospective, frozen):** on 150 unseen queries over fresh NULL-rich
  data, the kernel-decided checker
  - certified about two-thirds of real InferFiltersFromConstraints events and
    about half of PushDownPredicates events;
  - accepted none of 376 independently confirmed-invalid rewrites;
  - had every accepted event confirmed equivalent by an evaluator that shares no
    code with Coq or with the printer.
- **Strength of H0:** it tests the whole chain (Spark trace → printer → kernel
  terms → SQL semantics) against SQL semantics in a second engine. It is still
  only one admissible interpretation for opaque operators and bounded random
  testing: it can refute, not prove.
- **Not established:**
  - hidden optimizer state (X 0/16 stands), event omission, and
    composition across the optimizer run;
  - agreement of Spark's evaluator with the model beyond integer comparisons,
    IsNotNull, And and Or;
  - the 108 UNKNOWN target events (`In`, `IsNull`, `Add`, `Cast`) and 28
    rejected ones (non-IsNotNull inference, ON-clause pushdown, fused
    push-and-combine).

## Evidence hierarchy after V18

1. Kernel-proved: `check_sound`, `check2_sound` (axiom-free).
2. Kernel-decided per event, now **prospectively**: V18 frozen cohort.
3. Independent semantic cross-check: SQLite differential (H0) and query
   oracle (H5).
4. Trusted: printer, opaque-descriptor identity, JSON fidelity
   (V14 stored identity only for W17–W20).
5. Open: hidden state, composition, broader expression grammar.
