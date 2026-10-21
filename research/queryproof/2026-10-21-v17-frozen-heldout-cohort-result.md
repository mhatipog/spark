# QueryProof — V17 frozen held-out cohort: result (21 October 2026)

First prospective evaluation of the kernel-decided checker (`check2`, V16) on
data and queries it had never seen.

## Protocol and freeze

- Components: `tools/queryproof_v17_heldout_20261021/`. They comprise a generator
  (seed 20261021, 150 queries, fresh NULL- and duplicate-rich tables `ta/tb/tc`),
  Spark capture with result rows, an independent SQLite oracle, a scorer, and a
  freeze/verify tool.
- Freeze commit `cea457f418`. It was re-frozen at `0a8b6c8ae4` after a trigger-only
  workflow change (dispatch needs the default branch). **No cohort query had
  been executed by Spark, Coq or SQLite before either freeze.** The pipeline was
  dry-run only on the V16 development probes.
- Run: CI 39639245281 at `66bc2ee920` (run-request marker only). The log shows
  `QUERYPROOF_V17_FREEZE_VERIFIED 14` and byte-identical cohort regeneration
  before execution.
- Definitions (frozen): the unit is one effective rule event. ACCEPTED means a
  kernel-checked `check2 old new = true`. UNKNOWN means not printable, and counts
  as not accepted.

## Frozen verdicts (as scored, no re-interpretation)

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| **H1 (primary)**: zero mutants accepted | **25 / 584 accepted** | **FAILED** |
| H2: IFFC coverage ≥ 0.60 | 88 / 136 = **0.647** (11 rejected, 37 UNKNOWN) | passed |
| H3: PDP coverage ≥ 0.50 | 116 / 203 = **0.571** (24 rejected, 63 UNKNOWN) | passed |
| H4: no other rule accepted | BooleanSimplification 2, ReorderJoin 1 | **FAILED** |
| H5: Spark = SQLite result bags | 150 / 150 match, 0 errors | passed |

Mutants rejected by family: unimplied_conjunct 110, drop_pushed_filter 86,
or_join_condition 73, wrong_guard_attribute 61, wrong_filter_attribute 58,
left_outer_preserved_guard 56, null_safe_join 41, frame_edit 37,
swapped_guard_sides 37. 170 mutants were UNKNOWN at the printer.

## Post-hoc analysis of the failures (after the run; not part of the frozen scoring)

All 25 accepted mutants were inspected in the generated kernel file. **Each is a
semantically valid rewrite; none is a checker or printer soundness failure.**
The frozen claim that every mutant family is "semantically wrong by construction"
was false. That is a test-design error:

1. `left_outer_preserved_guard` (17): the generator converts the *first* join
   in preorder to LEFT OUTER. In three-way joins that is the upper join, while
   the inserted guards sit below an unchanged inner join, so they stay valid.
2. `left_outer_preserved_guard` and `or_join_condition` on Filter-merge events
   (7): the inserted `IsNotNull(x)` is implied by the input's *own* filter
   (e.g. `x > 0`), not by the join, so changing the join kind or OR-ing the ON
   condition leaves it valid.
3. `wrong_guard_attribute` (1, H148): the replacement attribute `ta.c` is also
   null-rejected by an ON conjunct (`ta.c > tb.c`), so the "wrong" guard is
   legitimate.

H4: the three accepted events of other rules (duplicate-conjunct removal and
conjunct movement between joins) fall inside `check2`'s conjunct-set
equivalence and are covered by `check2_sound`. The frozen prediction that no
other rule would be accepted was simply wrong.

Coverage gaps among the 35 REJECTED target-rule events (sampled):

- IFFC inferring non-IsNotNull constraints (`a = b ∧ b ≥ 0 ⇒ a ≥ 0`,
  constant propagation).
- PushDownPredicates moving ON-clause conjuncts into a child or a lower join
  with no Filter above.
- A push through a join fused with combining into an existing child filter.

The 100 UNKNOWN target events come from `In`, `IsNull`, `Add` and `Cast`
(see `unsupported_by_rule_reason` in the kernel report).

## Interpretation

- The safety evidence the frozen run was designed to provide is **weaker than
  planned**. H1 failed as scored. The post-hoc explanation (valid mutants) is
  supported by inspection, not by an independent frozen test.
- No accepted event or mutant contradicts `check2_sound`, and Spark and SQLite agree
  on all 150 queries.
- Coverage generalized to unseen queries at roughly the level predicted
  (0.65 / 0.57), with UNKNOWN as the largest loss.

## Lessons for the next frozen run

1. Mutants must be *semantically validated* as wrong before they count, e.g. by
   differential evaluation of OLD vs MUTANT on randomized databases with an
   independent evaluator. A mutant that is equivalent on all trials is
   discarded, not counted.
2. Target join-kind mutants at the join whose inputs actually received the
   guard, and only when the guard is justified by that join.
3. Predict "other rules accepted" as a count to report, not as a hypothesis,
   because conjunct-set equivalence legitimately covers parts of other rules.
