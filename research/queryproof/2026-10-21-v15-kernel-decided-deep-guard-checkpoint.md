# QueryProof — V15: kernel-decided deep guard certificates on real Catalyst events (21 October 2026)

Successor round 2. Retrospective on historical W32 development traces.
Frozen W32 16/32 and X 0/16 are unchanged and are not comparable with the counts below.

## Question

V13's per-event Coq instances certified constants chosen by Python (see the
round-1 checkpoint). Can the *kernel itself* decide, from the actual
before/after Catalyst plan terms, that a real `InferFiltersFromConstraints`
event preserves query results, so that Python only translates?

## What was built

- `research/queryproof/proofs/QueryProofDeepGuard.v`. A deep embedding of a
  Catalyst fragment: resolved attribute ids, 3VL expressions
  (`<, <=, >, >=, =, <=>, IsNotNull, And, Or`, integral literals),
  Filter, attribute-only Project, Inner/LeftOuter Join, and opaque leaves and
  unary operators (Sort, Aggregate, aliased Project, …) given by descriptor
  index and output ids. The executable `check old new` accepts only when NEW is
  OLD with `IsNotNull` guard conjunctions inserted above join inputs, where
  every guarded attribute is in that input's output and is null-rejected by the
  *unchanged* join condition. There is no guard on the LEFT OUTER preserved
  side, the right-side guards are not left attributes, and everything else is
  syntactically equal.
- `check_sound : check o n = true -> forall db ops, eval db ops o = eval db ops n`,
  universally quantified over every interpretation of leaves and opaque
  operators. `Print Assumptions check_sound` reports **Closed under the global
  context** (no axioms, no `Admitted`). The file holds 9 kernel-evaluated controls.
- `tools/queryproof_v15_deep_guard_20261021/`:
  - `print_terms.py`: TreeNode JSON → terms. It is the only trusted
    translation and never decides acceptance. Anything outside the grammar is
    UNKNOWN.
  - `capture_w32.py`: raw trace capture with the disposable AST overlay.
  - `run_v15.py`: Coq decides every effective event of *every* rule
    (pass 1, `vm_compute`). Every verdict is then re-asserted as a
    kernel-checked `Example` (pass 2), and each acceptance gets a
    `check_sound` instance. Six JSON-level adversarial mutant families
    per accepted event: wrong guard attribute, null-safe join, LEFT OUTER
    preserved-side guard, swapped guard sides, OR join condition, frame edit.
- `.github/workflows/queryproof-v15-deep-guard.yml`.

## Results

CI run 39602825893 (commit `54a9a16e57`, Coq 8.18.0, official pyspark 4.2.0).
Run 39601642073 failed on a relative-path bug after the theorem had already
compiled. It is kept on record.

| Measure | Result |
| --- | --- |
| Effective rule events in W32 traces (all rules) | 308 |
| Printed into the grammar | 104 (204 UNKNOWN: 132 `View`, 64 `UnresolvedAttribute`, 8 `Cast`) |
| `InferFiltersFromConstraints` events | 28, in W01–W20 and W25–W32 (none in W21–W24: OR joins infer no guard) |
| Kernel-accepted | **28/28 IFFC events**; 0 events of any other rule |
| Printed events of other rules | 76, **all kernel-rejected** (ColumnPruning 28, PushDownPredicates 28, RemoveRepetitionFromGroupExpressions 16, ReplaceDistinctWithAggregate 4) |
| Adversarial mutants | **152/152 kernel-rejected**, 0 UNKNOWN |
| Kernel-checked statements generated | 256 (104 event verdicts + 152 mutant verdicts) |

The accepted events cover INNER equi-joins (guards on both sides), LEFT OUTER
equi-joins (guard only on the NULL-supplying right side), strict `<` joins,
and the guard step inside DISTINCT and HAVING plans.
An earlier local run (Windows, same code) gave 14/14 and 76/76 on the cases
captured then.

## Proof status (evidence hierarchy)

1. **Kernel-proved:** `check_sound`, with no axioms, for every database and
   every opaque-operator interpretation.
2. **Kernel-decided per event:** acceptance of the 28 real events and rejection of
   76 other-rule events and 152 mutants. Python decides nothing.
3. **Trusted, not proved** (the remaining trusted base):
   - (a) the ~200-line printer;
   - (b) Spark's evaluator agrees with `eval_e` on integral comparisons, `IsNotNull`,
     `And` and `Or`, with no ANSI errors in this grammar;
   - (c) equal opaque descriptors and leaf descriptors denote equal behaviour;
   - (d) TreeNode JSON is faithful for the translated fields. V14 attests
     stored-identity preservation only for W17–W20.
4. **Not covered:** tags, caches, SQLConf and optimizer control flow (X 0/16
   stands), event omission, and composition over the optimizer suffix.
   Each certificate is local to one rule event.
5. **Prospective:** none. W32 is development data.

## Interpretation

This is the first point in QueryProof where the per-event formal check is
*informative*. The kernel discriminates real guard insertions from 76 different
real rewrites and from 152 adversarial variants, and its acceptance carries
a theorem with no hidden assumptions inside Coq. In form, it is translation
validation of one real Catalyst rule (in the style of Alive or CompCert
validators), with an explicit trusted printer.
It is not a Spark correctness proof. "28/28" is a count of local
rule events and must not be added to, or compared with, the W32 suffix scores.

## Next highest-information steps

1. Freeze a new held-out cohort (seeded NULL/duplicate data, new join shapes:
   `<=`/`>`, multi-key AND conditions, LEFT JOIN with a pre-existing filter,
   self-joins, casts) with acceptance rules and commit SHA, *before* running.
2. Extend `check` to IFFC's filter-merge form (`Filter(c)` → `Filter(c AND g)`)
   and to a second rule (`PushDownPredicates` through Project/Join) with its
   own soundness theorem.
3. Compose per-event certificates across the trace with an explicit
   hidden-state contract, using the round-1 stored-identity result for binding.
