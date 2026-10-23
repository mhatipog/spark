# QueryProof — V21: certifying the top V20 trace blockers (23 October 2026)

## What changed (all before the freeze)

`QueryProofDeepWide2.v` and `QueryProofTrace2.v` are copies, so V19/V20 stay
frozen. `check3_sound`, `check4_sound` and `trace_sound` are all closed under
the global context.

- **Transitive implication.** A new conjunct may be an old conjunct with `y`
  renamed to `x` (or the reverse), where an old strict `x = y` holds. A TRUE
  strict equality forces equal non-NULL values (`rename_eval`,
  `eq_pairs_sound`). This covers IFFC constraint propagation through equi-join
  keys and the inferred contradictions such as `x < x`. Transport along `<=>`
  or `<` is rejected (kernel controls).
- **EliminateOuterJoin.** `Filter(c, LeftOuter(l, r))` → `Filter(c, Inner(l, r))`
  when `c` null-rejects an attribute of `r` that `l` does not output: padded
  rows are NULL there, so the filter discards exactly them (`eoj_shift`).
  Rejected when keyed on a preserved-side column or under `IS NULL`.
- **ReplaceDistinctWithAggregate** (`print_terms_v21.py`). `Distinct(q)` and a
  grouping-only `Aggregate(A, A, q)` (attribute references, `A ⊆ q.output`) both
  print as one reserved dedup operator over `Project(A, q)`. This **operator
  identification is trusted**. It is sanity-checked end to end by the SQLite
  query oracle on real DISTINCT queries.
- **Side form.** The project-transparent side form also offers the old inner
  filter as an old conjunct when its columns are projected, so conjuncts can be
  transported from it. Found on development probes.

Development traces only (21 queries): 8 → 15 whole traces certified.

## Freeze and run

Seed 20261025, 150 queries (143 distinct; 12 SQL strings shared with V17–V20
over different data). Frozen at `f945ee19e2` with 24 components. Run
**39989148686** at `ea575f3797`, with `QUERYPROOF_V21_FREEZE_VERIFIED 24` before
execution. No infrastructure failures.

## Frozen verdicts

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| T0a: certified traces' endpoints agree in SQLite | 111 / 111 | passed |
| T0b: ≥ 80% evaluable | 1.00 | passed |
| **T1 (primary)**: no trace mutant accepted | **0 / 398** | **passed** |
| T1b: ≥ 100 trace mutants | 398 | passed |
| E1: no invalid event mutant accepted | 0 / 880 (557 validated wrong + 323 ill-scoped) | passed |
| E1b: ≥ 150 validated wrong | 557 | passed |
| T2: connected observation | 0 breaks, 0 link failures, 0 disconnected cuts | passed |
| T3: contract coverage | only `dataset_id`; ANSI 150/150 | passed |
| **T4: whole-trace coverage ≥ 0.65** | **111 / 147 = 0.755** (V20: 0.544) | **passed** |
| H5: Spark = SQLite | 150 / 150 | passed |

Per-rule event coverage under `check4`:

| Rule | Accepted |
| --- | --- |
| InferFiltersFromConstraints | 125 / 132 (V20: 113 / 144) |
| ReplaceDistinctWithAggregate | 10 / 10 (V20: 0 / 15) |
| EliminateOuterJoin | 9 / 10 (V20: 0 / 12) |
| PushDownPredicates | 239 / 246 |
| ColumnPruning | 123 / 130 |
| RemoveNoopOperators | 25 / 25 |
| CollapseProject | 6 / 6 |

### Remaining blockers (first failing rule)

| Rule | Traces |
| --- | --- |
| ReorderJoin | 9 |
| ColumnPruning (under opaque operators) | 7 |
| PushDownPredicates | 4 |
| InferFiltersFromConstraints | 4 |
| BooleanSimplification | 1 |
| EliminateOuterJoin | 1 |

A further 10 traces are unprintable: `Cast` (7) and a non-integral literal (3).

## Trust base (unchanged except one addition)

Trusted:

- the printer, now including the Distinct/dedup-Aggregate identification;
- evaluator agreement on the expression grammar, ANSI `Add`, and errors as
  "not TRUE";
- opaque-descriptor identity and JSON fidelity;
- the V20 effect contract (name-based tag read-site inventory; physical
  planning and AQE are result-equivalent).

Outside the certified suffix: physical planning, AQE and execution.
