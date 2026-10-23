# QueryProof — V22: leaf data identity and opaque-read pruning (23 October 2026)

## Why

- **Soundness hole.** The rejected-event SQLite scan over the V19–V21 cohorts
  found no Spark bug candidates. It did surface one ConvertToLocalRelation event
  (V20 cohort, T142) that is correct in Spark but whose old and new leaves print
  identically. TreeNode JSON serializes `LogicalRDD` with `"rdd": null` and
  omits `LocalRelation.data`, so **leaf data identity was invisible to the
  printer**. A rule that changed only a leaf's rows would have been accepted by
  syntactic equality. No accepted event in any frozen cohort was affected: every
  ConvertToLocalRelation/PropagateEmptyRelation event was rejected or
  unprintable, and all accepted events agree in SQLite.
- **Coverage.** ColumnPruning under opaque operators (Aggregate) blocked
  whole traces.

## What changed (before the freeze)

- `instrument_v22.py` fingerprints every leaf on every effective event
  (`LogicalRDD` id, `LocalRelation` row hash). `attach_leaf_ids.py` injects the
  fingerprints into the trace JSON, and `print_terms_v22.py` makes them part of
  leaf identity and **refuses unobserved leaves**. A new trace mutant,
  `leaf_data_swap`, keeps the plan JSON identical but changes a leaf's data.
- `QueryProofDeepWide3.v` / `QueryProofTrace3.v` (copies) give opaque operators
  read-restricted semantics: `eval (POp1 k out reads q) = normalize out (ops k out (normalize reads (eval q)))`.
  Pruning under an opaque operator is therefore equality on its read columns.
  The printer extracts `reads` syntactically, which is trusted. All theorems are
  closed under the global context.

## Frozen run

Seed 20261026, 150 queries, frozen at `873c43be8c` (30 components), run
**40009895947** at `dd79a0ec52`.

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| T0a: certified endpoints agree in SQLite | 110 / 110 | passed |
| T0b: ≥ 80% of certified traces evaluable | 1.00 | passed |
| T1 (primary): no trace mutant accepted | **0 / 529** (incl. 110 `leaf_data_swap`) | passed |
| T1b: ≥ 100 trace mutants | 529 | passed |
| E1: no invalid event mutant accepted | 0 / 934 | passed |
| E1b: ≥ 150 validated-wrong event mutants | 591 | passed |
| T2: connected observation | 0 breaks | passed |
| T3: contract coverage | only `dataset_id`; ANSI on | passed |
| **T4: whole-trace coverage ≥ 0.78** | **110 / 144 = 0.764** | **FAILED** |
| H5: Spark = SQLite | 150 / 150 | passed |
| T5: every leaf identity observed | 1224 / 1224 events | passed |

Per-rule outcomes:

- ColumnPruning: 132 / 132 accepted (V21: 123 / 130).
- First failing rule among uncertified traces: InferFiltersFromConstraints 14,
  ReorderJoin 9, PushDownPredicates 1, BooleanSimplification 1.
- Unprintable traces: 9 (`Cast` 5, non-integral literal 4).

## Interpretation

- The leaf-identity hole is closed, and the closure is exercised by 110 rejected
  leaf-data swaps.
- Pruning under opaque operators is no longer a blocker.
- The coverage prediction failed. This cohort contained more of the remaining
  IFFC shape (a filter added both above and inside a pruning Project together
  with equality transport) and ReorderJoin, so the gain was 0.755 → 0.764,
  not the predicted ≥ 0.78. The verdict stands as scored.
