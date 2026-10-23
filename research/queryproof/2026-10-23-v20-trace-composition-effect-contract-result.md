# QueryProof — V20: whole-trace certificates under a hidden-state effect contract (23 October 2026)

## The idea

The predecessor tried to *observe and match* hidden optimizer state (tags,
caches), and the strict guard accepted 0/16. V20 reframes the problem. Every
certificate is decided on the **observed** before/after plans of a rule event,
whatever hidden state caused the rule to fire. So hidden state that only
steers *which* rewrites happen (optimizer tag reads, the ineffective-rule cache,
statistics, physical-plan choice) cannot break soundness. Only hidden state that
the evaluator reads when computing results can. That splits the problem into:

1. **Static effect contract** (`tools/queryproof_v20_trace_20261023/`):
   - `TAG_INVENTORY.json`: all 25 `TreeNodeTag`s in the official Spark 4.2.0
     source jars from Maven Central (SHA-1 verified), with read and write sites.
   - `EFFECT_CONTRACT.json`: each tag classified as optimizer control,
     analysis-only, naming or physical/display, plus the non-tag hidden state:
     ineffective-rule bits and statistics (control), derived lazy values, and
     SQLConf, which is semantic and must be ANSI.
   - No tag is read in a result-affecting way.
2. **Dynamic check:** the disposable observer `instrument_v20.py` records the
   tag *names* on every plan of the optimizer run, plus ANSI.
3. **Composition in Coq:** `research/queryproof/proofs/QueryProofTrace.v`,
   with `trace_sound` and `check4_sound` closed under the global context.
   - `check4` = V19 `check3` plus ColumnPruning, CollapseProject and
     RemoveNoopOperators. These are proved as equality on the needed columns
     through Project, Filter, INNER/LEFT OUTER Join and opaque operators; under
     an opaque operator the input must be unchanged, which is conservative.
   - `trace_ok` requires that every event passes `check4`, that **connectivity**
     holds (event i's output is syntactically event i+1's input, so omitted
     plan changes are exposed), and that every observed tag is contract-OK.
   - The contract enters as an explicit hypothesis: a plan's meaning does not
     depend on hidden state whose observed tags are all contract-OK.
     `trace_sound` then equates the meaning of the first optimizer plan with
     that of the final one.

## Freeze, failure and rerun (all on record)

- Seed 20261024, 150 queries (143 distinct; 12 SQL strings shared with
  V17–V19 over different data). Frozen at `d8ef8039b0` with 25 components.
- Run 39975244992 executed the full cohort, then **crashed** in `run_trace.py`
  report aggregation (`KeyError 'links_ok'`). Empty traces were skipped when
  building Coq inputs but not when building the report, and the development set
  had no such query. Trace-level verdicts were never computed.
- A 4-line fix (an empty trace is trivially unchanged and counted separately)
  went in at `948b71b1e9`. The checker, proofs, cohort, definitions and
  thresholds are unchanged. Re-frozen at `4a24fe232e`, rerun **39979489609** at
  `cbf227a17d`.
- This is an infrastructure rerun *after* execution. The per-event numbers are
  identical in both runs.

## Frozen verdicts (rerun)

| Hypothesis | Result | Verdict |
| --- | --- | --- |
| T0a: certified traces' endpoints agree in SQLite | 80 / 80 | passed |
| T0b: ≥ 80% of certified traces evaluable | 1.00 | passed |
| **T1 (primary)**: no trace mutant accepted | **0 / 300** | **passed** |
| T1b: ≥ 100 trace mutants | 300 | passed |
| E1: no invalid per-event mutant accepted (`check4`) | 0 / 902 (563 validated wrong + 339 ill-scoped) | passed |
| E1b: ≥ 150 validated-wrong event mutants | 563 | passed |
| T2: connected observation | 0 JSON breaks, 0 kernel link failures, 0 disconnected cuts | passed |
| T3: contract coverage | only `dataset_id` observed; ANSI on in 150/150; no observer errors | passed |
| **T4: whole-trace coverage ≥ 0.40** | **80 / 147 = 0.544** | **passed** |
| H5: Spark = SQLite | 150 / 150 | passed |

Trace mutants rejected by kind:

| Mutant | Rejected |
| --- | --- |
| drop_middle_event | 69 |
| swap_adjacent_events | 71 |
| unknown_tag | 80 |
| splice_unimplied_conjunct | 33 |
| splice_drop_pushed_filter | 27 |
| splice_wrong_guard_attribute | 20 |

3 queries had no effective optimizer event and are counted apart.

### Why the other 67 traces are not certified

First failing rule per trace:

| First failing rule | Traces |
| --- | --- |
| InferFiltersFromConstraints (transitive inference) | 17 |
| ReplaceDistinctWithAggregate | 15 |
| EliminateOuterJoin | 8 |
| PushDownPredicates | 5 |
| ReorderJoin | 3 |
| ColumnPruning (under an Aggregate) | 2 |

17 more traces are unprintable: a non-integral literal (10) or `Cast` (7).

Per-event coverage under `check4`:

| Rule | Accepted |
| --- | --- |
| ColumnPruning | 131 / 134 |
| RemoveNoopOperators | 27 / 27 |
| CollapseProject | 8 / 8 |
| PushDownPredicates | 252 / 260 |
| InferFiltersFromConstraints | 113 / 144 |

ConstantPropagation, ConvertToLocalRelation, PropagateEmptyRelation,
EliminateOuterJoin, ReorderJoin and ReplaceDistinctWithAggregate are not certified.

## What this establishes

**For 54% of unseen queries, the entire logical optimization of a real Spark
4.2.0 query** is certified: from the first optimizer plan after analysis to the
plan handed to physical planning. The certificate is a kernel-checked chain of
rule certificates whose composition is proved in Coq without axioms. It is
conditional on one explicit, inventoried and dynamically checked effect contract.
The checker rejected every one of 300 corrupted traces and 902 invalid single
rewrites, and every certified trace's endpoints agree under an independent
SQLite evaluation.

## What it does not establish

- The effect contract is **trusted**, not proved. It rests on a name-based
  static read-site inventory (reflective or aliased reads would be missed) and
  on result-equivalence of physical planning and AQE.
- The printer, evaluator agreement on the expression grammar, ANSI `Add`
  semantics, opaque-descriptor identity and JSON fidelity (V14 stored identity
  only for W17–W20) remain trusted.
- Physical planning, AQE re-optimization and execution are outside the
  certified suffix.
- The observed tag set was just `dataset_id` on these query shapes. Tags such as
  `ROOT_REPARTITION` (V18B) would need queries that trigger them.
