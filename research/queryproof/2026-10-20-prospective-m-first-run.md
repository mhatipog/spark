# QueryProof prospective M48: first-run evidence and interpretation

**Date:** 2026-10-20  
**Owner:** Metin Hatipogullari  
**Repository / research branch:** `mhatipog/spark`, `research/queryproof-adversarial-tag-frame-20261020`  
**Scientific status:** Prospective synthetic cohort, *conditional local optimizer-suffix certificates*, not broad Spark correctness proof.

## Freeze provenance

- Before any M SQL execution, committed `tools/queryproof_prospective_m_20261020/CASES.json`, `PROTOCOL.json`, `DESIGN.md`, and an orchestration wrapper `run_m.py`.
- Fixed verifier/checker baseline: `40bf2fbab64c717c72ec630566bb5c8cf705c2a4`. The wrapper composes pre-existing local checkers and independent scope/state/frame/RDD-input/all-rule/tag-shape observers without altering their proof decisions.
- The first attempted workflow registration failed during GitHub YAML parsing because of a colon in a step label; **no Spark SQL ran**. The label was corrected before the first actual M execution. This failure is administrative, not scientific evidence.
- **First executed cohort:** https://github.com/mhatipog/spark/actions/runs/39417193656 ; run SHA `3ecf680c838bc967b39d0b1516182fe945841437`.
- **Posthoc stratification of that exact preserved artifact, with no rerun:** https://github.com/mhatipog/spark/actions/runs/39417529333 .
- The GitHub Actions artifact `queryproof-prospective-m48-frozen-first-run` retains the sanitized JSON for 30 days; its SHA-256 artifact digest is `02bf65f8942e8a97053d0787bf9482d622d4491aa3d03f0e611352732c33adf6`.

## Preregistered design

48 synthetic SQL cases, 12 families × 4 independently seeded table instances. The first 24 are deliberately enriched for previously supported repeated grouping keys. The other 24 challenge the fragment using a no-repeat aggregate control, equijoins, left joins, UNION ALL, DISTINCT, and HAVING. Each case uses separate integer source tables with duplicates and NULLs. SQL/dataset family dependence makes the overall percentage **not a population estimate** for actual Spark workloads.

The protocol explicitly allows zero certificates as a valid scientific outcome and forbids tuning the corpus or checker after first-run observation while still describing revised results as prospective.

## Actual first-run measurements

| Measurement | Result |
|---|---:|
| Cases executed | **48/48** |
| Missing/error cases | **0** |
| Finite Spark 4.2.0 vs SQLite rowbag mismatches | **0** |
| Captured changed Catalyst rule events | **412** |
| Captured optimizer suffixes after Finish Analysis | **48** |
| Changed suffixes | **48** |
| Complete structurally certified changed suffixes | **20** |
| Candidates passing frame guard | **20** |
| Candidates passing live RDD identity witness | **20** |
| Candidates without observed all-rule silent drift | **20** |
| Candidates passing source-pinned tag shape gate | **20** |
| Combined intersection | **20**, because *each candidate gate set equals the same structural baseline set* |
| Total actual Catalyst rule calls | **26,244** |
| `fastEquals` no-op calls | **25,832** |
| Observed silent hidden drift / silent input swap | **0 / 0** |
| Post-FinishAnalysis tag occurrences (repeated snapshots) | **9,052** |
| Observed suffix tag type | Only `dataset_id:scala.collection.mutable.HashSet`, all `Long` elements |

**Caveat:** The frozen legacy five-checker event union returned UNKNOWN on all 412 events; the 20 conditional suffix certificates arise from the later, separately frozen and source-specific grouping/liveness/frame rules used by the *hierarchical scope ledger*. These two outputs are different algorithms with different supported grammars. This difference is not to be concealed or called a contradiction.

## Stratified results (posthoc descriptive readout)

| Stratum | Supported conditional changed suffixes | Interpretation |
|---|---:|---|
| Target-enriched, 24 cases | **16/24** | Some generalization within familiar grouping grammar |
| Generalization challenge, 24 cases | **4/24** | Narrow benefit; all four arise in the no-repeat aggregate control |

| Family | Cases | Complete candidates |
|---|---:|---:|
| REPEAT_COUNT | 4 | 4 |
| MOD_REPEAT_SUM | 4 | 4 |
| REORDERED_KEYS | 4 | 4 |
| FILTERED_REPEAT | 4 | 4 |
| MOD_REPEAT_MAX | 4 | 0 |
| NESTED_PROJECT_REPEAT | 4 | 0 |
| NO_REPEAT_CONTROL | 4 | 4 |
| INNER_JOIN_REPEAT | 4 | 0 |
| LEFT_JOIN_REPEAT | 4 | 0 |
| UNION_ALL_REPEAT | 4 | 0 |
| DISTINCT_CONTROL | 4 | 0 |
| HAVING_REPEAT | 4 | 0 |

No confidence intervals are reported: observations are clustered by query family and deliberately selected; treating the 48 cases as an i.i.d. production sample would be scientifically misleading.

## What the evidence actually establishes

1. The installed, source-pinned instrumentation ran on a *new* SQL corpus with zero detected trace-integrity faults, Spark/SQLite rowbag discrepancies, or observed all-rule silent-state drift.
2. In the narrow supported relational fragment and under the predeclared abstract semantics, 20 genuine changed Spark optimizer suffixes have contiguous local proof chains and passed every separately measured necessary guard.
3. The H `dataset_id` classification survived an independently seeded M corpus: only well-shaped source-pinned dataset-ID tags appeared in observed suffix states. This improves empirical stability of the narrow exception, not a global noninterference theorem.
4. An important falsification survived: **the method does not currently certify joins, union, DISTINCT, HAVING or several nested/grouped aggregate shapes**. The generalization result is substantially weaker than the aggregate 20/48 score may imply.
5. All 412 recorded changes in the legacy event union remain UNKNOWN under *its own* older five-certifier grammar. Broader correctness and superiority over prior art (SQLSolver, HoTTSQL/Cosette, etc.) are not demonstrated.

## Highest-information next experiment

Do **not** retune M, H, K or L and rebrand a rerun as fresh validation.

A candidate Round N extension should choose **one** of the actual post-FinishAnalysis abstention families (likely `ColumnPruning` under nested projection or `CollapseProject`) and formalize the exact operator-substitution law with a small, mechanized/canonical relation semantics. Build authentic Spark negative controls that violate one necessary assumption at a time, including RDD aliasing, SQL NULL, attribute identity and nontrivial metadata. Compare against the predeclared oracle and one SQLSolver case without alleging general dominance. Once the proposed proof checker and witness set are frozen, independently generate a genuinely novel cohort N with stratum-separated analysis and abstentions reported.

No upstream PR was opened. All executions used free GitHub Actions.
