# QueryProof — controlled top-k membership cross-check, 2026-10-17

## Decisive empirical result

**Both independent versioned jobs passed:** [GitHub Actions 39060749288](https://github.com/mhatipog/spark/actions/runs/39060749288).

The same independent input-stage, partition-composable top-k witness was compared to the output row identities of (i) *plain* CTE and safe projections and (ii) *join-producing correlated subquery* projections. Each experiment uses the six distinct input ages and original correlated count/EXISTS shape from upstream Spark PR #59445. The output is checked as a **multiset of selected unique names**: an order-insensitive membership witness, not a second full SQL execution.

### Results by Spark version and projection type

| Query projection | Spark 4.0.1 violations / runs | Spark 4.2.0 violations / runs |
|---|---:|---:|
| Plain name selection | 0 / 8 | 0 / 8 |
| Deterministic `length(name)` | 0 / 8 | 0 / 8 |
| Uncorrelated scalar `max(b)` | 0 / 8 | 0 / 8 |
| Uncorrelated EXISTS | 0 / 8 | 0 / 8 |
| **Correlated scalar COUNT** | **8 / 16** | **16 / 16** |
| **Correlated EXISTS** | **8 / 16** | **16 / 16** |
| **TOTAL** | **16 / 64** | **32 / 64** |

Across both versions, **48/64 correlated-subquery executions yielded a source-stage top-k membership disagreement; none of 64 control executions did**. There were no run errors and no UNKNOWN verdicts in the supported unique-age and unique-name fixture.

### Why the release difference matters

- 4.0.1: the correlated `LIMIT 3` query failed **16/16** combinations of COUNT/EXISTS, AQE enabled/disabled and repeats; correlated `OFFSET 3` passed **16/16**.
- 4.2.0: both correlated `LIMIT 3` and correlated `OFFSET 3` failed **16/16** each, with AQE enabled or disabled.
- All four non-correlated/safe-projection control families passed in both releases.

This isolates the observed discrepancy to the correlated projection paths **within the tested fixture** and distinguishes the scope of the two Spark releases. It does not prove which specific optimizer rule first causes the 4.2.0 offset behavior or establish that the offset bug was introduced in 4.2.0: different planner choices and plans remain to be examined. Do not claim independent bug discoveries.

### Reproducibility details

- Original six rows and related 4-row `k` table mirror [PR #59445](https://github.com/apache/spark/pull/59445).
- Both official released PySpark versions installed untouched. Spark runs with 5 shuffle partitions, auto broadcast threshold -1; AQE enabled and disabled in separate sessions.
- Distinct ages make a deterministic total ranking on age DESC; unique names make the output identities checkable.
- Independent source-stage reference `Top_(offset+limit)` formed by per-partition bounded heaps, then a bounded merge. Source table scanned separately *before* evaluating the correlated projection. This extra source scan has an execution cost **not yet measured**.
- The known-ordering SPARK-59126 calibration still detected 3/3 violations with AQE enabled per release. On the 17 frozen independent SQL family controls, 60 in-scope executions observed no ordering violations, and eight unsupported runs returned UNKNOWN.
- Tests of the associative top-k summary passed 75 seeded partition layouts times six q values, alongside null/tie/mutation order contract tests.

## Scientific interpretation

Two different necessary contracts now have executable, independently run evidence on real released engines:

1. **Result sequence monotonicity**: catches global ORDER BY inversions, even when the bag of rows is correct.
2. **Source-stage top-k selection membership**: catches wrong selected identities after LIMIT/OFFSET, even if the projected rows can be sorted without exposing the error.

These elementary checks are **not original theoretical discoveries**. A research contribution would require safely extracting and maintaining the obligations automatically through Spark Catalyst and AQE without manually naming a source relation or an exposed key. In particular, ambiguous ties, joins that change row multiplicity, hidden ordering keys, and unsupported comparators require explicit UNKNOWN results.

## Next strict scientific milestone

Freeze an *unseen* Spark correctness-issue benchmark that includes errors neither ordering nor top-k membership can catch (wrong aggregates, predicates and NULL behaviors), then test a provenance-aware `PlanContract` effect-system checker against independent SQL reference oracles. Report its real recall and specificity including UNKNOWN, compare it to existing DBMS testing techniques, and measure overhead. **No claim of general verification, novel Spark defect, or publishable high-impact method yet.**

This branch does not touch the existing upstream PR, and the earlier known AQE Sort-loss root cause already has a distinct upstream cardinality repair.
