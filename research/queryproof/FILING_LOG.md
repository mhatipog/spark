# QueryProof filing log

Every report and PR filed upstream from QueryProof findings. Only the items judged worth filing are listed here; the lost and skipped findings are in `2026-11-14-filing-pack.md`.

Status as of 2026-11-18. Update the status column when a ticket moves.

## Reports

| Id | Kind | Project | Ticket | Filed | Our fix PR | Status |
| --- | --- | --- | --- | --- | --- | --- |
| (SPARK-60319) | wrong rows | Spark | [SPARK-60319](https://issues.apache.org/jira/browse/SPARK-60319) | 2026-10-17 | [apache/spark#59445](https://github.com/apache/spark/pull/59445) | open, no review |
| A1 | silent corruption on write | Arrow C++ / pyarrow ORC writer | [apache/arrow#55618](https://github.com/apache/arrow/issues/55618) | 2026-11-15 | — | open |
| TD | read failure | Trino | [trinodb/trino#32153](https://github.com/trinodb/trino/issues/32153) | 2026-11-15 | — (one-liner; needs Trino CLA) | open |
| Z1 | wrong rows | Spark | [SPARK-61738](https://issues.apache.org/jira/browse/SPARK-61738) | 2026-11-15 | — (fix by another contributor: [apache/spark#60522](https://github.com/apache/spark/pull/60522)) | open, PR available |
| G | wrong rows | Spark | [SPARK-61739](https://issues.apache.org/jira/browse/SPARK-61739) | 2026-11-15 | [apache/spark#60523](https://github.com/apache/spark/pull/60523) | open |
| X1 | wrong rows | DataFusion | [apache/datafusion#27455](https://github.com/apache/datafusion/issues/27455) | 2026-11-15 | — | open |
| T3 | wrong rows | Trino | [trinodb/trino#32154](https://github.com/trinodb/trino/issues/32154) | 2026-11-15 | — | **retracted 2026-11-20**: closed by us as not a Trino bug; the PyArrow fixture was corrupted by A1 (child stats count == rows). Spark/Trino-written files correct on 484. Evidence: web-operations branch research/qp-trino-hive-verified-orc-20261120 |
| W1 | read failure (regression) | arrow-rs | [apache/arrow-rs#12265](https://github.com/apache/arrow-rs/issues/12265) | 2026-11-15 | — (fix by another contributor: [apache/arrow-rs#12266](https://github.com/apache/arrow-rs/pull/12266)) | open, PR available |
| C2 | read failure | Trino | [trinodb/trino#32155](https://github.com/trinodb/trino/issues/32155) | 2026-11-15 | — | open |
| X2 | wrong rows | DataFusion | [apache/datafusion#27456](https://github.com/apache/datafusion/issues/27456) | 2026-11-15 | — | open |
| S | wrong rows | ORC Java | [ORC-2237](https://issues.apache.org/jira/browse/ORC-2237) | 2026-11-15 | — | open |
| N1 | query failure | Spark | [SPARK-61740](https://issues.apache.org/jira/browse/SPARK-61740) | 2026-11-15 | [apache/spark#60524](https://github.com/apache/spark/pull/60524) | open |
| N2 | planning failure (CBO) | Spark | [SPARK-61741](https://issues.apache.org/jira/browse/SPARK-61741) | 2026-11-15 | [apache/spark#60525](https://github.com/apache/spark/pull/60525) | open |
| DN | wrong rows (delta-rs NaN-excluding float stats; Delta Spark skipping drops NaN rows) | delta-rs | [delta-io/delta-rs#5074](https://github.com/delta-io/delta-rs/issues/5074) | 2026-11-16 | — | open |
| DZ | wrong rows (float partition filters use total order, -0.0 vs 0.0) | delta-rs | [delta-io/delta-rs#5078](https://github.com/delta-io/delta-rs/issues/5078) | 2026-11-16 | — | open |
| TM | wrong rows (truncate(W) wraps near INT_MIN/LONG_MIN; Spark, pyiceberg, Trino drop rows) | Iceberg Java (+ pyiceberg) | [apache/iceberg#19023](https://github.com/apache/iceberg/issues/19023) | 2026-11-18 | — | open |
| IN | wrong rows (ORC: IS NULL on a struct field misses rows whose parent struct is NULL; OrcMetrics null count 0 + SearchArgument isNull; SELECT and DELETE, Spark and Trino) | Iceberg Java | [apache/iceberg#19064](https://github.com/apache/iceberg/issues/19064) | 2026-11-20 | [#19065](https://github.com/apache/iceberg/pull/19065) | open; fix verified (web-operations run 44757359913) |
| ON | wrong rows (ORC Java SearchArgument IS_NULL on a field nested in a struct skips row groups where the struct is NULL; 1.9.8, 2.1.4, 2.3.1) | Apache ORC (Java) | [ORC-2238](https://issues.apache.org/jira/browse/ORC-2238) | 2026-11-20 | [apache/orc#2742](https://github.com/apache/orc/pull/2742) | open; fix verified (web-operations run 44764383984: 311 tests pass, control fails) |

## Comments and standalone PRs

| Id | What | Link | Date | Status |
| --- | --- | --- | --- | --- |
| iceberg-rust | ask for a 0.10.x backport of the day-transform fix (#3323) for pyiceberg | [comment on apache/iceberg-rust#3315](https://github.com/apache/iceberg-rust/issues/3315#issuecomment-6891131197) | 2026-11-15 | no reply yet |
| Y1 | default Parquet `created_by` without a version | [apache/datafusion-python#1835](https://github.com/apache/datafusion-python/pull/1835) | 2026-11-15 | open |
| SPARK-59607 | add affected versions 3.5–4.2.1 and ORC | [comment](https://issues.apache.org/jira/browse/SPARK-59607?focusedCommentId=18145189) | 2026-11-15 | no reply yet |
| iceberg ±0 | signed-zero evidence from the skip model | [comment on apache/iceberg#18723](https://github.com/apache/iceberg/issues/18723#issuecomment-6916031458) | 2026-11-16 | open |
| iceberg NaN | NaN under Spark order still wrong on 1.12.0 | [comment on apache/iceberg#9130](https://github.com/apache/iceberg/issues/9130#issuecomment-6916032118) | 2026-11-16 | closed stale |
| X1 floats | float partition spellings | [comment on apache/datafusion#27455](https://github.com/apache/datafusion/issues/27455#issuecomment-6921404161) | 2026-11-16 | open |
| W1 follow-up | bharadwaj-pendyala offered to fix W1; we said go ahead and offered to test | [comment on arrow-rs#12265](https://github.com/apache/arrow-rs/issues/12265#issuecomment-6896079067) | 2026-11-15 | fix PR apache/arrow-rs#12266 opened 14:50Z, CI green; we tested it on real fastparquet 2026.9.0 files ([comment](https://github.com/apache/arrow-rs/pull/12266#issuecomment-6901877877)): fixed, control unchanged |

## Totals

- 16 reports in 9 projects (12 filed 2026-11-15, 2 on 2026-11-16, 1 on 2026-11-18, plus SPARK-60319 from October).
- 12 wrong-result bugs (IN and ON added 2026-11-20): A1 corrupts data at write time; SPARK-60319, Z1, G, X1, X2, S, DN, DZ and TM return wrong rows. (T3 retracted 2026-11-20: caused by A1.)
- 5 failures on valid input: TD, W1, C2, N1, N2.
- Fix PRs by us: SPARK-60319, G, N1, N2, Y1. Z1 and W1 are being fixed by other contributors.

## Not filed (optional, low value)

- TD and C2 fix PRs (Trino): blocked until the Trino CLA is signed.
- X2 fix PR (DataFusion): possible, needs a machine that can build DataFusion.
- SPARK-57499 comment (recheck branch-4.2 first).
- DuckDB VARIANT decimals print `.5` after a Parquet round trip.

## Notes

- Spark fix PRs 60523–60525 were rebased on master cbfd79f17c1 (2026-11-15). N1 needed a manual merge with SPARK-30628's change to `CleanupDynamicPruningFilters`. The fork's `Build` workflow did not start on push (no push has triggered Actions in `mhatipog/spark` since 2026-11-06), so the suites were run on the Oracle VM (Java 21, aarch64) and the results posted on each PR on 2026-11-15:
  - G: FileBasedDataSourceSuite + Parquet/ORC V1/V2 filter suites, 261 passed, 0 failed. Control (master + new tests only): 2/2 new tests fail.
  - N1: all 8 DynamicPartitionPruning suites, 352 passed, 0 failed, 11 ignored (pre-existing AQE-on ignores). Control: new test fails in 6/8 suites with "IN dynamicpruning#N has not finished"; the two V2CatalystFilter suites pass either way.
  - N2: FilterEstimationSuite, 90 passed, 0 failed. Control: 2/2 new tests fail.
  - Logs on the VM: `~/qp-verify-logs/` (script `~/qp_verify.sh`).
- All Spark fix PRs answer the generative-AI question with `Generated-by: Claude Code (Claude Opus 5.5)`.
