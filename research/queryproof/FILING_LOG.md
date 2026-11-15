# QueryProof filing log

Every report and PR filed upstream from QueryProof findings. Only the items judged worth filing are listed here; the lost and skipped findings are in `2026-11-14-filing-pack.md`.

Status as of 2026-11-15. Update the status column when a ticket moves.

## Reports

| Id | Kind | Project | Ticket | Filed | Our fix PR | Status |
| --- | --- | --- | --- | --- | --- | --- |
| (SPARK-60319) | wrong rows | Spark | [SPARK-60319](https://issues.apache.org/jira/browse/SPARK-60319) | 2026-10-17 | [apache/spark#59445](https://github.com/apache/spark/pull/59445) | open, no review |
| A1 | silent corruption on write | Arrow C++ / pyarrow ORC writer | [apache/arrow#55618](https://github.com/apache/arrow/issues/55618) | 2026-11-15 | — | open |
| TD | read failure | Trino | [trinodb/trino#32153](https://github.com/trinodb/trino/issues/32153) | 2026-11-15 | — (one-liner; needs Trino CLA) | open |
| Z1 | wrong rows | Spark | [SPARK-61738](https://issues.apache.org/jira/browse/SPARK-61738) | 2026-11-15 | — (fix by another contributor: [apache/spark#60522](https://github.com/apache/spark/pull/60522)) | open, PR available |
| G | wrong rows | Spark | [SPARK-61739](https://issues.apache.org/jira/browse/SPARK-61739) | 2026-11-15 | [apache/spark#60523](https://github.com/apache/spark/pull/60523) | open |
| X1 | wrong rows | DataFusion | [apache/datafusion#27455](https://github.com/apache/datafusion/issues/27455) | 2026-11-15 | — | open |
| T3 | wrong rows | Trino | [trinodb/trino#32154](https://github.com/trinodb/trino/issues/32154) | 2026-11-15 | — | open |
| W1 | read failure (regression) | arrow-rs | [apache/arrow-rs#12265](https://github.com/apache/arrow-rs/issues/12265) | 2026-11-15 | — | open |
| C2 | read failure | Trino | [trinodb/trino#32155](https://github.com/trinodb/trino/issues/32155) | 2026-11-15 | — | open |
| X2 | wrong rows | DataFusion | [apache/datafusion#27456](https://github.com/apache/datafusion/issues/27456) | 2026-11-15 | — | open |
| S | wrong rows | ORC Java | [ORC-2237](https://issues.apache.org/jira/browse/ORC-2237) | 2026-11-15 | — | open |
| N1 | query failure | Spark | [SPARK-61740](https://issues.apache.org/jira/browse/SPARK-61740) | 2026-11-15 | [apache/spark#60524](https://github.com/apache/spark/pull/60524) | open |
| N2 | planning failure (CBO) | Spark | [SPARK-61741](https://issues.apache.org/jira/browse/SPARK-61741) | 2026-11-15 | [apache/spark#60525](https://github.com/apache/spark/pull/60525) | open |

## Comments and standalone PRs

| Id | What | Link | Date | Status |
| --- | --- | --- | --- | --- |
| iceberg-rust | ask for a 0.10.x backport of the day-transform fix (#3323) for pyiceberg | [comment on apache/iceberg-rust#3315](https://github.com/apache/iceberg-rust/issues/3315#issuecomment-6891131197) | 2026-11-15 | no reply yet |
| Y1 | default Parquet `created_by` without a version | [apache/datafusion-python#1835](https://github.com/apache/datafusion-python/pull/1835) | 2026-11-15 | open |

## Totals

- 13 reports in 7 projects (12 filed 2026-11-15, plus SPARK-60319 from October).
- 8 wrong-result bugs: A1 corrupts data at write time; SPARK-60319, Z1, G, X1, X2, T3 and S return wrong rows.
- 5 failures on valid input: TD, W1, C2, N1, N2.
- Fix PRs by us: SPARK-60319, G, N1, N2, Y1. Z1 is being fixed by another contributor.

## Not filed (optional, low value)

- SPARK-59607 versions comment (3.5–4.2.1 and ORC).
- SPARK-57499 comment (recheck branch-4.2 first).
- DuckDB VARIANT decimals print `.5` after a Parquet round trip.

## Notes

- Spark fix PRs 60523–60525 were rebased on master cbfd79f17c1 (2026-11-15). N1 needed a manual merge with SPARK-30628's change to `CleanupDynamicPruningFilters`. They have not been compiled on that base yet: the fork's `Build` workflow did not start on push (no push has triggered Actions in `mhatipog/spark` since 2026-11-06).
- All Spark fix PRs answer the generative-AI question with `Generated-by: Claude Code (Claude Opus 5.5)`.
