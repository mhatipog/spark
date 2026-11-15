# Programme v3, round 1: results (2026-11-14)

Plan: `2026-11-14-programme-v3.md`. Filing decisions: `2026-11-14-filing-pack.md`.
Scripts: `tools/queryproof_v3/`. Hunts HA–HD ran locally on Windows. HE (Spark-written tables)
is committed for CI.

Versions: pyarrow 26.0.1, DuckDB 2.0.0 (iceberg and delta extensions), DataFusion 55.0.0,
Polars 2.1.0, pyiceberg 0.12.0 with pyiceberg-core 0.10.1, deltalake 1.6.6, fastparquet 2026.9.0.

## Hunts

| Hunt | Family | What | Size | Raw differences | Survived triage | New after the tracker search |
| --- | --- | --- | --- | --- | --- | --- |
| HA `hunt_ice.py` | F1, F4, F8 | Iceberg written by pyiceberg: identity/bucket/truncate/day/hour/month/year partitions, truncate(16) metrics, NaN/±0. Read by pyiceberg, Polars (native with metadata statistics on and off, and the pyiceberg reader) and DuckDB `iceberg_scan` | 10 types, 76 tables, about 12k predicate × reader checks | 336 | 7 root causes | 0 |
| HB `hunt_trunc.py` | F1, F4 | truncated BYTE_ARRAY statistics and column indexes (length 8, arrow-rs) vs pyarrow; U+10FFFF, `0xFF`, multi-byte characters cut at the boundary. Read by DuckDB, DataFusion (4 modes), Polars, pyarrow | 2 types × 4 layouts | 0 (harness only) | — | — |
| HC `hunt_delta.py` | F1, F3, F8 | Delta written by delta-rs (one file per row, one file, partitioned). Read by delta-rs SQL, Polars `scan_delta`, DuckDB `delta_scan` | 8 types × 3 layouts | 139 | 2 root causes + datafusion#26490 | 0 |
| HD `hunt_del.py` | F3 | answers from Iceberg metadata after deletes (Polars `fast_deletion_count` / `use_metadata_statistics`, DuckDB) | 84 tables | 0 | — | **invalid:** pyiceberg 0.12 silently uses copy-on-write even with `write.delete.mode=merge-on-read`, so no delete files were written. Redone as HE |
| HE `hunt_spark_tables.py` | F3, F5 | Spark 4.0.1 + Iceberg 1.10.0 (v2 merge-on-read position deletes, v3 deletion vectors, type promotion, rename) and Delta 4.0.0 (deletion vectors, column mapping); read by pyiceberg, Polars, DuckDB, deltalake | 14 scenarios | not run yet: GitHub had not processed the push (f53c643d08) when this was written | | |

## Root causes found, and what happened to each

| Id | Where | Finding | Verdict |
| --- | --- | --- | --- |
| I1 | pyiceberg | A double literal on a float column: `FloatLiteral` keeps the unrounded `.value`, and the inclusive metrics evaluator compares bounds with it, while row filtering uses the float32 value. `EqualTo("v", 0.1)` returns nothing; `0.10000000149021612` returns the row or not depending on the file layout | lost: iceberg-python#4108 (open since 10-14) |
| I2 | pyiceberg | `NOT IN {-0.0, …}` drops `0.0` when each value is in its own file, but not when they share one file | too small; area just reworked (#4324, #4331); not filed |
| I3 | pyiceberg | NaN in an identity-partitioned float column, ZeroDivisionError in `bin_pack_arrow_table` (the NaN partition filter matches 0 rows) | lost: #4030 (merged 09-30, not in 0.12.0) |
| I4 | pyiceberg | month/year projection raises OverflowError for `d < DATE '0001-01-01'` / `d > DATE '9999-12-31'` | type-range edge, not filed |
| I5 | pyiceberg | `truncate[4]` on `decimal(9,2)` refuses `-9999999.99` (the result needs precision 10) | spec corner, not filed |
| DAY | pyiceberg-core 0.10.1 (iceberg-rust) | `day()` of some pre-1970 timestamps late in the day is one day late (`1899-12-31 23:59:59.5` → 1900-01-01). All four readers then miss the row | lost: iceberg-rust#3315/#3323, released in 0.11.0, but pyiceberg pins `<0.11`; backport comment drafted |
| D1 | delta-rs 1.6.6 | Decimal statistics go through f64 (`-1.000000000000000001` → `-1.0`), giving wrong rows in delta-rs SQL, Polars and DuckDB | lost: fixed on main 11-01, unreleased |
| D2 | DataFusion / DuckDB | NaN is left out of Parquet/Delta max; engines that sort NaN last prune files whose only match is NaN | known: datafusion#15812, duckdb#25521 |
| — | DataFusion | NaN rows read back as the statistics constant when min == max | known: datafusion#26490 |

Re-checked today on current releases: A1, X1, X2, W1 and Y1 still reproduce. On the main
branches, the code behind Z1, N2, G, TD, C2, X1 and X2 is unchanged.

## Reading

1. **Zero new bugs from about 15 local hunt-hours.** Every real root cause was already
   reported or fixed. Most were fixed in October/November 2026, several within days of each
   other. iceberg-python and delta-rs show a run of fixes in exactly the families this
   programme targets (float literals, NaN, signed zero, decimal statistics, pre-epoch
   transforms). Someone, probably with similar tooling, is sweeping the Python and Rust
   lakehouse libraries right now.
2. **The family method works as a detector**, but not, in this round, as a source of new
   bugs. In one afternoon HA and HC found 7 genuine root causes that other people needed
   weeks of reports to find. That supports using historical recall (§6 of the plan) as the
   evaluation, not the count of new bugs.
3. **Crowded ground moves the remaining yield to:**
   - the Java implementations (Spark, Iceberg Java, Delta Spark, Trino), where our prior
     finds are and where the Python sweep is not;
   - writers whose files other engines read (A1, W1, C2 were all writer × reader).
   That is why HE exists.
4. **Harness lesson:** check that the feature under test was actually exercised (HD) before
   counting a clean result.

M1 gate status (≥ 4 new wrong-result bugs in ≥ 3 projects by 11-24): 0 so far from v3. The
backlog from earlier rounds already holds 6 unfiled wrong-result bugs in 5 projects (A1, Z1,
T3, X1, X2, G).

## Round 2 (2026-11-14/15): the Java side

Fork push triggers stopped firing after 11-06 (reruns still work), so these ran on the runner
pool from `mhatipog/web-operations` branch `research/qp-v3-hunts-20261114`
(`research/qp-v3/`, workflow `qp-v3-hunt.yml`). Spark 4.0.1, Iceberg 1.10.0, Delta 4.0.0.

| Hunt | What | Result | Verdict |
| --- | --- | --- | --- |
| HE | Spark writes Iceberg v2 merge-on-read (position deletes), v3 deletion vectors, type promotion and rename, and Delta with deletion vectors and column mapping. Read by pyiceberg, Polars (metadata statistics and fast deletion counts on and off), DuckDB, deltalake | Iceberg: no differences in any reader. Delta: DuckDB `delta_scan` returns 0 rows for `g = 1` on column-mapped partitioned tables | known: duckdb-delta#343, fixed by #408 (10-20). Polars and deltalake correctly refuse column mapping |
| HG | pyiceberg and delta-rs tables read by Iceberg Java and Delta Spark, against the same data files read with pushdown off | string/binary: 0 wrong over 13 tables × 591 predicates (truncated bounds handled). float/double: `-0.0` vs `0.0` and NaN vs `> Infinity` | ±0 and NaN: SPARK-60322 (10-17), lost. dec38 on Delta: D1 (fixed on delta-rs main) |
| HG | dates and timestamps in the 1582 gap | Delta tables lose rows for `=`, `>=`, `>` on gap dates | **our G.** Probe: depends only on `spark.sql.parquet.filterPushdown`, not on Delta's stats skipping. It extends G: equality fails too when the file holds a gap value (verified with a pyarrow-written file) |
| HH | Spark datetime pushdown with JVM zone ≠ session zone (JVM America/Los_Angeles; session UTC/LA/Kolkata; java8 API on/off; CORRECTED/LEGACY; Parquet MICROS/MILLIS, ORC) | 401 differences, all explained: G (1582 gap; with a non-UTC JVM the window shifts, as G's fix already allows for), Z1 (MILLIS, found again independently), ORC sub-millisecond stored values | ORC: SPARK-60326/60436 (fixed in 4.2.2/4.3.1/4.4.0, present in 4.0.1); probe shows no JVM-zone dependence. No new root cause |
| HI | pyarrow ORC round trip over random nested schemas (about 9,500) | only A1 | **A1 scope widened:** any struct with nulls below a struct without nulls, also through list/map. Outside that shape, 0 differences in about 7,000 schemas |
| HJ | DuckDB 2.0 shredded VARIANT in Parquet, filters on shredded paths | no pruning differences. Decimal VARIANTs with \|x\| < 1 read back as `DECIMAL(s, s)` and print `.5` instead of `0.5`, so `v::VARCHAR` changes after a round trip | low; follows duckdb#26180. Optional report |

**Competition.** Since October, at least three reporters have filed Spark pushdown
correctness tickets with essentially our method, on one file per value with pushdown on and
off: SPARK-60152, 60322, 60326, 60380, 61366, 61463, among others. Z1 (Parquet MILLIS) and G
(Parquet DATE/TIMESTAMP gap) were still unreported on 2026-11-15, but they are the next cells
of that sweep.

**Cumulative v3: 0 new root causes; 2 existing reports strengthened (A1, G).**
