# Interface hunt, phase two: results (from 2026-11-03)

Method: `2026-11-03-approach-v2.md`. Phase one (`2026-11-01-interface-results.md`) is frozen
and is not revised here. Drafts for filing: `UPSTREAM_FILING_GUIDE.md` §2k (W1), §2l (Y1), §2m (Z1), §2n (C2).
Triage: `tools/queryproof_interface/triage_p2.py` with `known_issues.json`; every signature
names the report or the documented behaviour it stands for.

## Runs

| Run | What | Checks | Engine core-hours | Result |
| --- | --- | --- | --- | --- |
| 41625465111 | Round A: O1 auditor and O2 writer × reader matrix over spec1–4 and random seeds 1–5 (phase-one budgets) | 9,637,793 | 76.46 (audit 0.006) | completed, 36/36 jobs |
| 41645440275 | Triage probe: chDB nested metadata, Y1 negative control (DuckDB reader only) | – | – | completed |
| 41659652665 | Round B: O3 cast templates over spec1–4 and random seeds 6–9; probe adds Trino's cause chain | 9,848,315 | 75.13 (audit 0.005) | completed, 33/33 jobs; Trino probe failed on a table-name clash (fixed) |
| 41659954483 | Round C: O4 transplants (spec5, spec4 with engine-written layouts and round-C writer settings; random seeds 10–11 with the same writers) | 4,309,158 | 32.02 (audit 0.003) | completed, 17/17 jobs; Trino probe failed again (old probe) |
| 41701059378 | Probe only (`QP_PROBE_ONLY=1`): Z1 Spark-only reproduction and controls, Y1 minimal reproduction, Trino cause chain on chDB files | – | – | completed; Z1, Y1 and C2 confirmed |

Engines and writers in round A: Spark 4.2.1, DuckDB 2.0.0, DataFusion 55.0.0, Trino 484,
Polars 2.1.0, pyarrow 26.0.0 (dataset); writers pyarrow and parquet-java (phase one) plus
Polars, DuckDB, DataFusion, fastparquet 2026.9.0 and chDB 4.4.0 (ClickHouse 26.9.2).

## Round A: what the findings were

All round-A findings outside the phase-one list were traced before counting anything as new.
Counts are findings (one per query × mode kind), not bugs.

**New (ours, not found in any tracker):**

| Id | Where | Found by | Severity | Draft |
| --- | --- | --- | --- | --- |
| W1 | arrow-rs 59+ rejects the empty-list Thrift header fastparquet writes for 0-row files (regression of #8826 by #9924); DataFusion 55 cannot open them, 53/54 can | spec and random (any 0-row fastparquet file) | read failure, no wrong rows | §2k |
| Y1 | datafusion-python `ParquetWriterOptions` default `created_by = "datafusion-python"` has no version, so parquet-java's PARQUET-246 guard rejects DELTA_BYTE_ARRAY pages (Spark row-based reader, Hive) | spec only (long strings) | read failure, no wrong rows | §2l |

Negative controls: W1, a one-row file from the same writer opens; Y1, the same bytes with a
versioned `created_by` read in every Spark mode (probe 41645440275).

**Rediscovered (already reported upstream; each was a real wrong result or failure the matrix
found on its own):**

- datafusion#26490: a plain `SELECT` returns NaN rows as the statistics constant when
  min == max (spec, O3 shapes; DataFusion 52+, bisected locally: 51 correct, 52 wrong).
- SPARK-60598: Parquet aggregate pushdown reads the wrong column's statistics when a nested
  column precedes it (random only; filed upstream 2026-10-27).
- arrow-rs#11018: skipping 256-value DELTA_BINARY_PACKED miniblocks (DuckDB's block size)
  fails; fixed on arrow-rs main by #11021, not in DataFusion 55.
- ClickHouse#119473 (fix PR #119570 open): a NULL `Nullable(Tuple)` with a nested tuple is
  written as a present struct; sibling leaves then disagree on the same row's level (seen by
  decoding the pages of the probe's files). The auditor's histogram finding is a symptom.
- arrow-rs#11963 (auditor), duckdb#27454/#27778, duckdb#25521, datafusion#18083,
  datafusion#26491/#26138/#15812, trinodb#31908/#30544, PARQUET-1222, plus phase-one items
  (N4, N5, G/GT, A1, TD, X1, SPARK-59605, SPARK-60797, ORC-2226).

**Explained, not bugs:** feature gaps (Spark vectorized BYTE_STREAM_SPLIT, Trino int/float to
double coercion, pyarrow decimal partition keys, Polars MAP), by-design behaviour (ClickHouse
has no `Nullable(Array)`/`Nullable(Map)`, integer SUM wrap in Polars and pyarrow, float SUM
order, ORC calendar before 1582), and three harness gaps (Polars timestamp literals, Python
year-0 results). A float-sum signature first matched by regex hid 409 non-float differences;
it now requires the results to differ only in floating-point rounding (`float_only`), and the
409 fall to their real signatures (N4, SPARK-60797). No new item came out of them.

**Open:** Trino rejects chDB's column index on nested columns ("Corrupted column index",
3314 findings, 19 shapes) although the index passes Trino's structural checks; chDB omits
the optional `null_counts` there. Round B's probe prints Trino's cause chain.
*Resolved after round C (probe 41701059378): a Trino bug, C2; see the round C section.*

## Round B: what the findings were

No new item. Every finding fell to an existing signature or to one of four new non-bug
classes, all from the O3 cast templates:

- harness: the template names a cast the engine rejects at analysis (Spark ANSI cast rules,
  Trino `TYPE_MISMATCH`, DataFusion unsupported casts): 2,417 findings.
- by design: a cast that fails on some row errors in one mode and is skipped in another
  (pruning, pushdown or evaluation order); no mode returns wrong rows: 1,420.
- by design: pyarrow compute refuses a lossy implicit int-to-float cast: 692.
- harness: the reference model compares a REAL column with a float64 literal; every engine
  rounds the literal to REAL and they all agree: 97.

Signatures widened rather than added: duckdb#27454 (pyarrow's "delta bit width larger than
integer bit width" on DuckDB files), datafusion#18083 (lower-case partition marker in cast
errors), the nanosecond-range limit (DataFusion `Date32` to `Timestamp`), and the Trino
UINT_32 shape list (R007_024, a `uint32` column). The Y1 negative control held again
(non-vectorized Spark fails only with the default `created_by`).

The Trino × chDB column-index group came back (2,890 findings, 14 shapes). The column indexes
Trino rejects are spec-legal as far as the probe shows: one page per chunk, matching list
sizes, `null_counts` omitted, all-NULL pages with empty bounds. Trino's
`TupleDomainParquetPredicate.getDomain` passes its size check on these, so the exception
comes from inside the conversion block; the cause chain is still to be read.
*Resolved after round C: C2, see the round C section.*

## Round C: what the findings were

Round C transplants the triggers of recent wrong-result reports from other engines (spec5)
and adds engine-written Hive layouts and round-C writer settings. Most of its wrong results
were the transplanted bugs themselves, still open or fixed only on main, now seen in more
readers than the original report named.

**New:**

| Id | Where | Found by | Severity | Draft |
| --- | --- | --- | --- | --- |
| Z1 | Spark Parquet pushdown floors a sub-millisecond TIMESTAMP literal on a TIMESTAMP_MILLIS column; `<` and `<>` prune row groups that hold matching rows (master unchanged; the ORC analogue SPARK-60326 is fixed) | spec5 only (T2, T4 with `pa_ts_millis`) | wrong rows, default configuration | §2m |

**Rediscovered (each a wrong result or failure):**

- arrow#51626 (open): pyarrow's statistics for dictionary strings under a NULL struct. The
  `pa_dict_input_bloom` files make `IS NULL` on the child drop rows in Spark, DuckDB and Trino;
  the auditor flags the same chunks (null_count, null_pages, inexact min).
- datafusion#24951 (fixed on main, not in 55): a partitioned write puts a NULL key under
  `p=0` or `s=`, so every reader returns 0 or `''` for it. Seen in all six readers.
- datafusion#22142 (fixed on main by #22906, not in 55): `CAST(ms_col AS finer) = literal` is
  rewritten to compare with the literal truncated to milliseconds; before 1970 it rounds
  toward zero, so `'1899-12-31 23:59:59.999999'` matches `1900-01-01`. Reproduced locally.
- polars#22435 (open): the same truncation in Polars, SQL and expressions. Reproduced locally.
- ORC-645 (open since 2020): `1969-12-31 23:59:59.999` written by the ORC Java writer reads
  back as `1970-01-01 00:00:00.999` in Spark, Trino and pyarrow.
- datafusion#26492, #17420 and #18083 (partition spelling, partition column inside the file,
  NULL partition); trinodb#30544 on the widened INT32 file (T6); duckdb#27454; the Trino UINT_32
  family; and the round-A items.

**Explained, not bugs:** BYTE_STREAM_SPLIT on INT32/INT64 (Trino) and on FLBA (DuckDB) are
feature gaps; Spark's backslash escapes in string literals are a template gap; the
`pa_int96_legacy_stats` files fabricate INT96 statistics with min == max, which Trino trusts by
design (PARQUET-1026), so that transplant was not a valid trigger. One interop item is left
unconfirmed: DuckDB does not parse a Hive directory `s=q?r` that Polars writes on Linux.

**C2 (new, Trino), the column-index group of rounds A–C (3,362, 2,891 and 684 findings).**
Probe 41701059378 printed the cause chain: with every page a null page and `null_counts`
omitted, `TupleDomainParquetPredicate.getDomain` hands the typed `getDomain` empty min/max
lists and `SortedRangeSet` throws `cannot use empty rangeList`, reported as a corrupted
column index. pyarrow files of the same shapes read in the same run (they write
`null_counts`). The file is spec-legal; the code is unchanged on master; trinodb#18985 (2023)
hit the same exception and was closed without a file. Query failure, no wrong rows. Draft §2n.

**Probe 41701059378, the other checks:** Z1 reproduced with files Spark wrote itself (`<` 0
rows instead of 1, `<>` 1 instead of 2, vectorized on and off, only with pushdown); controls
correct (pushdown off, TIMESTAMP_MICROS files, literals on the millisecond grid). The Y1
minimal reproduction fails with the default `created_by` and reads 5 rows with the control.

## Reading so far

Round A spent 76 core-hours for two new low-severity bugs and about a dozen rediscoveries,
most reported in the last three months. The writer × reader axis does find real defects
(W1, Y1, and independently datafusion#26490, ClickHouse#119473, arrow-rs#11018), but the
trackers are crowded: every wrong-result finding of round A was already reported.

After rounds B and C (23.8M checks, 183.6 engine core-hours in all):

| Oracle | Round | Core-hours | New | Notes |
| --- | --- | --- | --- | --- |
| O1 auditor + O2 writer × reader | A | 76.5 | W1, Y1, C2 | C2 resolved after C |
| O3 cast templates | B | 75.1 | none | about 4,600 findings were template or by-design noise |
| O4 transplanted triggers | C | 32.0 | Z1 | most hits were the transplanted bugs, still open or fixed only on main |

Per core-hour the transplants (O4) were the most productive and the cast templates (O3) the
least: engines reject or order-evaluate failing casts in documented ways, and the oracle has
to model each of them. Z1 is the only new wrong-result bug of phase two; W1, Y1 and C2 are
read failures. The O4 hits show a second use of the method: a report filed against one
engine is checked against all the others (arrow#51626 drops rows in Spark, DuckDB and Trino;
datafusion#24951 is wrong in all six readers; the Polars and DataFusion literal truncation
is the same bug as Z1 in two other engines).
