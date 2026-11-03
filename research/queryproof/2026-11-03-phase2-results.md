# Interface hunt, phase two: results (from 2026-11-03)

Method: `2026-11-03-approach-v2.md`. Phase one (`2026-11-01-interface-results.md`) is frozen
and is not revised here. Drafts for filing: `UPSTREAM_FILING_GUIDE.md` §2k (W1), §2l (Y1).
Triage: `tools/queryproof_interface/triage_p2.py` with `known_issues.json`; every signature
names the report or the documented behaviour it stands for.

## Runs

| Run | What | Checks | Engine core-hours | Result |
| --- | --- | --- | --- | --- |
| 41625465111 | Round A: O1 auditor and O2 writer × reader matrix over spec1–4 and random seeds 1–5 (phase-one budgets) | 9,637,793 | 76.46 (audit 0.006) | completed, 36/36 jobs |
| 41645440275 | Triage probe: chDB nested metadata, Y1 negative control (DuckDB reader only) | – | – | completed |
| 41659652665 | Round B: O3 cast templates over spec1–4 and random seeds 6–9; probe adds Trino's cause chain | | | queued |
| 41659954483 | Round C: O4 transplants (spec5, spec4 with engine-written layouts and round-C writer settings; random seeds 10–11 with the same writers) | | | queued |

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

## Reading so far

Round A spent 76 core-hours for two new low-severity bugs and about a dozen rediscoveries,
most reported in the last three months. The writer × reader axis does find real defects
(W1, Y1, and independently datafusion#26490, ClickHouse#119473, arrow-rs#11018), but the
trackers are crowded: every wrong-result finding of round A was already reported.
