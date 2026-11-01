# Interface hunt: results so far (2026-11-01, day 1 of about 21)

Method: `2026-11-01-interface-plan.md`. Specs: `2026-11-01-storage-interface-contracts.md`.
Drafts for filing: `UPSTREAM_FILING_GUIDE.md` §2d (G), §2e (S), §2f (T3).

## Runs

| Run | Engines | Checks | Result |
| --- | --- | --- | --- |
| 41283678653 | Spark 4.2.1, DuckDB 2.0.0, DataFusion 55 | 430,512 | completed |
| 41285645618, 41286303228 | + Trino 484 | – | **failed:** Trino did not start. The image owns `/data` and our read-only mount covered it. Kept as failed. |
| 41286769809 | all four | 525,417 | completed |

- **Probes and fix verification:**
  - 41285645614: the G and S probes on 4.0.5, 4.1.4 and 4.2.1. The 3.5.9 job failed to write pre-1582 dates without a rebase mode; the probe now sets one.
  - 41286231489: the G fix, 191 of 191 tests pass, and the probe shows no G difference on the fixed master.
  - 41286256046: the G negative control, which fails the new test.
  - 41291209213: the T3 probe on Trino.
  - 41292295627: G and S reproduce on 3.5.9 and master 4fecd73, as on 4.0.5, 4.1.4 and 4.2.1 (21 differences on each line).

## New bugs, by concept and engine

A bug counts only if it is reproduced in a minimal form and no matching JIRA, GitHub issue
or PR turns up with full-text search.

| Concept | Spark | ORC (Java library) | Trino | DuckDB | DataFusion |
| --- | --- | --- | --- | --- | --- |
| C1 NULL semantics in statistics | (N4 = SPARK-60362, known) | – | **T3**: ORC `o.i.c IS NULL` drops rows with a NULL middle ROW | – | – |
| C2 empty or pruned input | (N5 = SPARK-60487, SPARK-59607, known) | – | – | – | – |
| C3 min/max ordering | **G**: a DATE literal in the 1582 gap is pushed as 1582-10-15 (Parquet and ORC; fix verified) | **S**: predicates in UTF-16 order vs UTF-8 statistics | – | – | – |
| C4 dictionary and bloom | (SPARK-60322 signed zero, known) | – | (#30544 widened column + bloom, #31905 signed zero, known) | – | (#26491 bloom signed zero, known) |

C3 also had known issues in DuckDB (#25521, NaN excluded from min/max) and DataFusion
(#26490, NaN; #26138, -0.0 in IN lists).

- **G** is a Spark bug.
- **S** sits in the ORC Java reader, but it gives wrong results in Spark under default
  settings.
- **T3** is a Trino bug.

So **two of the three new bugs are outside Spark.**

## Rediscovered known bugs

These show the oracle detects real bugs. None of them counts as new.

- **Spark:** SPARK-60362 (N4), SPARK-60322, SPARK-59607 (via round 8), SPARK-60487 (N5).
- **ORC:** ORC-2226 (seek error for an all-NULL trailing row group).
- **DuckDB:** duckdb#25521.
- **DataFusion:** datafusion#26490, #26491, #26138.
- **Trino:** trinodb/trino#30544 and #31905.

All of these were filed between August and October 2026, so others are hunting the same
ground right now. Novelty checks must use full-text search, and must happen right before
each filing.

## Not yet classified

- **Spark and Trino, ORC files with dates before 1582:** a different calendar between the
  writer and the reader. A Spark-written ORC file read by Trino, and pyarrow-written ORC
  files read by Spark, give dates shifted from the Python answer. This looks like the
  known hybrid vs proleptic calendar mismatch (SPARK-31423 family). Check whether the
  writer marks its calendar before calling it a bug.
- **Spark, parquet-mr record-level filter (`spark.sql.parquet.recordLevelFilter.enabled`)
  with -0.0 vs 0.0:** probably the SPARK-60322 family.

## Cost: bugs per runner-hour

Cost is measured as job wall time on GitHub-hosted runners (4 vCPU), failed runs included.

| Method | Runner-hours | New bugs | New per runner-hour | Engines |
| --- | --- | --- | --- | --- |
| Random metamorphic SQL (`run_meta.py`, rounds 2–8; 13 runs) | 57.1 | 2 (N1, N2) | 0.035 | Spark, plus DuckDB and SQLite oracles |
| Spec-driven interface (4 runs) | 14.5 | 3 (G, S, T3) | 0.21 | Spark, DuckDB, DataFusion, Trino |

**Caveats, which matter for the stop rule:**

- **The baseline is retrospective.** It ran earlier and against Spark only, so it may be
  harder to beat by now (later rounds were drawn from ground already searched), or easier
  (it never ran against Trino or DataFusion).
- **The baseline omits round 1** (RUN_REQUEST, ablation and DuckDB runs), which found no
  new bug. Including it would only lower the baseline rate.
- **Small numbers.** Three bugs against two is not yet "clearly beats". The stop-rule
  check on about 2026-11-22 needs a matched-budget baseline run on the same engines
  (random SQL over random rows against Trino and DataFusion too) and more spec-driven
  rounds.
- **Novelty of G, S and T3** rests on searches done today, not on maintainer triage.
