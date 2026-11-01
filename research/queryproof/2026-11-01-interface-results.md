# Interface hunt: results so far (2026-11-01, day 1 of about 21)

Method: `2026-11-01-interface-plan.md`. Specs: `2026-11-01-storage-interface-contracts.md`.
Drafts for filing: `UPSTREAM_FILING_GUIDE.md` §2d (G, with GT), §2e (S), §2f (T3), §2g (A1), §2h (TD).

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
| C3 min/max ordering | **G**: a DATE literal in the 1582 gap is pushed as 1582-10-15 (Parquet and ORC; fix verified); pair two: also TIMESTAMP (GT, Parquet) | **S**: predicates in UTF-16 order vs UTF-8 statistics | **TD** (pair two): DELTA_BINARY_PACKED TINYINT/SMALLINT read fails, found on small-integer bounds | – | – |
| C4 dictionary and bloom | (SPARK-60322 signed zero, known) | – | (#30544 widened column + bloom, #31905 signed zero, known) | – | (#26491 bloom signed zero, known) |

C3 also had known issues in DuckDB (#25521, NaN excluded from min/max) and DataFusion
(#26490, NaN; #26138, -0.0 in IN lists).

- **G** is a Spark bug.
- **S** sits in the ORC Java reader, but it gives wrong results in Spark under default
  settings.
- **T3** is a Trino bug.

So **two of the three new bugs are outside Spark.**

The random baseline adds a fourth new bug outside Spark: **A1**, the pyarrow (Arrow C++) ORC
writer corrupting `struct<struct>` (concept C1, writer side). The section below covers it.

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

## Matched random baseline (built 2026-11-01)

`random_gen.py` (enabled with `QP_IFACE_GEN=random:SEED`) replaces only the shapes and the
queries. Everything else stays the same: the same writers, engines, modes and oracle.

- **Shapes:** random column types (the same type language, including nested types), random
  rows with 15% NULLs, split into 1–4 files, some of them empty.
- **Queries:** 114 random predicate and aggregate queries per shape. That gives about as
  many checks per pass as the contract templates.
- **Values:** uniform values plus the constants generic fuzzers use (0, ±1, type limits,
  the empty string). There are no contract-derived values: no NaN, no signed zero, no
  characters above U+FFFF, no gap dates.
- **Passes:**
  - pass A, run 41293634868: seed 1, four engines, 536,256 checks;
  - pass B, run 41293662806: seed 2, Spark, DuckDB and DataFusion, 437,760 checks.

  These mirror the two successful spec passes.
- **Harness change for both methods:** floats are compared to 12 significant digits,
  because a SUM depends on the order of its inputs. The smoke test showed this noise; no
  spec-driven bug depended on it.

**Baseline triage.** All 593 findings are classified.

| Findings | Class | New? |
| --- | --- | --- |
| 273 | `x IN (…, NULL)` pushed to ORC throws NPE: SPARK-59605 | known |
| 139 | "No min/max" / "Number of nulls not set" under aggregate pushdown | by design |
| 80 | ORC seek error: ORC-2226 | known |
| about 60 | ORC `IS NULL` / `<=> NULL` on a struct field: N4 = SPARK-60362 | known |
| 6 | COUNT over an empty file with aggregate pushdown: N5 = SPARK-60487 | known |
| 29 | **A1:** pyarrow's ORC writer corrupts `struct<struct>` when only the inner struct has NULLs. Child values shift and 0 is inserted. pyarrow 12.0.1–26.0.0; both pyarrow and ORC Java read the shifted values. | **new** (Arrow) |
| 6 | DataFusion BIGINT `SUM` wraps on overflow | semantics (DECIMAL variant is datafusion#26648) |

- **Where A1 was found:** as a `semantic_diff` (Spark with skipping off against the Python
  answer). Run 41299684841 bisected it, and run 41300305945 checked the pyarrow versions.
- **Why the spec set missed A1:** in every C1 file with a NULL inner struct, the outer
  struct was also NULL somewhere, or each row had its own file. The contract named the
  shape ("NULL at each level"), but the generator combined the levels too coarsely. That
  is a fair point in the baseline's favour.

## Cost: bugs per engine core-hour (the matched comparison)

Engine core-seconds are the time inside engine runs, times the cores each engine uses, as
measured by `run_interface.py`.

| Method | Passes | Engine core-hours | Checks | New bugs | New per core-hour | Outside Spark |
| --- | --- | --- | --- | --- | --- | --- |
| Spec-driven (C1–C4) | 41283678653, 41286769809 | 10.25 | 955,929 | 3 (G, S, T3) | 0.29 | 2 (S: ORC Java, T3: Trino) |
| Random, same harness | 41293634868, 41293662806 | 10.03 | 974,016 | 1 (A1) | 0.10 | 1 (A1: Arrow) |
| Random SQL (`run_meta.py`, rounds 2–8, retrospective) | 13 runs | about 57 runner-hours | – | 2 (N1, N2) | about 0.035 per runner-hour | 0 |

**Runner-hours** (job wall time, failed runs included):

- spec-driven: 14.5;
- matched random baseline: 4.9.

The spec passes ran about 4× longer per shard for the same engine time. Most likely
the cause is per-shape overhead outside the engine calls (Spark's own writers and the
Trino table creation per file set), which is the same code for both methods. This is not
explained yet. The engine core-hours are the matched measure, and wall time is reported as
recorded.

**Where this leaves the stop rule** (day 1 of about 21):

- Spec-driven found 3 new bugs and random 1, at the same engine compute. Both found bugs
  outside Spark.
- The bugs differ in kind:
  - spec: ordering and statistics semantics (G, S, T3);
  - random: a writer bug from a mix of NULL levels (A1).
- Three against one is a large ratio but a small count. Another matched pair is needed
  before 11-22: new spec shapes written from C1–C4 only, against new random seeds.
- The spec generator should combine NULL levels per file the way the random one does,
  without looking at A1's data. The contract already asks for it.

## Pair two (2026-11-01)

**What changed, for both methods alike:**

- new types: TINYINT, SMALLINT and TIMESTAMP;
- two new writer settings: pyarrow with statistics only on `k` (`pa_stats_k_only`), and
  parquet-mr with string statistics and the column index truncated to 4 bytes
  (`mr_parquet_trunc4`);
- Spark's session time zone is fixed to UTC.

**The two passes:**

- **Spec:** `spec2_shapes.py`, 18 shapes written from the contract items of C1–C4 that the
  first set covered too coarsely or not at all:
  - NULL levels combined within one file (the gap that let A1 through);
  - lists of structs, and structs of lists;
  - children that are all NULL;
  - missing statistics;
  - empty files with nested columns;
  - timestamps from year 1 to 9999, including the gap;
  - strings around the truncation limits;
  - DECIMAL(38,10) bounds;
  - small-integer bounds and widening.
- **Random:** seed 3, 32 shapes, 76 queries per shape, so the local check counts match
  (95,600 against 97,280 on DuckDB and DataFusion).

| Run | Method | Checks | Findings | Engine core-hours | Runner-hours |
| --- | --- | --- | --- | --- | --- |
| 41302131902 | spec2 | 446,022 | 528 | 5.22 | 2.48 |
| 41302164093 | random seed 3 | 449,920 | 190 | 5.01 | 2.29 |

The runner-hours are now close (pair one: 14.5 against 4.9). Pair one's spec figure
included two failed Trino starts and larger shapes, so the 4× gap noted above was mostly
not the method.

**Spec2 triage** (528 findings, all classified):

| Cluster | Class |
| --- | --- |
| pyarrow ORC `struct<struct>` read wrong with skipping off (Spark and Trino) | A1, known (ours) |
| ORC Java writer, `IS NULL` on a nested field | N4 = SPARK-60362 |
| Spark ORC "Seek … outside of the data" on all-NULL row groups | ORC-2226 |
| ORC TIMESTAMP equality with sub-millisecond digits | SPARK-60326 |
| Spark-written ORC moves 1582-10-05..14 values by 10 days (skipping off) | SPARK-31423 (Not A Problem) |
| Trino bloom filter, SMALLINT read as BIGINT | trinodb#30544 |
| DataFusion INT96 timestamps outside 1677–2262 wrap | datafusion#5655 / #9981 (`coerce_int96` opt-in) |
| **GT:** Spark Parquet pushdown of a TIMESTAMP literal on a gap day, shifted 10 days | **same root cause as G**, new type |
| **TD:** Trino fails reading DELTA_BINARY_PACKED TINYINT/SMALLINT ("byte overflow") | **new** (Trino) |

**Random seed 3 triage** (190 findings, all classified): SPARK-59605 (ORC `IN` with NULL),
ORC-2226, N4, A1, the by-design aggregate-pushdown errors, and **TD**, found independently
in a TINYINT column of a Spark v2 Parquet file. Nothing else new.

**GT.**
- Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master, for pyarrow-written and
  Spark-written TIMESTAMP_MICROS files (runs 41308537516 and 41309355229; 8 wrong
  queries per line).
- The Java 8 time API is correct. The V2 reader pushes no timestamp filter.
- G's fix branch now covers TIMESTAMP too (commit f6e82df: a test plus a UTC window
  widened by a day). Verification run 41310770200, negative control 41310801761.
  - *Added later the same day:* run 41310770200 **failed**. f6e82df built a `NumericRange`
    of microseconds, which has too many elements and throws. Commit 406b84e99b0 compares
    with two bounds instead. On it, 81/81 tests of the suite pass (run 41325775517, whose
    end-to-end step was misconfigured), and the end-to-end probe on the fixed build shows
    every G and GT line equal (run 41326938070; its 8 remaining differences are S, which
    this branch does not touch). The control run 41310801761 fails both new tests.
- It joins G's draft (§2d) instead of becoming a new JIRA, and is not counted as a new bug.

**TD.**
- Trino 476, 480 and 484 fail on `[-128, 127]` and on a page ending in one large jump
  (probe run 41310624280). A ramp, and 5,000 and 200,000 uniform random values over
  several page sizes, read correctly.
- The cause is an exact cast of the block's minimum delta when a miniblock has bit
  width 0 (`DeltaPackingUtils`, unchanged on master).
- It is user-facing in the sense of the rule: default Trino settings and any writer that
  uses DELTA_BINARY_PACKED for small integers. The data trigger is narrow, and a large
  uniform input does not reach it.
- Draft: §2h.

**Oracle gap found by TD.**
- An error in *every* mode was never recorded, because skip and mode checks compare modes
  with each other and the semantic check needs an "off" result.
- TD surfaced only because Trino's default mode skipped the bad file for some queries.
- `run_interface.py` now records `all_error` (every mode fails on a query the generator
  answers). Pair one and both pair-two passes ran without it, so their counts may miss
  such failures, for both methods equally.

**Cumulative matched comparison (pairs one and two):**

| Method | Engine core-hours | Checks | New bugs | Per core-hour | Outside Spark | Found only by this method |
| --- | --- | --- | --- | --- | --- | --- |
| Spec-driven | 15.47 | 1,401,951 | 4 (G, S, T3, TD) | 0.26 | 3 (S, T3, TD) | 3 (G, S, T3), plus GT |
| Random, same harness | 15.04 | 1,423,936 | 2 (A1, TD) | 0.13 | 2 (A1, TD) | 1 (A1) |

**Reading for the stop rule:**

- Pair two on its own is a tie: one new bug each, and it is the same bug. Spec2 also
  extended G to a second type.
- Overall, spec-driven is ahead two to one per core-hour, and it finds bugs outside Spark,
  but the counts are small.
- Pair two's spec shapes found no new C1 bug, although they now combine the NULL levels
  that A1 needed. Only known bugs (A1, N4, ORC-2226) came back.

## Pair three (2026-11-01)

**Design** (see the plan, "Pair three design"):

- **Spec:** `spec3_shapes.py`, 16 shapes from contract items not covered before:
  - UINT_8/16/32/64 values on both sides of the signed maximum;
  - bloom filters and dictionaries on unsigned columns read as a wider type;
  - all-NULL unsigned columns;
  - a directory with no data file;
  - files whose first row group has 0 rows, including nested columns;
  - map statistics over entries;
  - DECIMAL(18,2) sign order.
- **New for both methods:**
  - the unsigned types, which the random generator draws too;
  - GROUP BY and scalar-subquery templates;
  - the `all_error` kind;
  - integral decimals compared as integers.
- **Random:** seed 4, 57 queries per shape. Local check counts match (72,960 against
  73,240 on DuckDB and DataFusion).

| Run | Method | Checks | Findings | Engine core-hours | Runner-hours |
| --- | --- | --- | --- | --- | --- |
| 41326817833 | spec3 | 231,559 | 3,407 | 3.00 | 1.42 |
| 41326848563 | random seed 4 | 286,140 | 1,074 | 3.32 | 1.56 |

**Result: no new bug in either pass.** Every finding is classified below.

**Spec3 triage** (3,407 findings, 4,135 finding kinds; one finding can carry several):

| Cluster | Kinds | Class |
| --- | --- | --- |
| DuckDB and DataFusion refuse a directory with no file when they must infer the schema | 1,376 | by design |
| Trino reads UINT_32 values above 2³¹−1 as negative numbers (skipping off) | 805 | trinodb#31908 (filed 2026-10-30) |
| Trino "Corrupted statistics" on UINT_32 bounds that straddle the sign bit | 534 | trinodb#27274 (fix PR trinodb#31909 open) |
| Trino rejects UINT_64 declared as `decimal(20,0)` | 1,372 | feature gap (see below) |
| Trino bloom filter drops a UINT_32 row (BIGINT hash probing an INT32 filter) | 6 | trinodb#30544, also fixed by trinodb#31909 |
| Spark-written ORC, `IS NULL` on a field under a NULL struct | 42 | N4 = SPARK-60362 |

**Random seed 4 triage** (1,074 findings, 1,532 finding kinds):

| Cluster | Kinds | Class |
| --- | --- | --- |
| Trino Hive table creation rejects `timestamp(6)` ("configured precision is MILLISECONDS") | 912 | harness gap (see below) |
| Spark ORC pushdown NPE on an `IN` list containing NULL (all numeric types) | 420 | SPARK-59605 |
| Trino rejects UINT_64 as `decimal(20,0)` | 95 | feature gap |
| pyarrow ORC `struct<struct>` read wrong (Spark and Trino) | 16 | A1, known (ours) |
| Spark-written ORC nested `IS NULL` | 28 | N4 = SPARK-60362 |
| ORC "Seek … outside of the data" | 49 | ORC-2226 |
| "No min/max" / "Number of nulls not set" under aggregate pushdown | 12 | by design |

**Harness problems, recorded here and not fixed by rerunning:**

- **UINT_64 on Trino.** The plan assumed that Trino reads UINT_64 as `decimal(20,0)`, as
  Spark does. Trino builds a long-decimal reader only for a column with a DECIMAL
  annotation (`ColumnReaderFactory`), so every query fails. UINT_64 therefore has no
  Trino coverage in this pair, for either method. That is a missing feature, not a wrong
  answer, and it is not counted.
- **TIMESTAMP on Trino.** The Hive connector's default `timestamp_precision` is
  MILLISECONDS, so `CREATE TABLE` fails for every shape with a TIMESTAMP column. Spec3
  has no TIMESTAMP shape; random seed 4 has two. Pair two's Trino TIMESTAMP coverage is
  probably affected the same way. The fix is the session property
  `hive.timestamp_precision = 'MICROSECONDS'`, for the next run.
- **No-file directory on DuckDB and DataFusion.** This shape needs a declared schema on
  those engines. As run, only Spark and Trino tested it, and both were correct.

**What pair three says about the contracts:**

- The unsigned contract item (C3) is where Trino breaks, but someone had already reported
  every failure in October 2026, one of them two days before this run.
- Spark, DuckDB and DataFusion handled every unsigned shape, every 0-row row group, the
  directory with no data file, and the new GROUP BY and scalar-subquery templates without
  a difference.

**Cumulative matched comparison (pairs one to three):**

| Method | Engine core-hours | Checks | New bugs | Per core-hour | Outside Spark | Found only by this method |
| --- | --- | --- | --- | --- | --- | --- |
| Spec-driven | 18.47 | 1,633,510 | 4 (G, S, T3, TD) | 0.22 | 3 (S, T3, TD) | 3 (G, S, T3), plus GT |
| Random, same harness | 18.36 | 1,710,076 | 2 (A1, TD) | 0.11 | 2 (A1, TD) | 1 (A1) |

**Reading for the stop rule:**

- The ratio holds at two to one, but the last two pairs added one new bug between them
  (TD, found by both methods).
- Spec-driven yield has fallen pair by pair: three new bugs, then one, then none.
- Spec3's best region, Trino's unsigned reads, had already been reported by others. The
  shapes were good, but this ground had just been searched.

## Earlier cost estimate (superseded by the matched comparison above)

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
