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
