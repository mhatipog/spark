# QueryProof bug hunt, rounds 2–4: two new Spark bugs (31 October 2026)

This document continues `2026-10-23-hunt-spark-4.2.0-findings.md`. All runs used
GitHub-hosted runners on the public fork. Every finding was reproduced in isolation
before it was classified.

## Method changes since round 1

- **Composed queries.** `gen_deep.py` builds nested derived tables (joins, aggregates,
  set operations, windows, LIMIT/OFFSET, subquery predicates). `gen_rich.py` adds
  DATE and DECIMAL(5,2) columns, a ternary-logic-partitioning (TLP) predicate per case,
  and `q_dpp` (a UNION ALL of partitioned and other scans joined to a filtered
  relation).
- **Real storage.** `run_meta.py` writes catalog Parquet tables: `ta` and `tb` are
  partitioned by `a` and `te` by `k` (NULL partitions included), `tc` is bucketed, and
  `td` is plain. It then runs ANALYZE … FOR ALL COLUMNS.
- **Oracles.** Every query also runs under configurations that must not change
  results:
  - AQE off;
  - join strategies;
  - Parquet pushdown and the vectorized reader;
  - schema pruning, DPP and bucketing;
  - CBO with join reordering;
  - partition counts and codegen;
  - in the "wide" set additionally aggressive DPP, runtime bloom filters, reuse off,
    forced skew handling, the partition evaluator, and subexpression elimination off.

  TLP, DuckDB 2.0.0 and SQLite serve as external references.
- **Confirmation tools.**
  - `repeat_case.py` / `meta_case.py` re-run single cases.
  - `patch_lqs.sh` applies master's SPARK-57956 fix to a disposable wheel.
  - Version matrices cover 3.5, 4.0, 4.1 and 4.2.1, plus apache/spark master built from
    source.

## New bugs (no JIRA issue or pull request found as of 31 October 2026)

### N1. Dynamic partition pruning through UNION fails with whole-stage codegen off

```sql
CREATE TABLE p (x INT, k INT) USING parquet PARTITIONED BY (k);
INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, NULL);
CREATE TABLE n (k INT, v INT) USING parquet;
INSERT INTO n VALUES (1, 10), (2, 20), (5, 50);
CREATE TABLE d (k INT) USING parquet;
INSERT INTO d VALUES (1), (2), (7);
SET spark.sql.codegen.wholeStage = false;
SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT k FROM n) u
JOIN d ON u.k = d.k WHERE d.k IN (1, 2);
-- java.lang.IllegalArgumentException: requirement failed:
--   input[0, int, true] IN dynamicpruning#64 has not finished
--   at InSubqueryExec.prepareResult(subquery.scala:149)
--   at DynamicPruningExpression.eval(DynamicPruning.scala:111)
--   at InterpretedPredicate.eval <- FilterEvaluatorFactory
```

- **Affected:** 3.5.9, 4.0.5, 4.1.4, 4.2.1, and master `f090eaca12` (2026-10-31).
- **Conditions:**
  - It fails with AQE on and off. Bucketing is irrelevant. Expression keys (`k + 1`)
    and UNION DISTINCT also fail.
  - Default codegen works, and so does turning DPP off.
  - A partitioned table without the UNION works.
- **Likely cause:**
  - Since SPARK-39217, DPP filters are pushed into every UNION branch. On a branch
    whose scan is not partitioned, the predicate stays in a `FilterExec` as
    `dynamicpruningexpression(k IN dynamicpruning#…)` over a `ReusedSubquery`.
  - `InSubqueryExec.updateResult` does not broadcast DPP results. They are "used at the
    driver only" (SPARK-52974; already true on the AQE path).
  - With codegen off the interpreted Filter evaluates the predicate on executors. There
    the `@transient result` is gone and `resultBroadcast` is null, so `prepareResult()`
    fails.
  - With whole-stage codegen the predicate is compiled on the driver after the result
    exists, which hides the problem.
- **Found by:** the `no_codegen` oracle on hunt cases R034_00754 and R034_00886,
  confirmed in 114 further cases.

### N2. CBO filter estimation throws NumberFormatException on a pruned partition range

```sql
CREATE TABLE p (b INT, c INT, a INT) USING parquet PARTITIONED BY (a);
INSERT INTO p VALUES (1, 1, 1), (-1, 3, -3);
CREATE TABLE t (k INT, v INT) USING parquet;   -- empty
ANALYZE TABLE p COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;
SET spark.sql.cbo.enabled = true;
SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0;
-- java.lang.NumberFormatException: Character N is neither a decimal digit number,
--   decimal point, nor "e" notation exponential mark.
--   at scala.math.BigDecimal$.double2bigDecimal
--   at FilterEstimation.evaluateBinaryForNumeric(FilterEstimation.scala:562)
```

- **Affected:** 3.5.9, 4.0.5, 4.1.4, 4.2.1, and master `f090eaca12`. On master
  `evaluateBinaryForNumeric` is identical to 4.2.1. EXPLAIN COST fails as well.
- **Conditions:**
  - It needs the partitioned table, column statistics and the join. The LEFT JOIN form
    also fails.
  - Either filter alone works, as does the range without the join, a non-empty range
    (`a < 2`), a plain (unpartitioned) table, and table-level ANALYZE only.
- **Likely cause:**
  - The filters `a < 1` and the inferred `a > 0` estimate a distinct count of 0 while
    min/max survive.
  - The boundary case then computes `percent = 1.0 - 1.0 / ndv = -Infinity`.
  - The update step's `ceil(ndv * percent)` is `0 × -Infinity = NaN`, and the
    `BigDecimal` conversion throws.
  - This is a sibling of SPARK-23799 (2018, `evaluateInSet`, fixed in 2.4.0).
- **Related observation (not filed):** after partition pruning, EXPLAIN COST estimates
  0 rows for `a < 2 AND a > 0` even though partition `a=1` exists.
- **Found by:** the `cbo_reorder` oracle on R036_00669 and R036_00928 (seed 20261036).

## Known issues reproduced (not new)

| Symptom | Configuration | Upstream |
| --- | --- | --- |
| Root `ORDER BY … LIMIT 2` returns 5 rows; `SELECT DISTINCT` returns duplicates | **default**, 4.2.1, deterministic | SPARK-57956. The fix is only in 4.3.0 (unreleased); patching master's `LogicalQueryStage.maxRows` into 4.2.1 removes both (0/40, 0/8) |
| `x IN (subquery)` returns FALSE instead of NULL outside top-level WHERE | default | SPARK-58442, -59862, -60026, -27572 (open) |
| Crash with a single excludable rule off | `excludedRules` | SPARK-59019, -59044 (fixed on master), SPARK-57125 (open) |
| DuckDB `EXISTS` over a filtered outer join onto an empty table returns false | default DuckDB 2.0.0 and nightly 2.1.0.dev2610300858 | duckdb/duckdb#27803, fix PR #27809 open (three hunt cases reduce to it) |

Not reproduced and not counted: R035_01486's TLP query failed once in the hunt and
passed 12 of 12 times in isolation.

## Volume

| Round | Queries × datasets | Spark configurations per run | Wrong-answer leads | New |
| --- | --- | --- | --- | --- |
| gen_deep, 4.2.1 (seed 20261033) | 12,000 × 2 | full / minimal / no-codegen / SQLite | 26 (all known) | 0 |
| gen_rich Parquet (seed 20261034) | 1,800 × 2 | 13 + TLP + DuckDB/SQLite | 1 (DuckDB #27803) | N1 |
| gen_rich Parquet (seed 20261035) | 6,000 × 2 | 13 + TLP + DuckDB/SQLite | 2 (DuckDB #27803) | — |
| gen_rich wide (seed 20261036) | 3,600 × 2 | 16 + TLP + DuckDB/SQLite | 1 (DuckDB #27803) | N2 |

## Round 5: newer features and default-configuration oracles (seed 20261037)

- **Generator.** `gen_feat.py` adds a table `tf(k, st STRUCT<x, y>, arr ARRAY<INT>, js)`
  partitioned by `k`, and a VARIANT table `tv` (`parse_json(js)`) written to Parquet.
  Queries reach them through flattening derived tables:
  - nested fields;
  - array and higher-order functions;
  - `LATERAL VIEW [OUTER] explode`;
  - `try_variant_get`;
  - a UTF8_LCASE-collated column.
- **New default-configuration oracles** (no second engine):
  - NoREC: `COUNT(*) … WHERE p` versus `SUM(CASE WHEN p …)`.
  - The filter and aggregates pushed into the query, versus applied over its result
    pinned as a fresh DataFrame.
- **Variants.** Nested-reader and pruning switches were added.
- **Volume:** 3,304 of 3,600 queries × 2 datasets. Shard 7 ran out of driver heap
  because `localCheckpoint` pins were never released; the harness was fixed.

Results:

- **Default-configuration oracles:** no differences in 3,304 queries. No default
  wrong answer was found.
- **N1 again:** 67 codegen-off failures, now also with array-function pruning keys
  (`CASE WHEN array_contains(…)`, `element_at`).
- **N2 again:** one case.
- **Not a user-facing bug (N3, not filed).** 699 queries over `tv` fail with
  `[CLASS_NOT_OVERRIDE_EXPECTED_METHOD] …parquet.ScalarCastHelper must override either
  eval or nullSafeEval`.
  - `variant_probe.py` shows this happens only with the internal test setting
    `spark.sql.codegen.factoryMode=NO_CODEGEN`.
  - `wholeStage=false` alone and the default FALLBACK mode work.
  - The code documents the assumption: "No need to define the interpreted version of
    `eval`: the codegen must succeed."
  - Consequence for the method: the `no_codegen` variant (which also sets factoryMode)
    over-reports. Only failures that also occur with `wholeStage=false` alone count as
    user-facing. N1 does.

## Round 6: user-facing switches only, V1/V2 path reads (seed 20261039)

- **Generator:** `gen_feat.py`, 3,600 queries × 2 datasets.
- **Variant set `round6` (19 configurations):**
  - user-facing switches only, so the internal factoryMode that caused N3 is dropped;
  - `wholeStage=false` alone and combined with AQE off, no broadcast, or DPP off;
  - the same files read by path through the V1 source (`@paths_v1`) and the V2 file
    source (`@paths_v2`, `spark.sql.sources.useV1SourceList=''`), with and without
    Parquet pushdown.
- **Run:** 41212403583.

Results:

1. **On released versions N1 also fails under the default configuration.**
   - When the non-partitioned UNION branch's key is a higher-order function
     (`aggregate(arr, 0, (acc, e) -> …)` or `transform`), the DPP predicate is evaluated
     per row even with whole-stage codegen on.
   - `dpp_default_probe.py` reproduces it with defaults and with AQE off on 3.5.9, 4.0.5,
     4.1.4 and 4.2.1 (run 41224020778). A plain `size(arr)` key works, and DPP off works.
   - On master (`d781c67c7c`) this default-configuration variant passes. Something on
     master no longer forces interpreted evaluation for these keys. The `wholeStage=false`
     reproduction still fails on master.
   - On master with the N1 fix, all nine combinations are correct (run 41219613198).
   - A regression test for the higher-order-function case was tried and dropped. Inside
     Spark's test suite (master) it passed without the fix (runs 41220521947, 41222836809),
     so it would not guard anything. The fix branch keeps the original, verified test only
     (2eb639a).
   - About 220 further N1 hits under `wholeStage=false`, including struct-field keys.
2. **A V2 file source plus VARIANT regression in 4.1/4.2, fixed on master (not new).**
   - 183 cases over the VARIANT table fail only through the V2 path read:
     - `[INTERNAL_ERROR] Invalid call to exprId on unresolved object` (160) in
       `V2ScanRelationPushDown.pruneColumns`;
     - `[INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND] Could not find v#N`, the symptom
       SPARK-57499 describes;
     - plan validation failures in `RemoveRedundantAliases`.
   - Minimal case: `SELECT k, CASE WHEN is_variant_null(v) THEN 1 ELSE 0 END FROM tv`
     through the V2 file source.
   - It fails on 4.1.4, 4.2.1 and branch-4.2 HEAD (`b7689f08`, 4.2.2-SNAPSHOT). It works
     on 4.0.5 and master (`d781c67c7c`). This is a regression from the 4.1 DSv2 variant
     pushdown (SPARK-53805/53880).
   - SPARK-57499 (fix 4.3.0) does not cherry-pick cleanly onto branch-4.2, so the fixing
     commit is not isolated.
   - Classification: fixed upstream, not backported. A comment draft is in the filing guide.
3. **N2:** 2 more hits.
4. **No wrong answers:** the default-configuration oracles (NoREC and pinned result)
   found no differences.
5. **One harness-level oddity:** a case whose default run hit N1 while other
   configurations succeeded. This is plan-dependent, the same bug.

## Fix branches (in mhatipog/spark, on top of master f090eaca12)

| Bug | Branch | Commit | Verification |
| --- | --- | --- | --- |
| N2 | `research/queryproof-fix-n2-filterestimation-ndv-zero` | ec22a0d | `FilterEstimationSuite` 87/87 passed (run 41200217583). The end-to-end reproduction now runs (it failed on unpatched master). Negative control: the new test without the fix fails, 86 passed and 1 failed (run 41202403612). Spark's own Build workflow on the branch is running. |
| N1 | `research/queryproof-fix-n1-dpp-union-nonpartitioned` | 2eb639a | `DynamicPartitionPruningV1/V2 SuiteAEOff/AEOn`: 183 passed, 0 failed, 5 ignored (run 41202482079). All six end-to-end shapes are correct under every configuration, including codegen off (run 41204571954). Negative control: the new test without the fix fails with "IN dynamicpruning#8480 has not finished", 49 passed and 1 failed in DynamicPartitionPruningV1SuiteAEOff (run 41205691383). |

- **The fixes.**
  - N2 returns selectivity 0 when `distinctCount == 0`, mirroring SPARK-23799's
    `evaluateInSet`.
  - N1 makes `CleanupDynamicPruningFilters` drop a DPP predicate over a scan when its
    key references only relation columns the scan cannot prune on (non-partition columns
    of file and Hive tables, non-runtime-filter attributes of DSv2 scans).
- **Commit message and PR body.** Each commit message carries the PR body in Spark's
  template form, with the JIRA number left as `SPARK-XXXXX`.
- **Not for upstream:** the branches `research/queryproof-fix-n{1,2}-test-only-control`
  are controls only.

## Drafts for upstream (to be posted by the account owner)

**JIRA: N1.** *Title:* "DPP filter pushed into a non-partitioned UNION branch fails
with 'IN dynamicpruning#N has not finished' when whole-stage codegen is disabled".
*Component:* SQL. *Affects:* 3.5.x, 4.0.x, 4.1.x, 4.2.1, master. *Body:* the N1
reproduction, the stack trace and the cause analysis above. Suggested directions:
- broadcast the result, or keep it on executors, when a DPP `InSubqueryExec` remains
  in a non-scan Filter; or
- have `CleanupDynamicPruningFilters` drop DPP predicates on non-partitioned branches.

**JIRA: N2.** *Title:* "CBO: FilterEstimation.evaluateBinaryForNumeric throws
NumberFormatException (NaN) when distinctCount is 0 but min/max are set".
*Component:* SQL / Optimizer. *Affects:* 3.5.x – master. *Body:* the N2 reproduction
and trace. Suggested fix: guard `ndv == 0` (return `Some(0.0)` or skip the update),
as SPARK-23799 did for `evaluateInSet`.

**Comment on SPARK-57956.** The issue is also reproducible deterministically on
4.2.1 with the default configuration:

```sql
-- l(a) = 1..5, r(k, s) = (3,'x'), (3,'y'), (3,'z'), (4,'w'), as DataFrame temp views
SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l
LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2   -- returns 5 rows
```

A DISTINCT variant returns duplicates. Applying commit 5623395102 to 4.2.1 fixes both,
so please consider backporting to branch-4.2.
