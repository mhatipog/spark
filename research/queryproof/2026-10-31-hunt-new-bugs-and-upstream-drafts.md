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
