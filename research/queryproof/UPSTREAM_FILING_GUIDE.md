# Filing the QueryProof findings upstream (paste-ready)

Spark tracks bugs in **Apache JIRA**, project SPARK
(<https://issues.apache.org/jira/projects/SPARK>). Pull requests go to
`apache/spark` on GitHub and are titled `[SPARK-NNNNN][SQL] …`, referencing the JIRA.
GitHub Issues is enabled on apache/spark, but the contributing guide
(<https://spark.apache.org/contributing.html>) still points bug reports to JIRA, and
committers triage there.

**Correction (2026-11-01): do not file N4 or N5.** A full-text JIRA search during round 8
found that both were reported before we found them, and both already have open fix PRs
that take the same approach as our branches:

- **N4** is a duplicate of [SPARK-60362](https://issues.apache.org/jira/browse/SPARK-60362),
  filed 2026-10-19. The fix PR is [apache/spark#59747](https://github.com/apache/spark/pull/59747),
  opened 2026-10-23. It stops pushing IS NULL for a field under a nullable struct, and it
  adjusts the same OrcFilterSuite checks.
- **N5** is a duplicate of [SPARK-60487](https://issues.apache.org/jira/browse/SPARK-60487),
  filed 2026-10-22. The fix PR is [apache/spark#59763](https://github.com/apache/spark/pull/59763),
  opened 2026-10-24. It uses `Coalesce(…, 0)` and its test also checks the raw rows for
  NULL slots.

The round 7 novelty search (summary and keyword search) missed both. §2b and §2c below
are kept only as a record. Our fix branches for N4 and N5 add nothing upstream, so do not
open PRs from them.

**Suggested order:**
1. A1 (§2g): Arrow GitHub issue. pyarrow writes corrupt ORC files with nested structs; this is silent data corruption.
2. G (§2d): a wrong result under default settings, with a verified fix branch.
3. S (§2e): ORC JIRA; a wrong result in Spark under default settings.
4. T3 (§2f): Trino GitHub issue.
5. TD (§2h): Trino GitHub issue; a query failure, not a wrong result.
6. N1 (§1).
7. N2 (§2).
8. The two backport comments (§3, §3b), and optionally §3c.

Before filing each item, run the novelty search again. Several neighbouring bugs were
filed in October 2026.

## 0. One-time setup

1. **JIRA account.** Request one at <https://selfserve.apache.org/jira-account.html>.
   Anonymous tickets are not allowed, and the request is reviewed, which can take a day
   or two. You do not need an account to read issues.
2. **For pull requests:** your GitHub fork `mhatipog/spark` must have GitHub Actions
   enabled. Spark's PR checks run on the contributor's fork, and the PR waits for a
   successful run there.
3. **Generative AI disclosure.** Spark's PR template asks: "Was this patch authored or
   co-authored using generative AI tooling?" Answer **Yes**, naming the tool, per the
   ASF generative-tooling guidance (<https://www.apache.org/legal/generative-tooling.html>).
   Mention in the JIRA that the bugs were found by automated differential testing.

Order: file N1 and N2 (§1, §2), and comment on SPARK-57956 (§3). Once a JIRA number
exists, open the fix PRs (§4).

---

## 1. JIRA: N1 (DPP through UNION, codegen off)

- **Project:** Spark · **Issue type:** Bug · **Priority:** Major
- **Component/s:** SQL
- **Affects Version/s:** 3.5.0, 4.0.0, 4.1.0, 4.2.1, 5.0.0. Pick the listed versions
  closest to the tested 3.5.9 / 4.0.5 / 4.1.4 / 4.2.1 / master.
- **Labels:** (none needed)
- **Summary:**
  `Dynamic partition pruning filter pushed into a non-partitioned UNION branch fails with "IN dynamicpruning#N has not finished" when whole-stage codegen is disabled`
- **Description** (JIRA wiki markup, paste as is):

```
When whole-stage codegen is disabled, a query that joins a filtered relation to a UNION ALL of a partitioned and a non-partitioned table fails at execution time. With codegen enabled (the default) the same query succeeds.

h3. Reproduction (spark-sql / spark.sql)
{code:sql}
CREATE TABLE p (x INT, k INT) USING parquet PARTITIONED BY (k);
INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, NULL);
CREATE TABLE n (k INT, v INT) USING parquet;
INSERT INTO n VALUES (1, 10), (2, 20), (5, 50);
CREATE TABLE d (k INT) USING parquet;
INSERT INTO d VALUES (1), (2), (7);

SET spark.sql.codegen.wholeStage=false;
SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT k FROM n) u
JOIN d ON u.k = d.k WHERE d.k IN (1, 2);
{code}

h3. Actual
{noformat}
java.lang.IllegalArgumentException: requirement failed: input[0, int, true] IN dynamicpruning#64 has not finished
  at scala.Predef$.require(Predef.scala:337)
  at org.apache.spark.sql.execution.InSubqueryExec.prepareResult(subquery.scala:149)
  at org.apache.spark.sql.execution.InSubqueryExec.eval(subquery.scala:156)
  at org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression.eval(DynamicPruning.scala:111)
  at org.apache.spark.sql.catalyst.expressions.And.eval(predicates.scala:865)
  at org.apache.spark.sql.catalyst.expressions.InterpretedPredicate.eval(predicates.scala:57)
  at org.apache.spark.sql.execution.FilterEvaluatorFactory$FilterPartitionEvaluator...
{noformat}

h3. Expected
The same rows as with codegen enabled: 1, 1, 2, 2.

h3. Also with the default configuration on released versions
On 3.5.9, 4.0.5, 4.1.4 and 4.2.1, when the key of the non-partitioned branch is a higher-order function, the DPP predicate is evaluated per row even with whole-stage codegen enabled, so the query fails with default settings. On master (d781c67c7c) this default-configuration variant passes, but the spark.sql.codegen.wholeStage=false reproduction above still fails:
{code:sql}
CREATE TABLE n2 (arr ARRAY<INT>) USING parquet;
INSERT INTO n2 VALUES (array(1)), (array(1, 1)), (array(2, 3)), (NULL);
SELECT u.k FROM (SELECT k FROM p UNION ALL
                 SELECT aggregate(arr, 0, (acc, e) -> acc + e) AS k FROM n2) u
JOIN d ON u.k = d.k WHERE d.k IN (1, 2);
-- requirement failed: aggregate(input[0, array<int>, true], 0, lambdafunction(...)) IN dynamicpruning#N has not finished
{code}

h3. Scope
* Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (f090eaca12, 2026-10-31).
* Fails with AQE on and off, and with factoryMode=NO_CODEGEN as well as with only spark.sql.codegen.wholeStage=false.
* Also fails with an expression on the join key (SELECT k + 1 ... UNION ALL ...) and with UNION DISTINCT.
* Does not fail with spark.sql.optimizer.dynamicPartitionPruning.enabled=false, or when the UNION is removed (partitioned table joined directly).

h3. Analysis
Since SPARK-39217 the DPP filter is pushed into every branch of the UNION. On the branch whose scan is not partitioned the predicate cannot become a partition filter, so it remains in a FilterExec:
{noformat}
Filter (... AND dynamicpruningexpression(k IN dynamicpruning#N))
:  +- ReusedSubquery SubqueryBroadcast dynamicpruning#N ...
+- FileScan parquet n[k] ... PartitionFilters: []
{noformat}
For DPP, InSubqueryExec.updateResult() does not broadcast its result (the results are "used at the driver only", see SPARK-52974; this was already the case on the AQE path). With whole-stage codegen the predicate is compiled on the driver after the subquery has finished, so it works. Without codegen, the interpreted FilterExec evaluates the predicate in tasks: the @transient result is gone after serialization and resultBroadcast is null, so prepareResult() fails.

Possible fixes: do not leave DPP predicates in non-scan filters (drop them for branches whose relation is not partitioned on the pruning key, e.g. in CleanupDynamicPruningFilters), or broadcast the result when a DPP InSubqueryExec is evaluated outside a scan.

Found by automated differential testing (configuration-switch oracle over partitioned Parquet tables).
```

---

## 2. JIRA: N2 (CBO FilterEstimation NaN)

- **Project:** Spark · **Issue type:** Bug · **Priority:** Major
- **Component/s:** SQL, Optimizer
- **Affects Version/s:** 3.5.0, 4.0.0, 4.1.0, 4.2.1, 5.0.0
- **Summary:**
  `CBO: FilterEstimation.evaluateBinaryForNumeric throws NumberFormatException when the estimated distinct count is 0`
- **Description:**

```
With spark.sql.cbo.enabled=true and column statistics, planning a join between a partitioned table and an empty table fails with java.lang.NumberFormatException thrown from filter estimation. EXPLAIN COST fails the same way.

h3. Reproduction
{code:sql}
CREATE TABLE p (b INT, c INT, a INT) USING parquet PARTITIONED BY (a);
INSERT INTO p VALUES (1, 1, 1), (-1, 3, -3);
CREATE TABLE t (k INT, v INT) USING parquet;   -- left empty
ANALYZE TABLE p COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;

SET spark.sql.cbo.enabled=true;
SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0;
{code}

h3. Actual
{noformat}
java.lang.NumberFormatException: Character N is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
  at java.base/java.math.BigDecimal.<init>(BigDecimal.java:586)
  at scala.math.BigDecimal$.double2bigDecimal(BigDecimal.scala:304)
  at org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation.evaluateBinaryForNumeric(FilterEstimation.scala:562)
  at ...FilterEstimation.evaluateBinary(FilterEstimation.scala:295)
  at ...FilterEstimation.calculateSingleCondition(FilterEstimation.scala:170)
  at ...FilterEstimation.calculateFilterSelectivity(FilterEstimation.scala:130)
  at ...FilterEstimation.estimate(FilterEstimation.scala:51)
  at ...BasicStatsPlanVisitor$.visitFilter(BasicStatsPlanVisitor.scala:56)
{noformat}

h3. Expected
An empty result (as with spark.sql.cbo.enabled=false).

h3. Scope
* Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (f090eaca12); evaluateBinaryForNumeric is unchanged on master.
* Needs the partitioned table, column-level statistics and the join (LEFT JOIN fails too). Each filter alone, the range without the join, a non-empty range (p.a < 2), an unpartitioned p, or table-level ANALYZE only all work.

h3. Analysis
The predicates p.a < 1 and the inferred p.a > 0 leave a column stat whose distinctCount is 0 while min/max are still set. In evaluateBinaryForNumeric the boundary case (literal == max for LessThan, etc.) computes percent = 1.0 - 1.0 / ndv = -Infinity; the update then computes ceil(ndv * percent) = ceil(0 * -Infinity) = NaN, and the conversion to BigDecimal throws. SPARK-23799 fixed the same division-by-zero in evaluateInSet; evaluateBinaryForNumeric needs a similar guard for ndv == 0 (e.g. treat the predicate as selecting nothing).

Found by automated differential testing (CBO on/off oracle over partitioned Parquet tables).
```

---

## 2b. JIRA: N4 (ORC: IS NULL on a field of a NULL struct drops rows) — DO NOT FILE (duplicate of SPARK-60362)

This is a silent wrong result under the default configuration, so add the label
`correctness` (Spark's convention for wrong-result bugs).

- **Project:** Spark · **Issue type:** Bug · **Priority:** Critical (wrong result)
- **Labels:** correctness
- **Component/s:** SQL
- **Affects Version/s:** 3.5.0, 4.0.0, 4.1.0, 4.2.1, 5.0.0
- **Summary:**
  `ORC filter pushdown of IS NULL on a struct field skips rows where the struct itself is NULL`
- **Description:**

```
With the default configuration (spark.sql.orc.filterPushdown=true), a filter `st.x IS NULL` on an ORC file returns no rows for the rows where the whole struct `st` is NULL, although `st.x` is NULL there.

h3. Reproduction (PySpark)
{code:python}
df = spark.createDataFrame([(1, (1, "b")), (2, None)], "k INT, st STRUCT<x: INT, y: STRING>")
df.coalesce(1).write.mode("overwrite").orc("/tmp/orc_null_struct")
spark.read.orc("/tmp/orc_null_struct").where("st.x IS NULL").show()
# returns no rows; expected: (2, NULL)
spark.conf.set("spark.sql.orc.filterPushdown", "false")
spark.read.orc("/tmp/orc_null_struct").where("st.x IS NULL").show()
# returns (2, NULL)
{code}

h3. Scope
* Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (81654b27d2, 5.0.0-SNAPSHOT), with the V1 and the V2 ORC source and with the vectorized and the row-based reader. Parquet is not affected.
* `st.y IS NULL`, `NOT (st.x IS NOT NULL)` and `st.x IS NULL OR ...` are affected the same way. Setting spark.sql.optimizer.nestedPredicatePushdownSupportedFileSources to exclude orc did not stop the pushdown in our tests: the scan still reports PushedFilters: [IsNull(st.x)].
* Whether a query is affected depends on the file layout: a stripe or row group is skipped when every NULL of st.x in it comes from a NULL st. In a partitioned table this happens per partition.

h3. Analysis
The writer's file statistics for the example (ORC reader getStatistics()):
{noformat}
column 2 (st):   count: 1 hasNull: true
column 3 (st.x): count: 1 hasNull: false min: 1 max: 1
column 4 (st.y): count: 1 hasNull: false min: b max: b
{noformat}
ORC keeps a child column's statistics only for the rows where the parent struct is present, so st.x records hasNull = false. OrcFilters.buildLeafSearchArgument pushes IsNull("st.x") as an IS_NULL leaf on the nested column, ORC evaluates it against those statistics as NO and skips the row group. Spark re-applies the filter after the scan, but the skipped rows never reach it.

A Spark-side fix is to not push IsNull for a field nested in a nullable struct (IsNotNull and the value comparisons are safe, because they reject these rows anyway). A fix branch with tests is available. ORC itself could also take the parent's hasNull into account when it evaluates a nested IS_NULL leaf.

Found by automated metamorphic testing (the same rows read as Parquet and as ORC must give the same result).
```

---

## 2c. JIRA: N5 (aggregate pushdown: COUNT over no file returns NULL) — DO NOT FILE (duplicate of SPARK-60487)

- **Project:** Spark · **Issue type:** Bug · **Priority:** Major
- **Labels:** correctness
- **Component/s:** SQL
- **Affects Version/s:** 3.5.0, 4.0.0, 4.1.0, 4.2.1, 5.0.0
- **Summary:**
  `Aggregate push down: global COUNT returns NULL instead of 0 when the scan reads no file`
- **Description:**

```
With spark.sql.parquet.aggregatePushdown=true (or spark.sql.orc.aggregatePushdown=true) and the DSv2 file source, a global COUNT over a scan that reads no file returns NULL instead of 0. MIN/MAX correctly return NULL.

h3. Reproduction (PySpark)
{code:python}
spark.conf.set("spark.sql.sources.useV1SourceList", "")
spark.range(10).selectExpr("id", "id % 3 AS p").write.partitionBy("p").parquet("/tmp/agg_count")
spark.read.parquet("/tmp/agg_count").createOrReplaceTempView("t")
spark.conf.set("spark.sql.parquet.aggregatePushdown", "true")
spark.sql("SELECT count(*), count(id) FROM t WHERE p = 5").show()
# returns (NULL, NULL); expected (0, 0), as with aggregatePushdown=false
{code}

h3. Scope
* Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (81654b27d2, 5.0.0-SNAPSHOT).
* Parquet: a partition filter that prunes every file, an empty partitioned table, and an empty non-partitioned table written from an empty DataFrame. ORC: the first two (an empty ORC file carries a row count of 0).
* It also changes results that depend on the count, e.g. `WHERE (SELECT count(b) FROM empty_t) < 2` keeps no rows.
* Note for reproducing in Scala: Dataset.collect() shows 0, because the count column is declared not nullable and the row encoder then reads the null slot as 0. PySpark, scalar subqueries and predicates over the count see the NULL; df.queryExecution.executedPlan.executeCollect().head.isNullAt(0) is true.

h3. Analysis
For file sources the aggregate is pushed down partially: each file returns its count and V2ScanRelationPushDown rewrites COUNT into SUM over these partial counts. When no file is read the scan returns no row, and SUM over no rows is NULL, in a column declared not nullable. Coalescing the sum of the pushed down counts with 0 for global aggregates fixes it (grouped aggregates have no group without rows). A fix branch with a test is available.

Found by automated metamorphic testing (aggregate push down on/off must give the same result).
```

---

## 3. Comment on SPARK-57956 (backport request)

Open <https://issues.apache.org/jira/browse/SPARK-57956> and add a comment:

```
This also reproduces deterministically on 4.2.1 with the default configuration (AQE on), which is not listed in Affects Version/s:

{code:python}
spark.createDataFrame([(i,) for i in range(1, 6)], "a int").createOrReplaceTempView("l")
spark.createDataFrame([(3, "x"), (3, "y"), (3, "z"), (4, "w")], "k int, s string").createOrReplaceTempView("r")
spark.sql("""
  SELECT l.a, r.s FROM (SELECT a FROM l ORDER BY a LIMIT 3 OFFSET 2) l
  LEFT JOIN r ON l.a = r.k ORDER BY l.a, r.s LIMIT 2""").collect()
# returns 5 rows (10/10 runs); AQE off returns the expected 2 rows: (3, 'x'), (3, 'y')
{code}

The same root cause also drops a DISTINCT: AQE's OptimizeOneRowPlan removes the Aggregate when a non-materialized LogicalQueryStage reports an estimated rowCount <= 1, returning duplicate rows (seen with SELECT DISTINCT over a derived table with ORDER BY ... LIMIT 4 OFFSET 3).

Applying 5623395102 (the LogicalQueryStage.maxRows change) to 4.2.1 fixes both (0/40 and 0/8 wrong vs 10/10 and 8/8 without it). Could this be backported to branch-4.2 (and branch-4.1 if still maintained)?
```

Set nothing else. Committers decide the backport.

---

## 3b. Comment on SPARK-57499 (V2 VARIANT regression, backport request)

Open <https://issues.apache.org/jira/browse/SPARK-57499> and add:

```
A related failure in the same area still reproduces on 4.1.4, 4.2.1 and branch-4.2 (4.2.2-SNAPSHOT, b7689f08), while master and 4.0.5 are fine. With the V2 file source a projection using is_variant_null fails during column pruning:

{code:python}
spark.sql("""SELECT * FROM VALUES (1, parse_json('{"a": 1, "b": "x"}')), (2, parse_json('{"a": -2}')),
             (0, parse_json('null')), (3, parse_json('{"b": "y"}')) AS t(k, v)""").write.parquet(path)
spark.conf.set("spark.sql.sources.useV1SourceList", "")
spark.read.parquet(path).createOrReplaceTempView("tv")
spark.sql("SELECT k, CASE WHEN is_variant_null(v) THEN 1 ELSE 0 END FROM tv").collect()
{code}
{noformat}
org.apache.spark.sql.catalyst.analysis.UnresolvedException: [INTERNAL_ERROR] Invalid call to exprId on unresolved object
  at org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.exprId(unresolved.scala:310)
  at org.apache.spark.sql.catalyst.expressions.AttributeMap$.apply(AttributeMap.scala:35)
  at org.apache.spark.sql.catalyst.expressions.AliasHelper.getAliasMap(AliasHelper.scala:52)
  at org.apache.spark.sql.catalyst.planning.ScanOperation$.collectProjectsAndFilters(patterns.scala:65)
  at org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$$anonfun$pruneColumns$1.applyOrElse(V2ScanRelationPushDown.scala:800)
{noformat}
The same happens for EXISTS / IN subqueries over such a projection; try_variant_get alone works, and the V1 source works. In randomized testing the V2 path also hit "[INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND] Could not find v#N" (the symptom described here) and PLAN_VALIDATION_FAILED_RULE_IN_BATCH in RemoveRedundantAliases. The fix commit bd65ed7ce1 does not cherry-pick cleanly onto branch-4.2, so I could not confirm which master change fixes the is_variant_null case. Could the fix(es) be backported to branch-4.2 / branch-4.1?
```

---

## 2d. JIRA: G (filter pushdown with a DATE or TIMESTAMP in the 1582 calendar gap). Found by the interface hunt.

*Updated after pair two (2026-11-01): the same conversion shifts TIMESTAMP literals on
those days, for Parquet. Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (runs
41308537516 and 41309355229). One JIRA covers both; the fix branch now handles both.
ORC TIMESTAMPs before 1582-10-15 are a separate, known issue (SPARK-60152); leave them out.*

- **Project:** Spark · **Issue type:** Bug · **Priority:** Major
- **Labels:** correctness
- **Component/s:** SQL
- **Affects Version/s:** 3.5.0, 4.0.0, 4.1.0, 4.2.1, 5.0.0 (reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master 4fecd73)
- **Summary:**
  `Filter pushdown with a DATE or TIMESTAMP between 1582-10-05 and 1582-10-14 skips matching rows in Parquet and ORC`
- **Description:**

```
DATE values from 1582-10-05 to 1582-10-14 are valid in Spark's proleptic Gregorian calendar, but do not exist in the hybrid Julian/Gregorian calendar of java.sql.Date. When such a literal is pushed down to Parquet or ORC it becomes 1582-10-15, and row groups holding 1582-10-15 are skipped. This happens with the default configuration.

h3. Reproduction (PySpark)
{code:python}
import datetime
for d in ["1582-10-04", "1582-10-15", "1582-10-16"]:   # one file per value
    spark.createDataFrame([(datetime.date.fromisoformat(d),)], "d DATE").coalesce(1) \
        .write.mode("append").parquet("/tmp/gap_dates")
spark.read.parquet("/tmp/gap_dates").where("d > DATE'1582-10-10'").show()
# returns 1582-10-16 only; expected 1582-10-15 and 1582-10-16
spark.read.parquet("/tmp/gap_dates").where("d <> DATE'1582-10-10'").count()
# returns 2; expected 3
{code}
EXPLAIN shows the shifted literal: PushedFilters: [IsNotNull(d), GreaterThan(d,1582-10-15)]. With spark.sql.parquet.filterPushdown=false both queries are correct. ORC (spark.sql.orc.filterPushdown) behaves the same. With spark.sql.datetime.java8API.enabled=true Parquet is correct, but ORC still is not.

TIMESTAMP literals on those days are shifted the same way when pushed to Parquet (TIMESTAMP_MICROS columns, e.g. files written by other tools, or by Spark with spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS):
{code:python}
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
for t in ["1582-10-04 12:00:00", "1582-10-10 12:00:00", "1582-10-15 00:00:00"]:   # one file per value
    spark.sql(f"SELECT TIMESTAMP'{t}' AS t").coalesce(1).write.mode("append").parquet("/tmp/gap_ts")
spark.read.parquet("/tmp/gap_ts").where("t = TIMESTAMP'1582-10-10 12:00:00'").count()
# returns 0; expected 1. PushedFilters: [IsNotNull(t), EqualTo(t,1582-10-15 12:00:00.0)]
{code}
Here equality is wrong too, because the stored value is the gap value itself.

h3. Analysis
* DataSourceStrategy.translateLeafNodeFilter converts the literal with CatalystTypeConverters.convertToScala, which gives a java.sql.Date unless the Java 8 time API is enabled. A day of the gap has no java.sql.Date, so the Filter carries 1582-10-15. ParquetFilters and OrcFilters then push 1582-10-15.
* OrcFilters.castLiteralValue also converts a LocalDate to java.sql.Date (toJavaDate), because ORC's DATE literal is java.sql.Date. So ORC is affected with the Java 8 API too.
* Comparisons whose truth changes between the gap date and 1582-10-15 give wrong results: >, <> and NOT <=>. For the others, pushing 1582-10-15 only skips less.

* For TIMESTAMP, convertToScala gives a java.sql.Timestamp (DateTimeUtils.toJavaTimestamp rebases to the hybrid calendar in the JVM time zone), and a time on a gap day becomes the same time ten days later.

A fix is to not push down a predicate with a DATE or TIMESTAMP literal in the gap: in DataSourceStrategy when the Filter would carry java.sql.Date or java.sql.Timestamp (for TIMESTAMP, the gap widened by a day on each side in UTC, to cover every JVM time zone), and in OrcFilters for LocalDate. A fix branch with tests is available.

Found by spec-driven testing of storage interfaces: the same files and predicates with filter pushdown on and off.
```

---

## 2e. ORC JIRA (not Spark): string predicates compare in UTF-16 order, statistics are in UTF-8 order. Found by the interface hunt.

File this at <https://issues.apache.org/jira/projects/ORC> (it uses the same JIRA account).
Search once more right before filing, with `project = ORC AND text ~ "UTF-16"` and
`text ~ "surrogate"`.

- **Project:** ORC · **Issue type:** Bug · **Priority:** Major
- **Component/s:** Java
- **Affects Version/s:** 2.1.4, 2.2.2, 2.3.0, and the ORC version of Spark 3.5.9 (seen through Spark 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master)
- **Summary:**
  `Predicate pushdown compares strings in UTF-16 order against UTF-8 ordered statistics and skips matching row groups`
- **Description:**

```
The writer keeps string minimum/maximum in UTF-8 byte order (StringStatisticsImpl updates them with Text.compareTo). The reader's row-group and stripe pruning compares a STRING predicate with them as java.lang.String (RecordReaderImpl.getBaseObjectForComparison returns obj.toString(); ValueRange then uses String.compareTo), i.e. in UTF-16 code-unit order. The two orders differ for characters above U+FFFF against characters from U+E000 to U+FFFF: in UTF-8, U+1F600 sorts after U+E000; in UTF-16, its high surrogate 0xD83D sorts before 0xE000.

So a row group whose values are all above U+FFFF is skipped for predicates it satisfies.

h3. Reproduction (through Spark 4.2.1, ORC 2.3.0; one file per row)
{code:python}
for k, s in [(1, "\U0001F600"), (2, ""), (3, "￿"), (4, "\U00010000"), (5, "z")]:
    spark.createDataFrame([(k, s)], "k INT, s STRING").coalesce(1).write.mode("append").orc("/tmp/orc_astral")
spark.read.orc("/tmp/orc_astral").where("s > ''").select("k").show()
# returns 3; expected 1, 3, 4 (as with spark.sql.orc.filterPushdown=false)
spark.read.orc("/tmp/orc_astral").where("s >= '￿'").select("k").show()
# returns 3; expected 1, 3, 4
{code}
The ORC Java API reports the expected statistics, e.g. min = max = "😀" for the file of row 1. Files written by the C++ writer (pyarrow 26) are pruned the same way, so the reader side is at fault. By the same reasoning, equality and IN should also be affected when a row group's [min, max] contains the literal in UTF-8 order but not in UTF-16 order. That case was not tested: these files hold one value each.

A fix is to compare STRING predicate literals and statistics as UTF-8 bytes in getBaseObjectForComparison / ValueRange, e.g. as org.apache.hadoop.io.Text.

Found by spec-driven testing of storage interfaces: the same files and predicates with filter pushdown on and off.
```

Spark could also add a workaround: do not push a string comparison whose literal has a
character at or above U+D800. For such a literal the two orders cannot disagree. Leave
this to the ORC fix unless committers ask.

---

## 2f. Trino GitHub issue: T3 (ORC `IS NULL` two struct levels deep). Found by the interface hunt.

Trino tracks bugs as GitHub issues: <https://github.com/trinodb/trino/issues/new/choose>
(the "Bug report" template). Search the issues for "ORC" "IS NULL" "row" once more
before filing.

- **Title:** `Hive ORC: IS NULL on a field of a nested ROW skips rows where the middle ROW is NULL`
- **Body:**

````
**Version:** Trino 484 (Docker image trinodb/trino:484), Hive connector, file metastore, local file system.

**What happens.** For a column `o ROW(i ROW(c INTEGER), z INTEGER)` in an ORC file, `WHERE o.i.c IS NULL` does not return a row where `o` is present and `o.i` is NULL, although `o.i.c` is NULL there. `WHERE o.i IS NULL` returns it. A NULL `o` itself is handled correctly, and Parquet files with the same rows are correct.

**Reproduction** (files written by pyarrow 26; one stripe each):
```python
import pyarrow as pa, pyarrow.orc as orc
T = pa.struct([("i", pa.struct([("c", pa.int32())])), ("z", pa.int32())])
orc.write_table(pa.table({"k": pa.array([2, 4], pa.int32()),
                          "o": pa.array([{"i": None, "z": 1}, {"i": {"c": 7}, "z": None}], T)}),
                "/data/t/part-0.orc")
```
```sql
CREATE TABLE hive.qp.t (k INTEGER, o ROW(i ROW(c INTEGER), z INTEGER))
  WITH (external_location = 'local:///data/t', format = 'ORC');
SELECT k FROM hive.qp.t WHERE o.i.c IS NULL;   -- returns no row; expected 2
SELECT k FROM hive.qp.t WHERE o.i IS NULL;     -- returns 2
```

**Which layouts are affected:**

| Rows in the stripe | `o.i.c IS NULL` |
|---|---|
| only `o.i` NULL | wrong (no row) |
| `o.i` NULL and a row with `o.i.c = 7` | wrong (no row) |
| `o.i` NULL and a row with `o.i.c` NULL | correct |
| `o` NULL (top-level) | correct |

The result depends on whether some row of the stripe has an explicit NULL in `c`. So it looks like statistics-based skipping. ORC keeps a child column's statistics only for rows where its parent is present, so `c` has `hasNull = false` when every NULL of `o.i.c` comes from a NULL `o.i`. The plan pushes a constraint on the dereferenced field: `ScanFilterProject[... constraint on [o], filterPredicate = (o_i_c IS NULL)]`.

The top-level case being correct suggests that the reader accounts for a NULL top-level column but not for an intermediate ROW. (Spark had the same problem at one level: SPARK-60362.)

Found by spec-driven testing of storage interfaces: the same files and predicates across engines, with skipping on and off.
````

---

## 2g. Arrow GitHub issue: A1 (the ORC writer corrupts a struct nested in a struct). Found by the random baseline.

Arrow tracks bugs as GitHub issues: <https://github.com/apache/arrow/issues/new/choose>
("Bug report"; components C++ and Python). Search for "ORC struct" once more before filing.

- **Title:** `[C++][Python][ORC] Writing a struct inside a struct shifts child values when only the inner struct has nulls`
- **Body:**

````
**Describe the bug**

Writing a column `struct<i: struct<c: int32>>` to ORC, where the outer struct has no nulls and the inner struct has a null, writes wrong values: every child value after the null row is shifted by one, and the first shifted value becomes 0 (or "" for strings). pyarrow's own reader returns the wrong values, and so does the ORC Java reader (Spark 3.5.9–4.2.1). So the file content is wrong, not one reader.

```python
import pyarrow as pa, pyarrow.orc as orc
I = pa.struct([("c", pa.int32())])
t = pa.table({"o": pa.array([{"i": {"c": 1}}, {"i": None}, {"i": {"c": 3}}], pa.struct([("i", I)]))})
orc.write_table(t, "/tmp/a.orc")
print(orc.read_table("/tmp/a.orc").to_pylist())
# [{'o': {'i': {'c': 1}}}, {'o': {'i': None}}, {'o': {'i': {'c': 0}}}]   <- expected c = 3
```

With five rows (c = 95, 8, <i null>, 61, 41) the values read are 95, 8, <null>, 0, 61: the 41 is lost.

**Scope:**
* Affected: pyarrow 12.0.1, 15.0.2, 18.1.0, 21.0.0, 24.0.0 and 26.0.0 (Linux; 26.0.0 also on Windows).
* The same happens with a string child (`""` instead of the value).
* Not affected:
  * if the outer struct also has a null anywhere in the batch;
  * a single-level struct with a null;
  * a list of structs with a null element;
  * a struct containing a list with a null list.

So it looks like the null mask of the inner struct is not applied to its children when the outer struct has no nulls (`hasNulls` is false). The child then gets a value written for the row where its parent is null, and every reader takes one value too many from the child's data stream. The struct path is `WriteStructBatch` in `cpp/src/arrow/adapters/orc/util.cc`, together with how ORC C++'s struct writer passes the incoming null mask to its children.

**Component(s):** C++, Python

Found by randomized cross-engine testing of storage formats (the same rows read back by several engines).
````

---

## 2h. Trino GitHub issue: TD (DELTA_BINARY_PACKED TINYINT/SMALLINT fails with "byte overflow"). Found by the interface hunt, both methods.

Found in pair two by the spec-driven shapes (run 41302131902) and, independently, by the
random baseline (run 41302164093). Confirmed on Trino 476, 480 and 484 (probe run
41310624280); 458 and 466 did not start with the test catalog, so they are untested.
The code is unchanged on Trino master. Search the issues for "byte overflow" and
"DELTA_BINARY_PACKED" once more before filing (nothing on 2026-11-01).

- **Title:** `Parquet: reading a DELTA_BINARY_PACKED TINYINT or SMALLINT column fails with "byte overflow" / "short overflow"`
- **Body:**

````
**Version:** Trino 476, 480 and 484 (Docker images), Hive connector, default session properties.

**What happens.** A query that reads a valid Parquet file fails with `GENERIC_INTERNAL_ERROR: byte overflow` (TINYINT) or `short overflow` (SMALLINT). The column is INT32 with logical type INT(8) or INT(16), encoded as DELTA_BINARY_PACKED (for example by parquet-mr with `parquet.writer.version=PARQUET_2_0` and dictionary encoding off, or by pyarrow with `column_encoding`). pyarrow, DuckDB, DataFusion and Spark read the same files correctly.

**Reproduction:**
```python
import pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pa.table({"b": pa.array([-128, 127], pa.int8())}), "/data/t/part-0.parquet",
               use_dictionary=False, column_encoding={"b": "DELTA_BINARY_PACKED"}, data_page_version="2.0")
```
```sql
CREATE TABLE hive.qp.t (b TINYINT) WITH (external_location = 'local:///data/t', format = 'PARQUET');
SELECT count(*), sum(b) FROM hive.qp.t;   -- fails: byte overflow; expected 2, -1
```
The same with `pa.int16()`, values `[-32768, 32767]` and a SMALLINT column fails with `short overflow`.

**Which data is affected:**

| Values in the page | Result |
|---|---|
| `[-128, 127]` or `[127, -128]` | fails |
| 33 × `127`, then `-128` (the last miniblock holds one delta) | fails |
| `0, 1, …, 99` | correct |
| 5,000 and 200,000 uniform random values, several page sizes | correct |

**Cause.** `DeltaPackingUtils.unpackDelta(byte[] …)` calls `unpackEmpty(…, toByteExact(minDelta))` when a miniblock has bit width 0, and the `short[]` variant calls `toShortExact(minDelta)`. In DELTA_BINARY_PACKED the deltas of an INT32 column are int32 values: going from -128 to 127 is a delta of 255, and only the sum wraps back into the logical range. When a miniblock's deltas all equal the block's minimum delta (bit width 0), and that delta is outside the byte or short range, the exact cast throws. The bit-width > 0 path (`unpackByte`, `unpackShort`) has no exact cast, and uniform random data, which almost never produces a bit-width-0 miniblock, reads correctly. A fix is to narrow without the check, e.g. `(byte) minDelta` and `(short) minDelta`, as the other path effectively does.

Found by spec-driven testing of storage interfaces: the same files and predicates across engines, with skipping on and off.
````

---

## 3c. Optional comment on SPARK-59607 (affected versions)

SPARK-59607 lists only 5.0.0 and says "Parquet". Our probe (`partcount_probe.py`, run
41284727455) shows the bug on every released line, for ORC too. Add this only if it helps:

```
This also reproduces on 3.5.x, 4.0.x, 4.1.x and 4.2.1, and for ORC with spark.sql.orc.aggregatePushdown=true (DSv2 file source). Partitioned by a with rows (1,10),(1,NULL),(NULL,20),(NULL,NULL),(NULL,30),(2,40):
  SELECT COUNT(a) FROM t                      -> 6 (expected 3)
  SELECT a, COUNT(a) FROM t GROUP BY a        -> the NULL group has COUNT(a) = 3 (expected 0)
  SELECT COUNT(a) FROM t WHERE a IS NULL      -> 3 (expected 0)
Both ParquetUtils.getPushedDownAggResult and OrcUtils add the file's row count for a partition column without looking at the file's partition value.
```

---

## 4. Pull requests (after the JIRAs exist)

Fix branches are prepared in `mhatipog/spark`; see the "Fix branches" section of
`2026-10-31-hunt-new-bugs-and-upstream-drafts.md` for names, status and test results:

- N2: `research/queryproof-fix-n2-filterestimation-ndv-zero` (commit ec22a0d)
- N1: `research/queryproof-fix-n1-dpp-union-nonpartitioned` (commit 2eb639a: fix and test)
- G: `research/queryproof-fix-g-gap-date-pushdown` (commit c2fe5d8, on master 4fecd73).
  Verified: 191 of 191 tests pass, and the negative control
  `research/queryproof-fix-g-test-only-control` fails the new test.
- N4 (`research/queryproof-fix-n4-orc-nested-isnull`, 4c95160) and N5
  (`research/queryproof-fix-n5-agg-pushdown-empty-count`, 8a54e2f): **do not open PRs.**
  apache/spark#59747 and #59763 already fix them (see the correction at the top). Keep
  the branches as research records.

Optionally rename a branch to something shorter first, e.g. `SPARK-NNNNN`. Do not use
the `…-test-only-control` branches: they contain the tests without the fix.
For each one:

1. Open the JIRA from §1 or §2 and note its number, e.g. SPARK-61234.
2. Rename the commit subject placeholder `[SPARK-XXXXX]` to the real number:
   `git commit --amend`, then force-push the fix branch. The fix branches are separate
   from the research branch.
3. Make sure the "Build" workflow on your fork succeeded for that branch.
4. Open the PR from `mhatipog/spark:<branch>` to `apache/spark:master`.
   - Title: `[SPARK-NNNNN][SQL] <JIRA summary>`.
   - Fill the template: what/why/user-facing change ("Yes, the query no longer fails"),
     how it was tested (the new unit test), and the generative-AI question (Yes).
5. Link the PR in the JIRA (it usually links itself when the title carries the number).

You can also file only the JIRAs and leave the fixes to committers. That is accepted
practice, and the reproductions above are complete.
