# Filing the QueryProof findings upstream (paste-ready)

Spark tracks bugs in **Apache JIRA**, project SPARK
(<https://issues.apache.org/jira/projects/SPARK>). Pull requests go to
`apache/spark` on GitHub and are titled `[SPARK-NNNNN][SQL] …`, referencing the JIRA.
GitHub Issues is enabled on apache/spark, but the contributing guide
(<https://spark.apache.org/contributing.html>) still points bug reports to JIRA, and
committers triage there.

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

## 4. Pull requests (after the JIRAs exist)

Fix branches are prepared in `mhatipog/spark`; see the "Fix branches" section of
`2026-10-31-hunt-new-bugs-and-upstream-drafts.md` for names, status and test results:

- N2: `research/queryproof-fix-n2-filterestimation-ndv-zero` (commit ec22a0d)
- N1: `research/queryproof-fix-n1-dpp-union-nonpartitioned` (commit 2eb639a)

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
