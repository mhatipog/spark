# SPARK-60319 review experiment

The PR guard fixes the query in [#59438](https://github.com/apache/spark/issues/59438). With the correlated count fixture, projecting after `orderBy(age.desc).limit(3)` selected IDs 3, 4 and 6. Moving the projection before `limit(3)` selected IDs 1, 2 and 3. Disabling `PushProjectionThroughLimitAndOffset` gave the same two results. The second query therefore needs a fix outside the original pushdown regression.

## Regression tests

Spark 5.0.0-SNAPSHOT from `61150996`, Scala 2.13.18, JDK 17 on Linux. Each variant used the same compiled Spark and tests. Only the optimizer rule differed. The unpatched rule matches the blob on the parent of the first PR commit (`2dad6637`).

| Rule | Slice then project | Project then slice |
| --- | ---: | ---: |
| Before the PR | 16/48 | 16/48 |
| PR guard, `27c65ec3` | 48/48 | 16/48 |
| Lift projections, `61150996` | 48/48 | 48/48 |

Each column covers scalar count, EXISTS and IN, four combinations of LIMIT and OFFSET, and both settings of broadcast and AQE. These assertions check row membership and projected values. The original SQL regression failed with the unpatched rule and passed with the guard. With the PR guard, its Catalyst suite passed 3/3 and SubquerySuite passed 117/117 executed tests (one ignored). The prototype passed all five Catalyst tests and all 144 tests in `DataFrameSubquerySuite`.

From a disposable checkout of `61150996`, run the following for each rule revision. Restore only the optimizer file to keep the test inputs identical.

```sh
rule=sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PushProjectionThroughLimitAndOffset.scala
git restore --source=27c65ec3 -- "$rule"
build/sbt 'sql/testOnly org.apache.spark.sql.DataFrameSubquerySuite -- -z "ordered row boundaries:"'
```

## A counterexample to the lift

Run in a Scala shell built from `61150996`, with `--master local[4]`.

```scala
spark.conf.set("spark.sql.shuffle.partitions", 3)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", true)
val p = spark.range(6).selectExpr("id + 1 AS id",
  "element_at(array(30,20,40,60,10,50), cast(id+1 as int)) AS age")
val k = Seq(1L, 1L, 1L, 2L).toDF("a")
val q = p.orderBy($"age".desc).select($"id", $"id".isin(k).as("v")).offset(1)
q.take(2).map(_.getLong(0)).sorted   // 3, 6
q.show(2)                             // displayed IDs 1, 3
```

The sorted IDs were 4, 6, 3, 1, 2, 5. `show(2)` fetches three rows to decide whether to print the truncation message. The join can reorder those three before the driver displays two. Correlated count and EXISTS exhibited the same discrepancy.

A shorter rewrite moved a global sort above a directly adjacent deterministic subquery projection and carried the sort keys through the projection. It passed the 96 placement tests but failed 8 of 15 checks that also varied the input shape. With the same fixture, `orderBy(age.desc).limit(5).select(id, inColumn).offset(1).limit(2)` returned IDs 2 and 3 instead of 6 and 3. Neither prototype is part of #59445.

## Broadcast IN benchmark

Local classic Spark, four cores, AQE off, eight shuffle partitions. The probe was a range of 262,144 rows in 64 partitions. The build side contained 320 keys. The sort key was `(id * 123457) % 262144`, descending. Unordered LIMIT kept 10 rows, sorted LIMIT kept 1,024, and OFFSET skipped 261,120. Each action collected both `id` and its IN result and checked the result.

The cells give median elapsed milliseconds and total tasks. Each variant ran in two fresh JVMs with the variant order reversed on the second pass. Each JVM ran two warm-up rounds and six measured rounds, shuffling query order within each round.

| Query | Before PR | PR guard | Lift |
| --- | ---: | ---: | ---: |
| Unordered LIMIT, then project | 31 ms / 3 | 128 ms / 67 | 118 ms / 67 |
| Sorted LIMIT, then project | 226 ms / 131 | 133 ms / 67 | 129 ms / 67 |
| Sorted OFFSET, then project | 282 ms / 138 | 314 ms / 139 | 302 ms / 139 |
| Unordered OFFSET, then project | 100 ms / 66 | 139 ms / 67 | 140 ms / 67 |
| Sort, project, LIMIT | 203 ms / 131 | 198 ms / 131 | 124 ms / 67 |
| Sort, project, OFFSET | 256 ms / 138 | 230 ms / 138 | 291 ms / 139 |

With the PR guard, unordered LIMIT lost the root `CollectLimit` and read all 64 probe partitions. Sorted LIMIT used `TakeOrderedAndProject` before the join. Sorted OFFSET introduced a single-partition exchange and a local sort. These timings describe this local broadcast-join fixture.

<details>
<summary>Benchmark driver</summary>

Place the Scala object below under `sql/core/src/test/scala/org/apache/spark/sql/` in the disposable checkout and run `build/sbt 'sql/Test/runMain org.apache.spark.sql.BoundaryBench'` with each rule.


```scala
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughLimitAndOffset

object BoundaryBench {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("SPARK-60319")
      .config("spark.ui.enabled", false)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", 8)
      .config("spark.sql.adaptive.enabled", false).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val rule = PushProjectionThroughLimitAndOffset.getClass
    println(s"RULE ${rule.getProtectionDomain.getCodeSource.getLocation} runtime=${spark.version}")
    try {
      val n = 262144
      val keep = 1024
      val input = spark.range(0, n, 1, 64).selectExpr("id", s"(id * 123457) % $n AS age")
      val small = spark.range(0, 320, 1, 2).selectExpr("id AS k")
      val ids = (0 until n).map(i => i.toLong).sortBy(i => -((i * 123457) % n))
      def run(name: String, round: Int): Unit = {
        val sorted = name.startsWith("sorted")
        val limit = name.contains("-limit")
        val df = if (sorted) input.orderBy($"age".desc) else input
        def slice(input: DataFrame): DataFrame =
          if (limit) input.limit(if (sorted) keep else 10) else input.offset(n - keep)
        def project(input: DataFrame): DataFrame = input.select($"id", $"id".isin(small).as("in_k"))
        val q = if (name.endsWith("-first")) slice(project(df)) else project(slice(df))
        val sc = spark.sparkContext
        val group = s"$round-$name"
        sc.setJobGroup(group, group)
        val start = System.nanoTime()
        val rows = q.collect().toSeq
        val ms = (System.nanoTime() - start) / 1000000.0
        val tracker = sc.statusTracker
        val stages = tracker.getJobIdsForGroup(group).flatMap(id =>
          tracker.getJobInfo(id).toSeq.flatMap(_.stageIds)).distinct
        val tasks = stages.flatMap(id => tracker.getStageInfo(id).map(_.numTasks)).sum
        val actual = rows.map(_.getLong(0))
        require(actual.distinct.size == rows.size && rows.forall(r =>
          r.getBoolean(1) == (r.getLong(0) < 320)))
        val correct = if (sorted) {
          actual.sorted == (if (limit) ids.take(keep) else ids.takeRight(keep)).sorted
        } else actual.size == (if (limit) 10 else keep)
        if (round == 0) println(s"PLAN $name\n" + q.queryExecution.executedPlan)
        println(f"BENCH,$round,$name,$ms%.2f,$tasks,$correct")
      }
      val cases = Vector("unordered-limit", "sorted-offset", "sorted-limit", "unordered-offset",
        "sorted-limit-first", "sorted-offset-first")
      for (r <- 0 until 8; name <- new scala.util.Random(713 + r).shuffle(cases)) run(name, r)
    } finally { spark.stop() }
  }
}
```

</details>
