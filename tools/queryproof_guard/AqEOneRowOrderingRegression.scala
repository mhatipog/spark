/*
 * Research-only standalone Scala regression for SPARK-59126.
 * Can be compiled against official Spark wheel jars without ScalaTest.
 */
package org.queryproof

import org.apache.spark.sql.SparkSession

object AqEOneRowOrderingRegression {
  private val query =
    """WITH top AS (
      | SELECT id, CAST(id % 1 AS INT) AS k
      | FROM range(0,3,1,1)
      | ORDER BY id ASC LIMIT 2 OFFSET 1
      |), rhs AS (
      | SELECT id AS bid, CAST(id % 1 AS INT) AS k
      | FROM range(0,1,1,1)
      |)
      |SELECT top.id,top.k,rhs.bid FROM top LEFT JOIN rhs USING(k)
      |ORDER BY top.id ASC,rhs.bid ASC
      |""".stripMargin

  private val expected = Seq((1L, 0, 0L), (2L, 0, 0L))
  private val inlineCTE = "org.apache.spark.sql.catalyst.optimizer.InlineCTE"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName("QueryProof-AQE-Scala-Regressions")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      for (mode <- Seq("bug-trigger", "normal", "aqe-off")) {
        val repetitions = if (mode == "bug-trigger") 8 else 3
        (0 until repetitions).foreach { trial =>
          val session = spark.newSession()
          session.conf.set("spark.sql.adaptive.enabled", (mode != "aqe-off").toString)
          session.conf.set("spark.sql.shuffle.partitions", "5")
          if (mode != "normal") {
            session.conf.set("spark.sql.optimizer.excludedRules", inlineCTE)
          }
          val actual = session.sql(query).collect().toSeq.map { row =>
            (row.getLong(0), row.getInt(1), row.getLong(2))
          }
          require(actual == expected,
            s"$mode trial $trial: ORDER BY lost on ${spark.version}: $actual, expected=$expected")
        }
        println(s"QUERYPROOF_SCALA_REGRESSION ${spark.version} $mode PASS $repetitions")
      }
      val single = spark.sql("SELECT id FROM range(1) ORDER BY id")
      require(single.collect().map(_.getLong(0)).toSeq == Seq(0L))
      val local = spark.sql("SELECT id FROM range(1) SORT BY id")
      require(local.collect().map(_.getLong(0)).toSeq == Seq(0L))
      println(s"QUERYPROOF_SCALA_REGRESSION ${spark.version} one-row-sorts PASS")
    } finally {
      spark.stop()
    }
  }
}
