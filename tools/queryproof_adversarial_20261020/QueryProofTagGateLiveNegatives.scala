/*
 * End-to-end adversarial tag census controls in official Spark 4.2.0.
 * No tag values or SQL literals are emitted; they remain only in the JVM.
 */
package org.apache.spark.sql.catalyst.rules

import scala.collection.mutable.HashSet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofTagGateLiveNegatives {
  private class Probe extends RuleExecutor[LogicalPlan] {
    private val identity = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan
    }
    override protected def batches: Seq[Batch] =
      Seq(Batch("Tag-gate identity control", Once, identity))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    val spark = SparkSession.builder()
      .appName("queryproof-live-tag-shape-negatives")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate().asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    try {
      val attr = AttributeReference("x", IntegerType, false, Metadata.empty)()
      val backing = spark.sparkContext.parallelize(Seq(InternalRow(5)), 1)
      val leaf = LogicalRDD(Seq(attr), backing)(spark)
      val runner = new Probe
      def test(label: String)(annotate: LogicalPlan => Unit): Unit = {
        val plan = Project(Seq(attr), leaf)
        annotate(plan)
        System.setProperty("queryproof.query.id", label)
        require(runner.execute(plan) eq plan, "Probe should not rewrite this plan")
        println("QUERYPROOF_LIVE_TAG_FIXTURE_EXECUTED=" + label)
      }

      test("tag-valid-long") { p =>
        p.setTagValue(TreeNodeTag[HashSet[Long]]("dataset_id"),
          HashSet[Long](101L, 202L))
      }
      test("tag-invalid-member") { p =>
        // Intentionally violates the source-pinned TreeNodeTag[HashSet[Long]].
        p.setTagValue(TreeNodeTag[HashSet[String]]("dataset_id"),
          HashSet[String]("not-a-long"))
      }
      test("tag-processed-join") { p =>
        p.setTagValue(TreeNodeTag[String]("processedJoinCondition"),
          "this tag must never be whitelisted")
      }
      test("tag-unknown-key") { p =>
        p.setTagValue(TreeNodeTag[String]("qp-invented-gate-probe"),
          "the unknown key must remain redacted")
      }
    } finally {
      spark.stop()
    }
  }
}
