/*
 * Disposable integration control: actual RuleExecutor rule boundaries.
 * A malicious input-RDD substitution must be rejected by the independently
 * emitted JVM input observer, even when Catalyst.toJSON is unchanged.
 * A simple same-RDD project collapse should preserve that input witness.
 */
package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofActualRuleInputControl {
  private class Probe(rule: Rule[LogicalPlan]) extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(Batch("One test rule", Once, rule))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    val spark = SparkSession.builder()
      .appName("queryproof-real-rule-input-attestation")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate().asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    try {
      val a = AttributeReference("v0", IntegerType, true, Metadata.empty)()
      val b = AttributeReference("v1", IntegerType, true, Metadata.empty)()
      val attrs = Seq(a, b)
      val leftRdd = spark.sparkContext.parallelize(Seq(InternalRow(1, 7)), 1)
      val rightRdd = spark.sparkContext.parallelize(Seq(InternalRow(2, 7)), 1)
      require(leftRdd.collect().head.getInt(0) != rightRdd.collect().head.getInt(0))
      val left = LogicalRDD(attrs, leftRdd)(spark)
      val negativeBefore = Project(attrs, left)
      val swap = new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
          case r: LogicalRDD => LogicalRDD(r.output, rightRdd)(spark)
        }
      }
      System.setProperty("queryproof.query.id", "swap-negative")
      val negativeAfter = new Probe(swap).execute(negativeBefore)
      require(negativeAfter != negativeBefore, "Input RDD mutation must be a real rule event")
      require(negativeBefore.toJSON == negativeAfter.toJSON,
        "Control requires the known lossy Catalyst-JSON alias")
      println("QUERYPROOF_ACTUAL_RULE_JSON_FALSE_IDENTITY=CONFIRMED")

      val positiveBefore = Project(attrs, Project(attrs, left))
      val collapse = new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
          case Project(projectList, Project(_, child)) =>
            Project(projectList, child)
        }
      }
      System.setProperty("queryproof.query.id", "same-rdd-positive")
      val positiveAfter = new Probe(collapse).execute(positiveBefore)
      require(positiveAfter != positiveBefore, "Positive must be an actual changed rule")
      require(positiveAfter.toJSON != positiveBefore.toJSON)
      require(positiveAfter.collectLeaves().exists {
        case r: LogicalRDD => r.rdd eq leftRdd
        case _ => false
      }, "Positive rewrite must retain the exact original RDD reference")
      println("QUERYPROOF_ACTUAL_RULE_SAME_RDD_POSITIVE=CONFIRMED")
    } finally {
      spark.stop()
    }
  }
}
