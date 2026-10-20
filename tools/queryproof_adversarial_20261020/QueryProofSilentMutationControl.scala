/* Negative control for in-place, fastEquals-invisible Spark rule mutation.
 * The ordinary QueryProof AST tracer records only effective changes; this
 * experimental probe must notice mutation in the no-op rule case.
 */
package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofSilentMutationControl {
  private class Probe(rule: Rule[LogicalPlan]) extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(Batch("One rule", Once, rule))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    val spark = SparkSession.builder()
      .appName("queryproof-silent-rule-observer")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate().asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    try {
      val attr = AttributeReference("v", IntegerType, true, Metadata.empty)()
      val backing = spark.sparkContext.parallelize(Seq(InternalRow(3)), 1)
      val leaf = LogicalRDD(Seq(attr), backing)(spark)
      val mutationTag = TreeNodeTag[String]("qp-hidden-mutated-in-place")
      val malicious = new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          plan.setTagValue(mutationTag, "this value is deliberately never logged")
          plan
        }
      }
      System.setProperty("queryproof.query.id", "silent-mutant")
      val oldPlan = Project(Seq(attr), leaf)
      val returned = new Probe(malicious).execute(oldPlan)
      require(returned eq oldPlan, "In-place mutation control must return same JVM plan")
      require(oldPlan.getTagValue(mutationTag).nonEmpty)
      println("QUERYPROOF_REAL_FASTEQUALS_TAG_MUTATION=CONFIRMED")

      val benign = new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan
      }
      System.setProperty("queryproof.query.id", "silent-benign")
      val cleanPlan = Project(Seq(attr), leaf)
      val cleanReturned = new Probe(benign).execute(cleanPlan)
      require(cleanReturned eq cleanPlan)
      require(cleanPlan.getTagValue(mutationTag).isEmpty)
      println("QUERYPROOF_REAL_FASTEQUALS_BENIGN_CONTROL=CONFIRMED")
    } finally {
      spark.stop()
    }
  }
}
