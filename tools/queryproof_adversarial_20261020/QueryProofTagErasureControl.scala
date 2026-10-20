/*
 * QueryProof: real Spark 4.2.0 positive/negative TreeNode-tag erasure controls.
 *
 * Research-only, never patches a production rule. No input values, plan text,
 * tag values or SQL literals are printed. Distinguishes a narrow phase-local
 * dataset_id hypothesis from the unsound blanket "all tags are inert" claim.
 */
package org.apache.spark.sql.catalyst.rules

import scala.collection.mutable.HashSet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.PushExtraPredicateThroughJoin
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofTagErasureControl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("queryproof-tag-erasure-adversarial-control")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate().asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    try {
      // Same attribute identities and *same physical RDD input* in both plans.
      val x = AttributeReference("x", IntegerType, false, Metadata.empty)()
      val raw = spark.sparkContext.parallelize(
        Seq(InternalRow(1), InternalRow(3), InternalRow(5)), 1)
      val leaf = LogicalRDD(Seq(x), raw)(spark)
      val first = Project(Seq(x), leaf)
      val second = Project(Seq(x), leaf)
      first.setTagValue(org.apache.spark.sql.classic.Dataset.DATASET_ID_TAG,
        HashSet[Long](101L))
      second.setTagValue(org.apache.spark.sql.classic.Dataset.DATASET_ID_TAG,
        HashSet[Long](202L))
      require(first.getTagValue(
        org.apache.spark.sql.classic.Dataset.DATASET_ID_TAG) !=
        second.getTagValue(org.apache.spark.sql.classic.Dataset.DATASET_ID_TAG))
      require(first.toJSON == second.toJSON,
        "Positive case must have identical tag-erased Catalyst serialization")
      require(first.collectLeaves().head eq second.collectLeaves().head,
        "Positive control must share the exact physical backing input leaf")

      val optimizedFirst = spark.sessionState.optimizer.execute(first)
      val optimizedSecond = spark.sessionState.optimizer.execute(second)
      require(optimizedFirst.toJSON == optimizedSecond.toJSON,
        "Distinct dataset_id values unexpectedly altered the optimized AST")
      val bag1 = org.apache.spark.sql.classic.Dataset.ofRows(spark, first)
        .collect().map(_.getInt(0)).sorted.toSeq
      val bag2 = org.apache.spark.sql.classic.Dataset.ofRows(spark, second)
        .collect().map(_.getInt(0)).sorted.toSeq
      require(bag1 == bag2 && bag1.size == 3,
        "dataset_id variation must preserve genuine executed row bags")
      println("QUERYPROOF_DATASET_ID_OPTIMIZER_STRUCTURE_SAME=CONFIRMED")
      println("QUERYPROOF_DATASET_ID_EXECUTED_ROWBAGS_SAME=CONFIRMED")
      println("QUERYPROOF_DATASET_ID_PHASE_NONINTERFERENCE=CONTROL_ONLY")

      // Spark's *actual* PushExtraPredicateThroughJoin reads this tag as a
      // memoization guard. A tag-only difference must alter a real rule result.
      val y = AttributeReference("y", IntegerType, false, Metadata.empty)()
      val rawRight = spark.sparkContext.parallelize(
        Seq(InternalRow(2), InternalRow(4), InternalRow(6)), 1)
      val right = LogicalRDD(Seq(y), rawRight)(spark)
      val cond = Or(
        And(EqualTo(x, Literal(1)), EqualTo(y, Literal(2))),
        And(EqualTo(x, Literal(3)), EqualTo(y, Literal(4))))
      val noTagJoin = Join(leaf, right, Inner, Some(cond), JoinHint.NONE)
      val markedJoin = Join(leaf, right, Inner, Some(cond), JoinHint.NONE)
      val processed = TreeNodeTag[Expression]("processedJoinCondition")
      markedJoin.setTagValue(processed, cond)
      require(noTagJoin.toJSON == markedJoin.toJSON,
        "Negative control requires same Catalyst JSON despite tag change")
      val pushed = PushExtraPredicateThroughJoin(noTagJoin)
      val skipped = PushExtraPredicateThroughJoin(markedJoin)
      require(pushed.exists(_.isInstanceOf[Filter]),
        "Unmarked join should create a real pushed Filter")
      require(!skipped.exists(_.isInstanceOf[Filter]),
        "Marked join should skip previously processed predicate push")
      require(pushed.toJSON != skipped.toJSON,
        "A blanket tag erasure would conceal different rule outcomes")
      println("QUERYPROOF_PROCESSED_JOIN_TAG_RULE_BEHAVIOR_DIFFERS=CONFIRMED")
      println("QUERYPROOF_GENERIC_TAG_ERASURE=FALSIFIED")
      println("QUERYPROOF_SPARK_OPTIMIZER_BUG=NOT_CLAIMED")
    } finally {
      spark.stop()
    }
  }
}
