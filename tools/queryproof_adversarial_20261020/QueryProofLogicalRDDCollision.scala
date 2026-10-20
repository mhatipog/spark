/*
 * Deliberately invalid logical-input substitution in real Spark 4.2.0.
 *
 * Two LogicalRDD objects have the same output attributes and TreeNode.toJSON
 * but different backing RDDs with different row bags. Used only as a negative
 * control for QueryProof, never as an optimizer rule or product patch.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofLogicalRDDCollision {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("queryproof-logical-rdd-collision")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate().asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    try {
      val a = AttributeReference("v0", IntegerType, true, Metadata.empty)()
      val b = AttributeReference("v1", IntegerType, true, Metadata.empty)()
      val attrs = Seq(a, b)

      val rdd1 = spark.sparkContext.parallelize(Seq(InternalRow(1, 7)), 1)
      val rdd2 = spark.sparkContext.parallelize(Seq(InternalRow(2, 7)), 1)
      val left = LogicalRDD(attrs, rdd1)(spark)
      val right = LogicalRDD(attrs, rdd2)(spark)

      require(left.rdd.id != right.rdd.id,
        "Independent input relations must not share an RDD identity")
      require(left.output == right.output,
        "Control requires identical output attributes and ExprIds")
      require(left.toJSON == right.toJSON,
        "The anticipated real Catalyst JSON collision did not reproduce")
      require(left.rdd.collect().map(_.getInt(0)).toSeq !=
        right.rdd.collect().map(_.getInt(0)).toSeq,
        "Input row-bags must genuinely differ")
      require(left.isTagsEmpty && right.isTagsEmpty,
        "This control must work independently of TreeNode tags")

      val w = new PrintWriter("artifacts/queryproof_logical_rdd_collision.json")
      try {
        w.println(s"""{"left":${left.toJSON},"right":${right.toJSON}}""")
      } finally {
        w.close()
      }
      println("QUERYPROOF_REAL_LOGICAL_RDD_JSON_COLLISION=CONFIRMED")
      println("QUERYPROOF_DIFFERENT_BACKING_ROWBAGS=CONFIRMED")
      println("QUERYPROOF_TAGS_EMPTY_IN_BOTH_RELATIONS=CONFIRMED")
      println("QUERYPROOF_SPARK_OPTIMIZER_BUG=NOT_CLAIMED")
    } finally {
      spark.stop()
    }
  }
}
