/*
 * QueryProof negative control — exact observer tag-encoding collision.
 *
 * This is an isolated experiment using genuine Spark 4.2 Catalyst TreeNode
 * tag storage. It never rewrites production Spark classes or frozen cohorts.
 */
package org.apache.spark.sql.catalyst.rules

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.types.{IntegerType, Metadata}

object QueryProofTagFrameCollision {
  private def queryProofValue(value: Any): String = value match {
    case null => "null"
    case s: scala.collection.Set[_] =>
      s.iterator.map(queryProofValue).toSeq.sorted.mkString("{", ",", "}")
    case m: scala.collection.Map[_, _] =>
      m.iterator.map { case (k, v) =>
        queryProofValue(k) + ":" + queryProofValue(v)
      }.toSeq.sorted.mkString("{", ",", "}")
    case s: Seq[_] => s.map(queryProofValue).mkString("[", ",", "]")
    case x => x.toString
  }

  private def queryProofNodeTags(node: TreeNode[_]): String = {
    val field = classOf[TreeNode[_]].getDeclaredFields.find(
      _.getName.endsWith("_tags")).getOrElse {
      throw new IllegalStateException("Catalyst private tag field absent")
    }
    field.setAccessible(true)
    val raw = field.get(node)
    if (raw == null) {
      ""
    } else {
      val tags = raw.asInstanceOf[
        scala.collection.mutable.Map[TreeNodeTag[_], Any]]
      tags.toSeq.map { case (key, value) =>
        key.name + ":" + queryProofValue(value)
      }.sorted.mkString("|")
    }
  }

  private def queryProofHash(data: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(data.getBytes(StandardCharsets.UTF_8))
      .map(b => f"${b & 0xff}%02x").mkString
  }

  def main(args: Array[String]): Unit = {
    val id = NamedExpression.newExprId
    val a = TreeNodeTag[String]("qpA")
    val b = TreeNodeTag[String]("qpB")
    val left = AttributeReference("x", IntegerType, true, Metadata.empty)(id)
    val right = AttributeReference("x", IntegerType, true, Metadata.empty)(id)

    // 1 tag and 2 tags have the same lossy delimiter-joined representation.
    left.setTagValue(a, "one|qpB:two")
    right.setTagValue(a, "one")
    right.setTagValue(b, "two")

    require(left.toJSON == right.toJSON, "Expected identical Catalyst plan JSON")
    require(left.getTagValue(a) != right.getTagValue(a),
      "The underlying tag values must genuinely differ")
    require(right.getTagValue(b).contains("two"),
      "The second plan must have an additional observable tag")
    require(!left.isTagsEmpty && !right.isTagsEmpty,
      "The runtime state witness must see one tagged node in each case")

    val rawLeft = queryProofNodeTags(left)
    val rawRight = queryProofNodeTags(right)
    require(rawLeft == rawRight,
      "The test did not reproduce the exact observer encoding collision")
    require(queryProofHash(rawLeft) == queryProofHash(rawRight),
      "Fingerprint collision was not reproduced")

    // Both plans: one tagged node, empty metadata, same ExprId and outputs.
    // The hash-based frame guard would see no drift despite changed map state.
    println("QUERYPROOF_TAG_FRAME_NONINJECTIVE=CONFIRMED")
    println("QUERYPROOF_DIFFERENT_TAG_MAPS_IDENTICAL_WITNESS=CONFIRMED")
    println("QUERYPROOF_SPARK_OPTIMIZER_BUG=NOT_CLAIMED")
  }
}
