/*
 * Disposable control: demonstrate that distinct JVM Catalyst states share a JSON encoding.
 * Compile only in the personal fork's isolated GitHub Actions job.
 */
package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder}

object QueryProofStateCollision {
  def main(args: Array[String]): Unit = {
    val id = NamedExpression.newExprId
    val metadata = new MetadataBuilder().putString("audit_control", "nonempty").build()
    val plain = AttributeReference("x", IntegerType, true, Metadata.empty)(id)
    val decorated = AttributeReference("x", IntegerType, true, metadata)(id)

    require(plain != decorated, "Control needs genuinely unequal Catalyst expressions")
    require(plain.toJSON == decorated.toJSON,
      "Expected metadata-insensitive TreeNode JSON collision")
    val beforeTag = plain.toJSON
    plain.setTagValue(TreeNodeTag[String]("queryproof-hidden-tag"), "changed")
    require(!plain.isTagsEmpty, "Tag control was not applied")
    require(plain.toJSON == beforeTag, "Expected tag-insensitive TreeNode JSON collision")
    println("QUERYPROOF_REAL_JVM_JSON_COLLISIONS=METADATA_AND_TAGS_CONFIRMED")
  }
}
