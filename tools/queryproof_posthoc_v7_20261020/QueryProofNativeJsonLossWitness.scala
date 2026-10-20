/* QueryProof: official Spark 4.2.0 Catalyst toJSON non-injectivity.
 *
 * Research only: the production TreeNode.parseToJson erases Metadata.
 * Compare genuine unequal Spark AttributeReference and LogicalPlan objects
 * against their toJSON representations. Include discriminating positive
 * controls for a full JVM ExprId, datatype and nullability.
 *
 * This is NOT a claim about SQL wrong answers or W32 occurrence.
 */
package org.apache.spark.sql.catalyst.expressions

import java.util.UUID

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{IntegerType, LongType, Metadata, MetadataBuilder}

object QueryProofNativeJsonLossWitness {
  private def metadata(label: String): Metadata =
    new MetadataBuilder().putString("queryproof_provenance", label).build()

  private def attribute(
      md: Metadata,
      eid: ExprId,
      nullable: Boolean = true,
      longType: Boolean = false): AttributeReference =
    AttributeReference("same_name", if (longType) LongType else IntegerType, nullable, md)(
      eid, Seq.empty)

  def main(args: Array[String]): Unit = {
    val sameIdentity = ExprId(7331L, new UUID(0L, 1L))
    val mdOne = metadata("secret-A")
    val mdTwo = metadata("secret-B")
    assert(mdOne.json != mdTwo.json)

    val first = attribute(mdOne, sameIdentity)
    val second = attribute(mdTwo, sameIdentity)
    assert(first.exprId == second.exprId)
    assert(first.metadata != second.metadata)
    assert(first != second && !first.fastEquals(second))
    assert(first.toJSON == second.toJSON,
      "This test expects official TreeNode.toJSON to erase metadata")
    println("QUERYPROOF_NATIVE_UNEQUAL_ATTRIBUTE_EQUAL_JSON_METADATA_ERASURE")

    val relationFirst = LocalRelation(Seq(first))
    val relationSecond = LocalRelation(Seq(second))
    assert(!relationFirst.fastEquals(relationSecond),
      "Different attribute metadata must distinguish genuine Catalyst plans")
    assert(relationFirst.toJSON == relationSecond.toJSON,
      "Metadata-erased serializer collision must survive a real logical-plan wrapper")
    println("QUERYPROOF_NATIVE_UNEQUAL_LOGICAL_PLAN_EQUAL_JSON_METADATA_ERASURE")

    val otherJVM = attribute(mdOne, ExprId(7331L, new UUID(0L, 2L)))
    assert(first.exprId.id == otherJVM.exprId.id)
    assert(first.exprId != otherJVM.exprId)
    assert(first.toJSON != otherJVM.toJSON,
      "Full jvmId should be retained for Spark ExprId")
    println("QUERYPROOF_NATIVE_EXPRID_JVM_UUID_DISAMBIGUATES_EQUAL_NUMERIC_ID")

    val otherNullability = attribute(mdOne, sameIdentity, nullable = false)
    assert(first.toJSON != otherNullability.toJSON,
      "Nullable field should remain visible")
    val otherType = attribute(mdOne, sameIdentity, longType = true)
    assert(first.toJSON != otherType.toJSON,
      "Datatype field should remain visible")
    println("QUERYPROOF_NATIVE_DATATYPE_NULLABILITY_POSITIVE_CONTROLS")

    val firstJson = first.toJSON
    val secondJson = second.toJSON
    println("QUERYPROOF_NATIVE_SERIALIZED_ATTRIBUTE=" + firstJson)
    println("QUERYPROOF_NATIVE_SOURCE_METADATA_A=" + mdOne.json)
    println("QUERYPROOF_NATIVE_SOURCE_METADATA_B=" + mdTwo.json)
    assert(firstJson == secondJson)
    println("QUERYPROOF_NATIVE_JSON_LOSS_EFFECT_CONFIRMED")
  }
}
