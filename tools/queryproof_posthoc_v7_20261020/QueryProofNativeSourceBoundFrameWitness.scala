/* QueryProof posthoc V8 seed: a source-bound metadata and identity frame.
 *
 * The native Spark TreeNode.toJSON erases Metadata (reproduced in run
 * 39461415673). Here we derive a separate, path-indexed, nontruncated
 * frame DIRECTLY from the actual Catalyst objects. The intentionally
 * tiny ACCEPTABLE grammar is an empty, non-streaming LocalRelation
 * with AttributeReference outputs only. Everything else throws UNKNOWN.
 *
 * This is source-attached observational evidence, NOT a verified
 * Spark-to-Coq interpretation, whole-optimizer trace or proof that
 * any post-cut mutation event was captured.
 */
package org.apache.spark.sql.catalyst.expressions

import java.util.UUID

import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder}

object QueryProofNativeSourceBoundFrameWitness {
  private def md(value: String): Metadata =
    new MetadataBuilder().putString("qp-test", value).build()

  private def attribute(id: Long, metadata: Metadata): AttributeReference =
    AttributeReference("x", IntegerType, nullable = true, metadata)(
      ExprId(id, new UUID(0L, 1L)), Seq.empty)

  /** Fail closed for ANY non-recognized plan or source attribute field. */
  private def sourceBoundFrame(plan: LogicalPlan): String = plan match {
    case relation: LocalRelation if !relation.isStreaming && relation.data.isEmpty =>
      val terms = relation.output.zipWithIndex.map {
        case (a: AttributeReference, index) =>
          val rawMetadata = a.metadata.json
          require(rawMetadata.length <= 65536,
            "UNKNOWN_OVERSIZED_SOURCE_METADATA")
          require(a.qualifier.size <= 8 &&
            a.qualifier.forall(_.length <= 1024),
            "UNKNOWN_QUALIFIER_LENGTH")
          JObject(List(
            "path" -> JString(s"/output/$index"),
            "class" -> JString(a.getClass.getName),
            "name" -> JString(a.name),
            "datatype" -> JString(a.dataType.json),
            "nullable" -> JBool(a.nullable),
            "expr_id_number" -> JInt(a.exprId.id),
            "expr_id_jvm" -> JString(a.exprId.jvmId.toString),
            "qualifier" -> JArray(a.qualifier.map(JString(_)).toList),
            "metadata_exact" -> JString(rawMetadata)))
        case _ => throw new IllegalArgumentException("UNKNOWN_OUTPUT_EXPRESSION")
      }
      compact(render(JObject(List(
        "schema" -> JString("queryproof.source-bound.localrelation.v0"),
        "source_plan_class" -> JString(relation.getClass.getName),
        "input_bag_kind" -> JString("empty"),
        "streaming" -> JBool(false),
        "outputs" -> JArray(terms.toList)))))
    case _ => throw new IllegalArgumentException("UNKNOWN_PLAN_CLASS_OR_DATA")
  }

  def main(args: Array[String]): Unit = {
    val a = attribute(11L, md("first"))
    val b = attribute(12L, md("second"))
    val x = LocalRelation(Seq(a, b))
    val y = LocalRelation(Seq(
      a.withMetadata(md("second")), b.withMetadata(md("first"))))
    assert(!x.fastEquals(y))
    assert(x.toJSON == y.toJSON,
      "Native serializer should not reveal changes in source metadata")
    println("QUERYPROOF_SOURCE_FRAME_NATIVE_JSON_COLLISION_CONFIRMED")

    val xFrame = sourceBoundFrame(x)
    val yFrame = sourceBoundFrame(y)
    assert(xFrame != yFrame,
      "Source-bound, path-indexed frame conflated swapped metadata")
    assert(xFrame.contains("/output/0") && xFrame.contains("/output/1"))
    assert(xFrame.contains("00000000-0000-0000-0000-000000000001"))
    println("QUERYPROOF_SOURCE_FRAME_PATH_INDEXED_METADATA_SWAP_DISTINGUISHED")
    println("QUERYPROOF_SOURCE_FRAME_A=" + xFrame)
    println("QUERYPROOF_SOURCE_FRAME_B=" + yFrame)

    val unsupported = Project(Seq(Alias(Literal(1), "x")()), OneRowRelation())
    val rejected = try {
      sourceBoundFrame(unsupported)
      false
    } catch {
      case e: IllegalArgumentException =>
        e.getMessage.contains("UNKNOWN_PLAN_CLASS_OR_DATA")
    }
    assert(rejected, "Out-of-fragment Catalyst plan was not rejected")
    println("QUERYPROOF_SOURCE_FRAME_UNSUPPORTED_PLAN_UNKNOWN")
    println("QUERYPROOF_SOURCE_FRAME_MINIMAL_REPAIR_DEVELOPMENT_CONFIRMED")
  }
}
