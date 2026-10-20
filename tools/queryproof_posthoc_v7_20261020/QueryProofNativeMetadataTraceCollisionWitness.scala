/* QueryProof: native RuleExecutor effective event with equal JSON endpoints.
 *
 * A deliberately constructed metadata-only rule creates a genuinely
 * unequal Spark LogicalPlan. Official Spark determines the rule is
 * effective using fastEquals, and QueryProof's existing disposable
 * RuleExecutor overlay records the event with before/after.toJSON.
 * The JSON encoding erases Metadata, so BOTH fields are identical.
 *
 * The rule is TEST-ONLY, the serialization behavior is PRODUCTION SPARK.
 * This is not a wrong-answer case or evidence W32 contained the event.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

object QueryProofNativeMetadataTraceCollisionWitness {
  private val alpha = new MetadataBuilder().putString("qp-value", "alpha").build()
  private val beta = new MetadataBuilder().putString("qp-value", "beta").build()

  object ChangeOnlyMetadata extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case r: LocalRelation =>
        LocalRelation(r.output.map {
          case a: AttributeReference => a.withMetadata(beta)
          case a => throw new IllegalStateException(
            "Control expected only AttributeReference output")
        })
      case _ => plan
    }
  }

  object ActualRuleExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("MetadataTransportWitness", Once, ChangeOnlyMetadata))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.query.id", "NATIVE_METADATA_JSON_COLLISION")

    val attr = AttributeReference("x", IntegerType, nullable = true, alpha)(
      ExprId(9431L, new UUID(0L, 1L)), Seq.empty)
    val before: LogicalPlan = LocalRelation(Seq(attr))
    val stderr = System.err
    val bytes = new ByteArrayOutputStream()
    val capture = new PrintStream(bytes, true, "UTF-8")
    val after = try {
      System.setErr(capture)
      ActualRuleExecutor.execute(before)
    } finally {
      capture.flush()
      System.setErr(stderr)
      capture.close()
    }
    assert(!before.fastEquals(after), "Spark failed to mark metadata change effective")
    assert(before.toJSON == after.toJSON,
      "Expected official Catalyst toJSON metadata collision")
    assert(before.output.head.metadata != after.output.head.metadata)

    val text = new String(bytes.toByteArray, StandardCharsets.UTF_8)
    val lines = text.split("\n").filter(_.startsWith("QUERYPROOF_AST\t"))
    assert(lines.length == 1,
      s"Expected one captured effective event, received ${lines.length}")
    val parts = lines.head.trim.split("\t").drop(1)
    assert(parts.length == 7, "Malformed real QUERYPROOF_AST event")
    val decoded = parts.map(x =>
      new String(Base64.getDecoder.decode(x), StandardCharsets.UTF_8))
    assert(decoded(3).contains("ChangeOnlyMetadata"),
      "Incorrect effective rule attributed to event")
    assert(decoded(5) == decoded(6),
      "Recorded event unexpectedly distinguishes differing source metadata")
    println("QUERYPROOF_NATIVE_METADATA_EFFECTIVE_RULE_EVENT_RECORDED")
    println("QUERYPROOF_NATIVE_UNEQUAL_SPARK_PLANS_IDENTICAL_CAPTURED_EVENT_JSON")
    println("QUERYPROOF_NATIVE_METADATA_ONLY_EVENT_CANNOT_BE_CERTIFIED_FROM_JSON_ALONE")
  }
}
