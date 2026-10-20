/* QueryProof posthoc V8: native real-rule event source frames.
 *
 * Adversarial experiment: a genuine Spark metadata-only rewrite produces
 * identical legacy toJSON before/after, while V8's source-bound event captures
 * different metadata at explicit output paths. A non-supported effective
 * Project rule must emit UNKNOWN, never a V8 acceptable event.
 *
 * Test-only rules, production Spark equality/serialization and RuleExecutor.
 * No claim to certify general optimizer transformations or hidden tags.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

object QueryProofNativeV8EventWitness {
  private val alpha = new MetadataBuilder().putString("qp-value", "alpha").build()
  private val beta = new MetadataBuilder().putString("qp-value", "beta").build()

  object ChangeOnlyMetadata extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case r: LocalRelation =>
        LocalRelation(r.output.map {
          case a: AttributeReference => a.withMetadata(beta)
          case _ => throw new IllegalArgumentException("unexpected output")
        })
      case _ => plan
    }
  }
  object MetadataExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V8SourceFrame", Once, ChangeOnlyMetadata))
  }

  object ChangeProject extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case p: Project =>
        Project(Seq(Alias(Literal(2), "v8-unrecognized")()), p.child)
      case _ => plan
    }
  }
  object UnsupportedExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V8UnsupportedProject", Once, ChangeProject))
  }

  private def evaluate(
      input: LogicalPlan,
      executor: RuleExecutor[LogicalPlan]): (LogicalPlan, String) = {
    val savedErr = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val output = try {
      System.setErr(sink)
      executor.execute(input)
    } finally {
      sink.flush()
      System.setErr(savedErr)
      sink.close()
    }
    (output, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  private def records(log: String, marker: String): Seq[Array[String]] =
    log.split("\n").filter(_.startsWith(marker + "\t")).toSeq.map { line =>
      line.trim.split("\t").drop(1).map(part =>
        new String(Base64.getDecoder.decode(part), StandardCharsets.UTF_8))
    }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.query.id", "V8_NATIVE_SOURCE_BOUND_EVENT")

    val attr = AttributeReference("x", IntegerType, nullable = true, alpha)(
      ExprId(9431L, new UUID(0L, 1L)), Seq.empty)
    val before: LogicalPlan = LocalRelation(Seq(attr))
    val (after, log) = evaluate(before, MetadataExecutor)
    assert(!before.fastEquals(after), "Spark did not mark metadata rewrite effective")
    assert(before.toJSON == after.toJSON, "Expected native legacy metadata collision")
    assert(before.output.head.metadata != after.output.head.metadata)

    val old = records(log, "QUERYPROOF_AST")
    assert(old.length == 1 && old.head.length == 7,
      "Expected exactly one intact historical trace event")
    assert(old.head(5) == old.head(6), "Historical event must reproduce collision")

    val complete = records(log, "QUERYPROOF_AST_V8")
    val unknown = records(log, "QUERYPROOF_AST_V8_UNKNOWN")
    assert(unknown.isEmpty, "Supported source frame incorrectly returned UNKNOWN")
    assert(complete.length == 1 && complete.head.length == 9,
      "Expected single complete V8 nine-field effective event")
    val event = complete.head
    assert(event(3).contains("ChangeOnlyMetadata"))
    assert(event(5) == event(6), "V8 should retain original lossy JSON endpoints")
    assert(event(7) != event(8), "V8 source-bound endpoints still collide")
    assert(event(7).contains("alpha") && event(8).contains("beta"))
    assert(event(7).contains("/output/0") && event(8).contains("/output/0"))
    assert(event(7).contains("00000000-0000-0000-0000-000000000001"))
    assert(event(8).contains("00000000-0000-0000-0000-000000000001"))
    println("QUERYPROOF_V8_NATIVE_EVENT_SOURCE_METADATA_COLLISION_REPAIRED")
    println("QUERYPROOF_V8_NATIVE_LEGACY_RECORD_PRESERVED")

    val unsupportedBefore: LogicalPlan =
      Project(Seq(Alias(Literal(1), "v8-original")()), OneRowRelation())
    val (unsupportedAfter, unsupportedLog) =
      evaluate(unsupportedBefore, UnsupportedExecutor)
    assert(!unsupportedBefore.fastEquals(unsupportedAfter),
      "Unsupported counterexample must be effective")
    assert(records(unsupportedLog, "QUERYPROOF_AST_V8").isEmpty,
      "Unsafe acceptance: unsupported Project produced V8 complete event")
    val rejects = records(unsupportedLog, "QUERYPROOF_AST_V8_UNKNOWN")
    assert(rejects.length == 1 && rejects.head.length == 6,
      "Unsupported effective Project must produce exactly one UNKNOWN event")
    assert(rejects.head(5) == "UNKNOWN_PLAN_CLASS_OR_DATA",
      "Wrong unsupported-plan rejection cause: " + rejects.head(5))
    assert(records(unsupportedLog, "QUERYPROOF_AST").length == 1,
      "Legacy source trace must still record unsupported event")
    println("QUERYPROOF_V8_NATIVE_UNSUPPORTED_EFFECTIVE_RULE_FAIL_CLOSED")
    println("QUERYPROOF_V8_NATIVE_EVENT_PROTOTYPE_TEST_COMPLETE")
  }
}
