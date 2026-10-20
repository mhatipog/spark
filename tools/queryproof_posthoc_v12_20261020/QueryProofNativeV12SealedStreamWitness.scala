/* QueryProof V12: real ConstantFolding + nested tag effects in one JVM.
 * This records a narrow source-event transcript and seals it after the last
 * invocation. It does not authenticate its own logs or prove SQL semantics.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.QueryProofTreeNodeTagEffects
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV12SealedStreamWitness {
  private def makePlan(): (LogicalPlan, Cast) = {
    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val bad = Cast(Literal("not-a-number"), IntegerType, evalMode = EvalMode.ANSI)
    val plan: LogicalPlan = Project(
      Seq(Alias(If(flag, bad, Literal(1)), "result")()), LocalRelation(Seq(flag)))
    (plan, bad)
  }

  object Production extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12Production", Once, ConstantFolding))
  }

  private def castOf(plan: LogicalPlan): Cast = plan match {
    case p: Project => p.projectList.head.collect { case c: Cast => c }.head
    case _ => throw new IllegalArgumentException("expected Project")
  }

  object InnerRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val c = castOf(plan)
      assert(c.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      c.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
      c.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      plan
    }
  }
  object Inner extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12InnerBatch", Once, InnerRule))
  }
  object OuterRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val c = castOf(plan)
      assert(!c.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      c.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      assert(Inner.execute(plan).fastEquals(plan))
      assert(c.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      c.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
      plan
    }
  }
  object Outer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12OuterBatch", Once, OuterRule))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.ast.v9.enabled", "true")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.query.id", "V12_NATIVE_NARROW_SEALED_STREAM")

    val saved = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    try {
      System.setErr(sink)
      val (production, bad) = makePlan()
      assert(Production.execute(production).fastEquals(production))
      assert(bad.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      val (nested, nestedBad) = makePlan()
      assert(Outer.execute(nested).fastEquals(nested))
      assert(!nestedBad.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      QueryProofTreeNodeTagEffects.seal()
    } finally {
      sink.flush()
      System.setErr(saved)
      sink.close()
    }

    // Select precisely the source-logger channel; V9 and Spark diagnostics
    // were never in the V12 checksum domain and must not be represented as such.
    val recorded = new String(bytes.toByteArray, StandardCharsets.UTF_8)
    val sourceLines = recorded.linesIterator.filter { line =>
      line.startsWith("QUERYPROOF_RULE_SCOPE_V11_") ||
      line.startsWith("QUERYPROOF_TAG_MUTATION_V11") ||
      line.startsWith("QUERYPROOF_V12_")
    }.toVector
    assert(sourceLines.size == 12, "expected 11 V11 events and one V12 seal")
    assert(sourceLines.last.startsWith("QUERYPROOF_V12_SEAL\t"))
    val stream = sourceLines.mkString("", "\n", "\n")
    println("QUERYPROOF_V12_STREAM_B64=" +
      Base64.getEncoder.encodeToString(stream.getBytes(StandardCharsets.UTF_8)))
    println("QUERYPROOF_V12_REAL_PRODUCTION_AND_NESTED_STREAM_CAPTURED")
  }
}
