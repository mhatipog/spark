/* QueryProof V13 negative: an actual ConstantFolding failure tag survives
 * a test-constructed move from a conditional to an unconditional context.
 * The same untagged/tagged AST then has a different optimization outcome.
 * The relocation itself is adversarial/test-only, not a Spark bug claim.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV13ContextShiftWitness {
  class OnlyConstantFolding extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V13RealConstantFolding", Once, ConstantFolding))
  }

  def capture[T](body: => T): (T, String) = {
    val previous = System.err
    val bytes = new ByteArrayOutputStream()
    val stream = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(stream)
      body
    } finally {
      stream.flush()
      System.setErr(previous)
      stream.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.ast.v9.enabled", "true")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.query.id", "V13_REAL_TAG_CONTEXT_SHIFT")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val bad = Cast(Literal("cannot-parse"), IntegerType, evalMode = EvalMode.ANSI)
    val conditional: LogicalPlan = Project(
      Seq(Alias(If(flag, bad, Literal(1)), "conditional_result")()),
      LocalRelation(Seq(flag)))
    val (folded, naturalLog) = capture {
      new OnlyConstantFolding().execute(conditional)
    }
    assert(folded.fastEquals(conditional))
    assert(bad.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(naturalLog.contains("QUERYPROOF_TAG_MUTATION_V11\t"))
    assert(!naturalLog.contains("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN\t"))
    println("QUERYPROOF_V13_PRODUCTION_CONDITIONAL_SOURCE_TAG_GENERATED")

    // Adversarial construct: move the very same tagged expression into
    // an unconditional position. Do NOT claim Spark does this relocation.
    val unconditional: LogicalPlan = Project(
      Seq(Alias(bad, "unconditional_result")()), LocalRelation(Nil))
    val astBefore = unconditional.toJSON
    val (taggedResult, taggedLog) = capture {
      new OnlyConstantFolding().execute(unconditional)
    }
    assert(taggedResult.fastEquals(unconditional))
    assert(!taggedLog.contains("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN\t"))
    assert(bad.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(unconditional.toJSON == astBefore)
    println("QUERYPROOF_V13_TAGGED_UNCONDITIONAL_OPTIMIZATION_SUCCEEDED")

    // Clear tag outside the RuleExecutor; the source observer correctly
    // reports UNKNOWN for this deliberately unsupported out-of-scope act.
    val (_, resetLog) = capture {
      bad.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
    }
    assert(resetLog.contains("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN\t"))
    assert(!bad.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(unconditional.toJSON == astBefore)
    println("QUERYPROOF_V13_IDENTICAL_STRUCTURAL_AST_DIFFERENT_TAG_STATE")

    var threw = false
    try {
      new OnlyConstantFolding().execute(unconditional)
    } catch {
      case NonFatal(_) => threw = true
    }
    assert(threw, "untagged unconditional ANSI Cast should fail in ConstantFolding")
    assert(unconditional.toJSON == astBefore)
    println("QUERYPROOF_V13_UNTAGGED_SAME_AST_THREW_DURING_OPTIMIZATION")
    println("QUERYPROOF_V13_TEST_ONLY_CONTEXT_RELOCATION_NOT_PRODUCTION_BUG")
    println("QUERYPROOF_V13_HIDDEN_TAG_CAN_AFFECT_RULE_CONTROL_FLOW")
  }
}
