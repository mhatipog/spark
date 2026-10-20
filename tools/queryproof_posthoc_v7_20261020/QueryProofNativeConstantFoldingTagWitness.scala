/* QueryProof Spark 4.2.0 native real ConstantFolding hidden-effect witness.
 *
 * A nonfoldable If condition has a foldable, invalid ANSI cast inside a
 * conditional branch. The production ConstantFolding.tryFold catches the
 * evaluation failure and writes FAILED_TO_EVALUATE to that expression.
 *
 * The purpose is to distinguish an actual production-rule hidden effect
 * from the previous deliberately constructed native test-only rules.
 * This is NOT a correctness bug report against Spark.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeConstantFoldingTagWitness {

  object ActualConstantFoldingExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("NativeRealConstantFolding", Once, ConstantFolding))
  }

  private def withTrace(body: => LogicalPlan): (LogicalPlan, String) = {
    val save = System.err
    val bytes = new ByteArrayOutputStream()
    val recorder = new PrintStream(bytes, true, "UTF-8")
    System.setErr(recorder)
    try {
      val plan = body
      recorder.flush()
      (plan, new String(bytes.toByteArray, StandardCharsets.UTF_8))
    } finally {
      System.setErr(save)
      recorder.close()
    }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.query.id", "NATIVE_REAL_CONSTANT_FOLDING")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val badCast = Cast(Literal("not-a-number"), IntegerType,
      evalMode = EvalMode.ANSI)
    assert(badCast.foldable)
    assert(!badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val conditional = If(flag, badCast, Literal(1))
    assert(!conditional.foldable)
    val query = Project(Seq(Alias(conditional, "result")()),
      LocalRelation(Seq(flag)))
    val before = query.toJSON
    val (result, eventTrace) = withTrace {
      ActualConstantFoldingExecutor.execute(query)
    }

    assert(badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE),
      "The actual Spark ConstantFolding rule never recorded the failed cast")
    assert(result.toJSON == before,
      "This witness requires exactly unchanged serialized Catalyst AST")
    assert(!eventTrace.contains("QUERYPROOF_AST\t"),
      "A structurally unchanged effective-rule stream must not claim capture")
    println("QUERYPROOF_NATIVE_ACTUAL_CONSTANT_FOLDING_SETS_UNOBSERVED_FAILURE_TAG")

    val positive = Project(Seq(Alias(Add(Literal(1), Literal(2)), "result")()),
      OneRowRelation())
    val positiveBefore = positive.toJSON
    val (positiveResult, positiveTrace) = withTrace {
      ActualConstantFoldingExecutor.execute(positive)
    }
    assert(positiveResult.toJSON != positiveBefore,
      "Positive folding control should structurally rewrite the plan")
    assert(positiveTrace.contains("QUERYPROOF_AST\t"),
      "Instrumented RuleExecutor did not capture real positive folding")
    println("QUERYPROOF_NATIVE_REAL_CONSTANT_FOLDING_POSITIVE_CONTROL_CAPTURED")
  }
}
