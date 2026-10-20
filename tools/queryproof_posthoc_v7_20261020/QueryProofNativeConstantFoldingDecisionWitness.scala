/* QueryProof: posthoc native production-ConstantFolding tag-dependence witness.
 *
 * This test executes the official Spark 4.2.0 ConstantFolding rule without
 * modifying that rule. It asks a narrower question than SQL equivalence:
 * does the real FAILED_TO_EVALUATE tag alter a subsequent folding decision?
 *
 * Artificial CountingFailure only instruments evaluation attempts. It is
 * deliberately NOT evidence of a Spark query-answer correctness bug.
 * Each call uses a FRESH Project root to rule out stale RuleId skip caches.
 * The frozen W32 and X evidence are entirely independent of this experiment.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

object QueryProofNativeConstantFoldingDecisionWitness {
  val evaluations = new AtomicInteger(0)

  case class CountingFailure() extends LeafExpression with CodegenFallback {
    override def nullable: Boolean = false
    override def dataType: DataType = IntegerType
    override def foldable: Boolean = true

    override def eval(input: InternalRow): Any = {
      evaluations.incrementAndGet()
      throw new IllegalArgumentException("QueryProof intentionally failing fold control")
    }
  }

  object ActualConstantFoldingExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("ActualConstantFoldingDecisionWitness", Once, ConstantFolding))
  }

  private def newPlan(flag: AttributeReference, expr: CountingFailure): LogicalPlan = {
    val conditional = If(flag, expr, Literal(1))
    assert(!conditional.foldable)
    Project(Seq(Alias(conditional, "result")()), LocalRelation(Seq(flag)))
  }

  private def runFresh(flag: AttributeReference, expr: CountingFailure): (String, String) = {
    val plan = newPlan(flag, expr)
    val before = plan.toJSON
    val oldErr = System.err
    val bytes = new ByteArrayOutputStream()
    val recorder = new PrintStream(bytes, true, "UTF-8")
    System.setErr(recorder)
    try {
      val result = ActualConstantFoldingExecutor.execute(plan)
      recorder.flush()
      (before + "\n" + result.toJSON,
        new String(bytes.toByteArray, StandardCharsets.UTF_8))
    } finally {
      System.setErr(oldErr)
      recorder.close()
    }
  }

  private def unchangedAndUntraced(snapshot: String, trace: String): Unit = {
    val split = snapshot.split("\n", 2)
    assert(split.length == 2 && split(0) == split(1),
      "Actual ConstantFolding unexpectedly changed the serialized logical plan")
    assert(!trace.contains("QUERYPROOF_AST\t"),
      "A tag-only effective step unexpectedly emitted a structural AST event")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.query.id", "NATIVE_REAL_CF_TAG_DECISION")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val failing = CountingFailure()

    // First real rule invocation: failed evaluation sets the production tag.
    val (firstAst, firstTrace) = runFresh(flag, failing)
    unchangedAndUntraced(firstAst, firstTrace)
    val initialAttempts = evaluations.get()
    assert(initialAttempts > 0, "Initial untagged expression was never evaluated")
    assert(failing.containsTag(ConstantFolding.FAILED_TO_EVALUATE),
      "Real production ConstantFolding failed to set its failure tag")
    println(s"QUERYPROOF_CF_INITIAL_EVAL_ATTEMPTS=$initialAttempts")

    // Second rule invocation: FRESH logical-plan nodes but REUSED tagged
    // expression. A rule-cache-only explanation cannot account for this.
    val (secondAst, secondTrace) = runFresh(flag, failing)
    unchangedAndUntraced(secondAst, secondTrace)
    assert(evaluations.get() == initialAttempts,
      "Production FAILED_TO_EVALUATE did not suppress the next evaluation attempt")
    println("QUERYPROOF_CF_TAGGED_FRESH_PLAN_SKIPS_EVALUATION")

    // Causal negative control: remove precisely the production tag, construct
    // yet another fresh plan and confirm the attempted fold returns.
    failing.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
    assert(!failing.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val (thirdAst, thirdTrace) = runFresh(flag, failing)
    unchangedAndUntraced(thirdAst, thirdTrace)
    assert(evaluations.get() > initialAttempts,
      "Removing the failure tag did not restore the fold attempt")
    val afterReset = evaluations.get()
    println(s"QUERYPROOF_CF_UNSET_TAG_RESTORES_EVALUATION_ATTEMPTS=$afterReset")

    // Independent fresh-expression control, same observed SQL type and rule.
    val (freshAst, freshTrace) = runFresh(flag, CountingFailure())
    unchangedAndUntraced(freshAst, freshTrace)
    assert(evaluations.get() > afterReset,
      "A fresh untagged expression failed to attempt evaluation")
    println("QUERYPROOF_CF_FRESH_UNTAGGED_CONTROL_ATTEMPTS_EVALUATION")
    println("QUERYPROOF_CF_REAL_RULE_TAG_CHANGES_NEXT_RULE_DECISION_CONFIRMED")
  }
}
