/* QueryProof V9 native falsification: source-tag side effects on an
 * ineffective real optimizer invocation, not merely a test-only rule.
 *
 * Official Spark ConstantFolding writes FAILED_TO_EVALUATE on a Cast in
 * place, then returns the structurally same Project. V8's event gate sees
 * nothing. V9 captures its pre-rule value BEFORE evaluation and records
 * the changed tag AFTER evaluation, independently of fastEquals.
 *
 * V9 proves observation of this one tag under this test grammar only.
 * Arbitrary plan states, transients and uninstrumented effects stay UNKNOWN.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV9PreRuleProductionTagWitness {
  object FoldingExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V9RealConstantFolding", Once, ConstantFolding))
  }

  private def run(plan: LogicalPlan): (LogicalPlan, String) = {
    val saved = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      FoldingExecutor.execute(plan)
    } finally {
      sink.flush()
      System.setErr(saved)
      sink.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  private def records(log: String, marker: String): Seq[Array[String]] =
    log.split("\n").filter(_.startsWith(marker + "\t")).toSeq.map { line =>
      line.trim.split("\t").drop(1).map(part =>
        new String(Base64.getDecoder.decode(part), StandardCharsets.UTF_8))
    }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.ast.v9.enabled", "true")
    System.setProperty("queryproof.query.id", "V9_NATIVE_REAL_CONSTANT_FOLDING_TAG")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val badCast = Cast(Literal("not-a-number"), IntegerType,
      evalMode = EvalMode.ANSI)
    assert(badCast.foldable)
    assert(!badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val condition = If(flag, badCast, Literal(1))
    assert(!condition.foldable)
    val query: LogicalPlan = Project(Seq(Alias(condition, "result")()),
      LocalRelation(Seq(flag)))
    val originalJson = query.toJSON
    val (after, events) = run(query)

    assert(badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE),
      "Production ConstantFolding did not set expected failure tag")
    assert(after.fastEquals(query), "Unexpected changed source plan")
    assert(after.toJSON == originalJson,
      "This control requires unchanged official toJSON")
    assert(records(events, "QUERYPROOF_AST").isEmpty,
      "Historical effective-only tracer unexpectedly captured tag effect")
    assert(records(events, "QUERYPROOF_AST_V8").isEmpty,
      "V8 effective-only event unexpectedly captured hidden tag effect")
    assert(records(events, "QUERYPROOF_AST_V8_UNKNOWN").isEmpty,
      "V8 was never invoked for structurally ineffective step")

    val v9 = records(events, "QUERYPROOF_RULE_EFFECT_V9")
    assert(v9.length == 1 && v9.head.length == 10,
      "V9 must record exactly one pre/post invocation, even if ineffective")
    assert(v9.head(3).contains("ConstantFolding"))
    assert(v9.head(5) == "false", "Actual Spark should report structural ineffectiveness")
    assert(v9.head(6) != v9.head(7), "V9 tag-frame before/after still collide")
    assert(v9.head(6).contains("\"failure_tag_present\":false"))
    assert(v9.head(7).contains("\"failure_tag_present\":true"))
    assert(v9.head(8) == v9.head(9), "Source JSON should remain unchanged")
    assert(records(events, "QUERYPROOF_RULE_EFFECT_V9_UNKNOWN").isEmpty)
    println("QUERYPROOF_V9_NATIVE_REAL_INEFFECTIVE_RULE_TAG_EFFECT_CAPTURED")
    println("QUERYPROOF_V9_NATIVE_PRE_RULE_SNAPSHOT_NECESSARY_FOR_TAG_EVIDENCE")

    val positive: LogicalPlan =
      Project(Seq(Alias(Add(Literal(1), Literal(2)), "result")()), OneRowRelation())
    val (positiveAfter, positiveLog) = run(positive)
    assert(!positiveAfter.fastEquals(positive))
    assert(records(positiveLog, "QUERYPROOF_AST").length == 1,
      "Historical positive structural event absent")
    assert(records(positiveLog, "QUERYPROOF_AST_V8").isEmpty,
      "V8 must never accept out-of-fragment Project")
    assert(records(positiveLog, "QUERYPROOF_AST_V8_UNKNOWN").length == 1,
      "V8 out-of-fragment effective rule did not fail closed")
    assert(records(positiveLog, "QUERYPROOF_RULE_EFFECT_V9").isEmpty,
      "V9 must never accept Project with no Cast")
    val rejects = records(positiveLog, "QUERYPROOF_RULE_EFFECT_V9_UNKNOWN")
    assert(rejects.length == 1 && rejects.head.length == 7)
    assert(rejects.head(6) == "UNKNOWN_CAST_COUNT")
    println("QUERYPROOF_V9_NATIVE_UNSUPPORTED_REAL_RULE_FAILS_CLOSED")
    println("QUERYPROOF_V9_NATIVE_SOURCE_TAG_EFFECT_EXPERIMENT_COMPLETE")
  }
}
