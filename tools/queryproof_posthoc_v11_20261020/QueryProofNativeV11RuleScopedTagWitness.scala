/* QueryProof V11: source-bound tag events attributed to real optimizer
 * invocations, with independent nested/out-of-scope adversarial controls.
 *
 * Official Spark 4.2.0 TreeNode.scala is pinned before disposable patching.
 * This establishes a trustworthy experimental association for these logged
 * mutations; it is NOT a complete certified Spark rule-effect calculus.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV11RuleScopedTagWitness {
  private def makePlan(): (LogicalPlan, Cast) = {
    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val bad = Cast(Literal("not-a-number"), IntegerType, evalMode = EvalMode.ANSI)
    val p: LogicalPlan = Project(
      Seq(Alias(If(flag, bad, Literal(1)), "result")()), LocalRelation(Seq(flag)))
    (p, bad)
  }

  object ActualFolding extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V11ProductionRule", Once, ConstantFolding))
  }

  private def onlyCast(plan: LogicalPlan): Cast = plan match {
    case p: Project => p.projectList.head.collect { case c: Cast => c }.head
    case _ => throw new IllegalArgumentException("expected one-Cast Project")
  }

  object NestedRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val cast = onlyCast(plan)
      assert(cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      cast.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
      cast.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      plan
    }
  }
  object NestedExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V11Inner", Once, NestedRule))
  }
  object OuterRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val cast = onlyCast(plan)
      assert(!cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      cast.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      val innerResult = NestedExecutor.execute(plan)
      assert(innerResult.fastEquals(plan))
      assert(cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      cast.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
      plan
    }
  }
  object OuterExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V11Outer", Once, OuterRule))
  }

  private def captured[T](body: => T): (T, String) = {
    val saved = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      body
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
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.query.id", "V11_ACTUAL_SOURCE_RULE_SCOPES")

    val (production, productionCast) = makePlan()
    val (productionResult, productionLog) = captured { ActualFolding.execute(production) }
    assert(productionResult.fastEquals(production))
    assert(productionCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val ruleBegin = records(productionLog, "QUERYPROOF_RULE_SCOPE_V11_BEGIN")
    val ruleEnd = records(productionLog, "QUERYPROOF_RULE_SCOPE_V11_END")
    val prodWrites = records(productionLog, "QUERYPROOF_TAG_MUTATION_V11")
    assert(ruleBegin.length == 1 && ruleEnd.length == 1)
    assert(ruleBegin.head.length == 6 && ruleBegin.head(4).contains("ConstantFolding"))
    assert(ruleBegin.head(1) == ruleEnd.head(1), "Lost invocation pairing")
    assert(prodWrites.exists(r =>
      r.length == 9 && r(4) == ruleBegin.head(1) &&
      r(5) == "SET" && r(6) == "FAILED_TO_EVALUATE" &&
      r(7) == "false" && r(8) == "true"),
      "Production source mutation not attributed to its exact rule invocation")
    assert(records(productionLog, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN").isEmpty,
      "Uncovered production tag path must block proof acceptance")
    assert(records(productionLog, "QUERYPROOF_AST").isEmpty)
    val v9 = records(productionLog, "QUERYPROOF_RULE_EFFECT_V9")
    assert(v9.length == 1 && v9.head(6) != v9.head(7))
    println("QUERYPROOF_V11_ACTUAL_CONSTANTFOLDING_RULE_IDENTITY_BOUND")
    println("QUERYPROOF_V11_PRODUCTION_RULE_TAG_EFFECT_COMPLETE_IN_TINY_DOMAIN")

    val (nestedPlan, nestedCast) = makePlan()
    val (nestedAfter, nestedLog) = captured { OuterExecutor.execute(nestedPlan) }
    assert(nestedAfter.fastEquals(nestedPlan))
    assert(!nestedCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val begins = records(nestedLog, "QUERYPROOF_RULE_SCOPE_V11_BEGIN")
    val ends = records(nestedLog, "QUERYPROOF_RULE_SCOPE_V11_END")
    assert(begins.length == 2 && ends.length == 2)
    assert(begins.head(1) != begins(1)(1), "Nested rules need distinct invocation IDs")
    assert(begins.head(4).contains("OuterRule") && begins(1)(4).contains("NestedRule"))
    assert(ends.map(_(1)) == Seq(begins(1)(1), begins.head(1)),
      "Nested invocations did not close in stack order")
    val writes = records(nestedLog, "QUERYPROOF_TAG_MUTATION_V11")
    assert(writes.length == 4 && writes.forall(_.length == 9))
    assert(writes.map(_(5)) == Seq("SET", "UNSET", "SET", "UNSET"))
    assert(writes.map(_(4)) ==
      Seq(begins.head(1), begins(1)(1), begins(1)(1), begins.head(1)),
      "Source mutations were assigned to the wrong nested invocation")
    assert(writes.map(_(2)).distinct.size == 1, "Same Cast lost JVM-local identity")
    assert(writes.map(_(1).toLong) == writes.map(_(1).toLong).sorted,
      "Mutation sequence was not monotone")
    val nestedV9 = records(nestedLog, "QUERYPROOF_RULE_EFFECT_V9")
    assert(nestedV9.length == 2 && nestedV9.forall(r => r(6) == r(7)),
      "Boundary model must remain blind to write-and-restore")
    assert(records(nestedLog, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN").isEmpty)
    println("QUERYPROOF_V11_NESTED_INVOCATION_LIFO_MUTATION_ATTRIBUTION_CONFIRMED")

    val (_, outsideLog) = captured {
      val unrelatedCast = Cast(Literal("outside"), IntegerType, evalMode = EvalMode.ANSI)
      unrelatedCast.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
    }
    assert(records(outsideLog, "QUERYPROOF_TAG_MUTATION_V11").isEmpty,
      "Tag write without a rule invocation must never count as supported event")
    val rejects = records(outsideLog, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN")
    assert(rejects.length == 1 && rejects.head.length == 10)
    assert(rejects.head(4) == "UNATTRIBUTED" &&
      rejects.head(9) == "UNKNOWN_OUTSIDE_RULE_SCOPE")
    println("QUERYPROOF_V11_OUT_OF_RULE_MUTATION_FAILS_CLOSED")
    println("QUERYPROOF_V11_REAL_RULE_SCOPED_EFFECT_LEDGER_TEST_COMPLETE")
  }
}
