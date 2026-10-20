/* V14: isolate and directly observe the suspected V13 cached-rule confound.
 * Real source-pinned TreeNode._ineffectiveRules MARK and SKIP events.
 * This test reuses a plan intentionally, then controls with a fresh
 * structurally identical plan. Context relocation is test-only.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV14RulePruningCacheWitness {
  class Folding extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V14SingleRealFoldingRule", Once, ConstantFolding))
  }
  def captured[T](body: => T): (T, String) = {
    val previous = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      body
    } finally {
      sink.flush()
      System.setErr(previous)
      sink.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }
  def cacheEvents(log: String): Seq[Seq[String]] = {
    log.linesIterator
      .filter(_.startsWith("QUERYPROOF_RULE_PRUNING_V14\t"))
      .map(_.split("\t").drop(1).toSeq.map { s =>
        new String(Base64.getDecoder.decode(s), StandardCharsets.UTF_8)
      }).toSeq
  }
  def projectEvents(log: String, kind: String): Seq[Seq[String]] =
    cacheEvents(log).filter(r =>
      r.length == 5 &&
      r(0) == "V14_RULE_CACHE_REPLAY" &&
      r(1) == kind &&
      r(3).endsWith(".Project"))

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.ast.v9.enabled", "true")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.rule.pruning.v14.enabled", "true")
    System.setProperty("queryproof.query.id", "V14_RULE_CACHE_REPLAY")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val failedCast = Cast(
      Literal("cannot-parse"), IntegerType, evalMode = EvalMode.ANSI)
    val conditional: LogicalPlan = Project(
      Seq(Alias(If(flag, failedCast, Literal(1)), "conditional")()),
      LocalRelation(Seq(flag)))
    val (conditionalAfter, sourceLog) = captured {
      new Folding().execute(conditional)
    }
    assert(conditionalAfter.fastEquals(conditional))
    assert(failedCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(sourceLog.contains("QUERYPROOF_TAG_MUTATION_V11\t"))
    println("QUERYPROOF_V14_PRODUCTION_FAILED_TAG_GENERATED")

    val commonId = NamedExpression.newExprId
    val reused: LogicalPlan = Project(
      Seq(Alias(failedCast, "unconditional")(exprId = commonId)),
      LocalRelation(Nil))
    val serial = reused.toJSON
    val (first, firstLog) = captured {
      new Folding().execute(reused)
    }
    assert(first.fastEquals(reused))
    val marks = projectEvents(firstLog, "MARK")
    assert(marks.nonEmpty, "first tagged Project did not mark an ineffective rule")
    println("QUERYPROOF_V14_REAL_PROJECT_RULE_CACHE_MARK_OBSERVED")

    val (_, outsideLog) = captured {
      failedCast.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
    }
    assert(outsideLog.contains("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN\t"))
    assert(!failedCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(reused.toJSON == serial)
    println("QUERYPROOF_V14_TAG_UNSET_INVISIBLE_TO_STRUCTURE_UNKNOWN_EVENT")

    val (reusedThrew, reuseLog) = captured {
      try {
        new Folding().execute(reused)
        false
      } catch {
        case NonFatal(_) => true
      }
    }
    assert(!reusedThrew, "reused Project unexpectedly reevaluated invalid Cast")
    assert(reuseLog.contains("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t"))
    val skips = projectEvents(reuseLog, "SKIP")
    assert(skips.nonEmpty, "no source-level Project rule-cache hit on reuse")
    assert(marks.exists(mark =>
      skips.exists(skip => mark(2) == skip(2) && mark(4) == skip(4))),
      "could not match MARK and SKIP to same rule and exact Project identity")
    println("QUERYPROOF_V14_SAME_PROJECT_SAME_RULE_CACHE_MARK_THEN_SKIP")

    val freshBad = Cast(
      Literal("cannot-parse"), IntegerType, evalMode = EvalMode.ANSI)
    val fresh: LogicalPlan = Project(
      Seq(Alias(freshBad, "unconditional")(exprId = commonId)),
      LocalRelation(Nil))
    assert(fresh.toJSON == serial)
    val (freshThrew, freshLog) = captured {
      try {
        new Folding().execute(fresh)
        false
      } catch {
        case NonFatal(_) => true
      }
    }
    assert(freshLog.contains("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t"))
    assert(freshThrew, "fresh identical untagged Project should attempt ANSI Cast")
    assert(fresh.toJSON == serial)
    println("QUERYPROOF_V14_FRESH_IDENTICAL_AST_NOT_PRUNED_THREW")
    println("QUERYPROOF_V14_V13_FIRST_RUN_CACHE_CONFOUND_EXPLAINED")
    println("QUERYPROOF_V14_TEST_ONLY_CONTEXT_RELOCATION_NO_SPARK_WRONG_ANSWER_CLAIM")
  }
}
