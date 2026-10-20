/* V13B: preserve failed V13 run; remove per-node rule-pruning-cache
 * confound by comparing fresh, isomorphic, never-before-optimized ASTs.
 * Same serialized structure and Alias exprId; one actual production
 * failure-tag generated in conditional context, one fresh untagged Cast.
 * The context relocation is TEST-ONLY. No Spark production bug claim.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV13BFreshContextWitness {
  class Folding extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V13BFreshRule", Once, ConstantFolding))
  }
  def captured[T](body: => T): (T, String) = {
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
    System.setProperty("queryproof.query.id", "V13B_FRESH_ISOMORPHIC_CONTEXT")
    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val naturallyTagged = Cast(
      Literal("cannot-parse"), IntegerType, evalMode = EvalMode.ANSI)
    val conditional: LogicalPlan = Project(
      Seq(Alias(If(flag, naturallyTagged, Literal(1)), "conditional_result")()),
      LocalRelation(Seq(flag)))
    val (result, sourceLog) = captured {
      new Folding().execute(conditional)
    }
    assert(result.fastEquals(conditional))
    assert(naturallyTagged.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(sourceLog.contains("QUERYPROOF_TAG_MUTATION_V11\t"))
    println("QUERYPROOF_V13B_TAG_CREATED_BY_REAL_CONSTANTFOLDING")

    // Both ASTs below are newly constructed. Do not optimize either
    // before beginning comparison; each has its own TreeNode rule cache.
    val freshUntagged = Cast(
      Literal("cannot-parse"), IntegerType, evalMode = EvalMode.ANSI)
    assert(!freshUntagged.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val commonId = NamedExpression.newExprId
    val taggedPlan: LogicalPlan = Project(
      Seq(Alias(naturallyTagged, "same")(exprId = commonId)), LocalRelation(Nil))
    val untaggedPlan: LogicalPlan = Project(
      Seq(Alias(freshUntagged, "same")(exprId = commonId)), LocalRelation(Nil))
    val canonical = taggedPlan.toJSON
    assert(untaggedPlan.toJSON == canonical, "AST representation unexpectedly shows tags")
    println("QUERYPROOF_V13B_FRESH_ISOMORPHIC_SERIALIZED_AST")

    val (taggedAfter, taggedLog) = captured {
      new Folding().execute(taggedPlan)
    }
    assert(taggedAfter.fastEquals(taggedPlan))
    assert(taggedLog.contains("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t"))
    assert(naturallyTagged.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    println("QUERYPROOF_V13B_TAGGED_FRESH_AST_OPTIMIZED_SUCCESSFULLY")

    val (threw, untaggedLog) = captured {
      try {
        new Folding().execute(untaggedPlan)
        false
      } catch {
        case NonFatal(_) => true
      }
    }
    assert(untaggedLog.contains("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t"))
    assert(threw, "fresh untagged unconditional ANSI Cast unexpectedly did not throw")
    assert(taggedPlan.toJSON == canonical)
    assert(untaggedPlan.toJSON == canonical)
    println("QUERYPROOF_V13B_UNTAGGED_FRESH_AST_THREW_DURING_OPTIMIZATION")
    println("QUERYPROOF_V13B_SAME_AST_DIFFERENT_RULE_OUTCOME_AFTER_CACHE_CONTROL")
    println("QUERYPROOF_V13B_TEST_ONLY_RELOCATION_NOT_SPARK_WRONG_ANSWER")
  }
}
