/* QueryProof V10: source-pinned Spark TreeNode mutation-site evidence.
 *
 * Directly instrument the actual official 4.2.0 source methods
 * TreeNode.setTagValue / unsetTagValue in a disposable CI overlay.
 *
 * (1) Real production ConstantFolding makes a SET even while structurally
 *     ineffective; V9 pre/post observations corroborate it.
 * (2) Deliberately test-only set-then-unset yields two source-site events,
 *     despite equal V9 boundary snapshots.
 * (3) Real bulk copyTagsFrom emits UNKNOWN, proving deliberately unhandled
 *     tag-transfer operations never masquerade as an accepted direct event.
 *
 * This is still an observational prototype, not an AST-to-Coq certificate.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV10TreeNodeMutationWitness {
  object ActualRule extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V10ActualConstantFolding", Once, ConstantFolding))
  }

  object TransientRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case p: Project =>
        val cast = p.projectList.head.collect { case c: Cast => c }.head
        assert(!cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        cast.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
        assert(cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        cast.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
        assert(!cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        plan
      case _ => plan
    }
  }

  object TransientExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V10TransientRule", Once, TransientRule))
  }

  private def makePlan(): (LogicalPlan, Cast) = {
    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val badCast = Cast(Literal("not-a-number"), IntegerType, evalMode = EvalMode.ANSI)
    val plan: LogicalPlan = Project(
      Seq(Alias(If(flag, badCast, Literal(1)), "result")()), LocalRelation(Seq(flag)))
    (plan, badCast)
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
    System.setProperty("queryproof.query.id", "V10_NATIVE_TREENODE_SOURCE_HOOKS")

    val (input, invalidCast) = makePlan()
    assert(!invalidCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val (result, productionLog) = captured { ActualRule.execute(input) }
    assert(result.fastEquals(input))
    assert(invalidCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    assert(records(productionLog, "QUERYPROOF_AST").isEmpty)
    assert(records(productionLog, "QUERYPROOF_AST_V8").isEmpty)
    val productionWrite = records(productionLog, "QUERYPROOF_TAG_MUTATION_V10")
    assert(productionWrite.exists(r =>
      r.length == 8 && r(4) == "SET" && r(5) == "FAILED_TO_EVALUATE" &&
      r(6) == "false" && r(7) == "true"),
      "Actual ConstantFolding write missing from instrumented official TreeNode source")
    val productionV9 = records(productionLog, "QUERYPROOF_RULE_EFFECT_V9")
    assert(productionV9.length == 1 &&
      productionV9.head(6).contains("\"failure_tag_present\":false") &&
      productionV9.head(7).contains("\"failure_tag_present\":true"),
      "Expected agreement with independent pre/post V9 source observation")
    println("QUERYPROOF_V10_ACTUAL_CONSTANTFOLDING_SOURCE_WRITE_CAPTURED")
    println("QUERYPROOF_V10_ACTUAL_RULE_V9_AND_SOURCE_HOOK_AGREE")
    println("QUERYPROOF_V10_ACTUAL_RULE_BULK_UNKNOWN_COUNT=" +
      records(productionLog, "QUERYPROOF_TAG_MUTATION_V10_UNKNOWN").size)

    val (transientPlan, transientCast) = makePlan()
    val (same, transientLog) = captured { TransientExecutor.execute(transientPlan) }
    assert(same.fastEquals(transientPlan))
    assert(!transientCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val writes = records(transientLog, "QUERYPROOF_TAG_MUTATION_V10")
    assert(writes.length == 2, "Real set/unset source sites should produce 2 log events")
    assert(writes.forall(_.length == 8), "Malformed mutation event")
    assert(writes.map(_(4)) == Seq("SET", "UNSET"), "Wrong source mutation order")
    assert(writes.map(_(6)) == Seq("false", "true"), "Wrong before-presence sequence")
    assert(writes.map(_(7)) == Seq("true", "false"), "Wrong after-presence sequence")
    assert(writes(0)(2) == writes(1)(2), "Lost actual node identity for transient writes")
    assert(writes(0)(1).toLong < writes(1)(1).toLong, "Non-monotonic mutation sequence")
    val transientV9 = records(transientLog, "QUERYPROOF_RULE_EFFECT_V9")
    assert(transientV9.length == 1 && transientV9.head(6) == transientV9.head(7),
      "The boundary-snapshot negative control did not reproduce")
    assert(records(transientLog, "QUERYPROOF_AST").isEmpty)
    println("QUERYPROOF_V10_TRANSIENT_MUTATION_HISTORY_RECOVERED")
    println("QUERYPROOF_V10_TRANSIENT_SAME_V9_SNAPSHOTS_CORRECTLY_EXPOSED")

    val (bulkEvents, bulkLog) = captured {
      val source = Cast(Literal("a"), IntegerType, evalMode = EvalMode.ANSI)
      val destination = Cast(Literal("b"), IntegerType, evalMode = EvalMode.ANSI)
      source.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      destination.copyTagsFrom(source)
      assert(destination.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      destination
    }
    assert(bulkEvents.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
    val unknown = records(bulkLog, "QUERYPROOF_TAG_MUTATION_V10_UNKNOWN")
    assert(unknown.exists(r => r.contains("UNKNOWN_BULK_TAG_OPERATION")),
      "Uninstrumented bulk map write must be explicitly UNKNOWN")
    println("QUERYPROOF_V10_BULK_TAG_COPY_FAILS_CLOSED")
    println("QUERYPROOF_V10_NATIVE_TREE_TAG_MUTATION_EXPERIMENT_COMPLETE")
  }
}
