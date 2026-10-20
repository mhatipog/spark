/* QueryProof posthoc V9 negative control: a real Spark mutable tag can be
 * temporarily changed and restored inside a single rule invocation.
 *
 * This rule is DELIBERATELY TEST-ONLY. It does not demonstrate a Spark wrong
 * answer. It falsifies the tempting but invalid inference that equal tracked
 * boundary snapshots imply that no tracked mutation occurred between them.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

object QueryProofNativeV9TransientTagCounterexample {
  object MutateAndRestore extends Rule[LogicalPlan] {
    var observedSet = 0
    var observedClear = 0

    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case p: Project =>
        val casts = p.projectList.head.collect { case c: Cast => c }
        assert(casts.size == 1)
        val expr = casts.head
        assert(!expr.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        expr.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
        assert(expr.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        observedSet += 1
        expr.unsetTagValue(ConstantFolding.FAILED_TO_EVALUATE)
        assert(!expr.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
        observedClear += 1
        plan
      case _ => plan
    }
  }

  object TransientExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V9TransientTagFalsifier", Once, MutateAndRestore))
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
    System.setProperty("queryproof.query.id", "V9_TRANSIENT_SNAPSHOT_COUNTEREXAMPLE")

    val flag = AttributeReference("flag", BooleanType, nullable = false)()
    val badCast = Cast(Literal("not-a-number"), IntegerType, evalMode = EvalMode.ANSI)
    val query: LogicalPlan = Project(
      Seq(Alias(If(flag, badCast, Literal(1)), "result")()),
      LocalRelation(Seq(flag)))
    val original = query.toJSON
    assert(!badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))

    val saved = System.err
    val stream = new ByteArrayOutputStream()
    val sink = new PrintStream(stream, true, "UTF-8")
    val after = try {
      System.setErr(sink)
      TransientExecutor.execute(query)
    } finally {
      sink.flush()
      System.setErr(saved)
      sink.close()
    }
    val text = new String(stream.toByteArray, StandardCharsets.UTF_8)
    assert(MutateAndRestore.observedSet == 1 && MutateAndRestore.observedClear == 1,
      "Control did not actually perform and witness both tag mutations")
    assert(!badCast.containsTag(ConstantFolding.FAILED_TO_EVALUATE),
      "Control did not restore native Spark tag")
    assert(after.fastEquals(query) && after.toJSON == original)
    assert(records(text, "QUERYPROOF_AST").isEmpty)
    assert(records(text, "QUERYPROOF_AST_V8").isEmpty)

    val event = records(text, "QUERYPROOF_RULE_EFFECT_V9")
    assert(event.length == 1 && event.head.length == 10)
    assert(event.head(5) == "false")
    assert(event.head(6) == event.head(7),
      "Pre/post V9 snapshots should collide after transient mutation")
    assert(event.head(6).contains("\"failure_tag_present\":false"))
    assert(event.head(8) == event.head(9))
    assert(records(text, "QUERYPROOF_RULE_EFFECT_V9_UNKNOWN").isEmpty)
    println("QUERYPROOF_V9_TRANSIENT_REAL_TAG_SET_AND_UNSET_OCCURRED")
    println("QUERYPROOF_V9_EQUAL_BOUNDARY_SNAPSHOTS_DESPITE_TRANSIENT_EFFECT")
    println("QUERYPROOF_V9_BOUNDARY_OBSERVATION_NOT_COMPLETE_EFFECT_HISTORY")
  }
}
