"""V9: independent pre-rule snapshot overlay for production ConstantFolding tag.

V8 observes replacement metadata after a rule returns. V9 tests a distinct
effect: Spark ConstantFolding mutates FAILED_TO_EVALUATE in place without
an effective structural rewrite. Attach a pre-rule snapshot to each invocation
and emit even when RuleExecutor.fastEquals says the plan is unchanged.

These are observational test events for a ONE-CAST Project fragment, never
general-purpose proof certificates; any other source grammar is UNKNOWN.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
BEFORE_ANCHOR = (
    "            val startTime = System.nanoTime()\n"
    "            val result = if (\n"
)
AFTER_ANCHOR = "            val effective = !result.fastEquals(plan)\n"
INSERT_ANCHOR = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"

V9_OBJECT = r'''
/** Posthoc V9 test only: a pre-/post-invocation source tag observation.
 * It NEVER denotes semantic preservation. It inspects exactly one Cast in
 * a Project, and only ConstantFolding's documented failure tag.
 */
private[rules] object QueryProofAstTraceV9 {
  import org.json4s.JsonAST._
  import org.json4s.jackson.JsonMethods.{compact, render}
  import org.apache.spark.sql.catalyst.expressions.Cast
  import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
  import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

  sealed trait BeforeState
  case object Disabled extends BeforeState
  final case class Captured(frame: String) extends BeforeState
  final case class Rejected(reason: String) extends BeforeState

  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val encode = java.util.Base64.getEncoder
  private def b64(value: String): String =
    encode.encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  private def safeReason(e: Throwable): String = e match {
    case a: IllegalArgumentException
        if a.getMessage != null && a.getMessage.startsWith("UNKNOWN_") =>
      a.getMessage
    case _ => "UNKNOWN_CAPTURE_EXCEPTION_" + e.getClass.getSimpleName
  }

  /** Reads one actual Catalyst expression tag, not a copied string-map mock. */
  private def frame(plan: LogicalPlan): String = plan match {
    case project: Project if project.projectList.size == 1 =>
      val casts = project.projectList.head.collect { case c: Cast => c }
      if (casts.size != 1) {
        throw new IllegalArgumentException("UNKNOWN_CAST_COUNT")
      }
      val cast = casts.head
      compact(render(JObject(List(
        "schema" -> JString("queryproof.v9.production-folding-tag.v1"),
        "source_plan_class" -> JString(project.getClass.getName),
        "cast_path" -> JString("/project_list/0/cast/0"),
        "source_cast_class" -> JString(cast.getClass.getName),
        "cast_data_type" -> JString(cast.dataType.json),
        "failure_tag_present" ->
          JBool(cast.containsTag(ConstantFolding.FAILED_TO_EVALUATE))))))
    case _ =>
      throw new IllegalArgumentException("UNKNOWN_PLAN_CLASS")
  }

  def begin[TreeType <: TreeNode[_]](plan: TreeType): BeforeState = {
    if (!java.lang.Boolean.getBoolean("queryproof.ast.v9.enabled")) {
      Disabled
    } else {
      plan match {
        case p: LogicalPlan =>
          try { Captured(frame(p)) }
          catch { case scala.util.control.NonFatal(e) => Rejected(safeReason(e)) }
        case _ => Rejected("UNKNOWN_NON_LOGICAL_PLAN")
      }
    }
  }

  def finish[TreeType <: TreeNode[_]](
      executor: String,
      batch: String,
      rule: String,
      iteration: Int,
      beforeState: BeforeState,
      before: TreeType,
      after: TreeType,
      effective: Boolean): Unit = {
    if (beforeState == Disabled) return
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    val prefix = Seq(query, executor, batch, rule, iteration.toString, effective.toString)
    val ordinal = seen.getAndIncrement()
    if (ordinal == 2500) {
      System.err.println("QUERYPROOF_RULE_EFFECT_V9_UNKNOWN\t" +
        (prefix :+ "UNKNOWN_EVENT_BUDGET_EXCEEDED").map(b64).mkString("\t"))
    } else if (ordinal < 2500) {
      (beforeState, before, after) match {
        case (Captured(pre), b: LogicalPlan, a: LogicalPlan) =>
          try {
            val post = frame(a)
            val fields = prefix ++ Seq(pre, post, b.toJSON, a.toJSON)
            System.err.println("QUERYPROOF_RULE_EFFECT_V9\t" +
              fields.map(b64).mkString("\t"))
          } catch {
            case scala.util.control.NonFatal(e) =>
              System.err.println("QUERYPROOF_RULE_EFFECT_V9_UNKNOWN\t" +
                (prefix :+ safeReason(e)).map(b64).mkString("\t"))
          }
        case (Rejected(why), _, _) =>
          System.err.println("QUERYPROOF_RULE_EFFECT_V9_UNKNOWN\t" +
            (prefix :+ why).map(b64).mkString("\t"))
        case _ =>
          System.err.println("QUERYPROOF_RULE_EFFECT_V9_UNKNOWN\t" +
            (prefix :+ "UNKNOWN_INCONSISTENT_PRE_POST").map(b64).mkString("\t"))
      }
    }
  }
}

'''

def patch_text(original: str) -> str:
    if original.count("QueryProofAstTraceV8.capture(") != 1:
        raise ValueError("V9 requires disposable V8-installed trace overlay")
    if "QUERYPROOF_RULE_EFFECT_V9" in original:
        raise ValueError("V9 was already instrumented")
    for anchor in (BEFORE_ANCHOR, AFTER_ANCHOR, INSERT_ANCHOR):
        if original.count(anchor) != 1:
            raise ValueError("V9 source anchor changed: " + repr(anchor))
    patched = original.replace(
        BEFORE_ANCHOR,
        "            val startTime = System.nanoTime()\n"
        "            val qpV9Before = QueryProofAstTraceV9.begin(plan)\n"
        "            val result = if (\n",
    )
    patched = patched.replace(
        AFTER_ANCHOR,
        AFTER_ANCHOR +
        "            QueryProofAstTraceV9.finish(name, batch.name, rule.ruleName, "
        "iteration, qpV9Before, plan, result, effective)\n",
    )
    patched = patched.replace(INSERT_ANCHOR, V9_OBJECT + INSERT_ANCHOR)
    assert patched.count("QueryProofAstTraceV9.finish(") == 1
    return patched


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V9_EPHEMERAL_PRE_RULE_PRODUCTION_TAG_CAPTURE_READY")
