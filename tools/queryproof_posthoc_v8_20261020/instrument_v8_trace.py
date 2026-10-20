"""Posthoc V8: overlay-only Spark source-bound trace instrumentation.

Do not edit the historical instrumented RuleExecutor or any historical W32/X
fixture. The CI runner first installs the pinned scope instrumentation, then
this patch changes only the disposable checkout copy.

V8 emits a distinct marker; legacy QUERYPROOF_AST events remain unchanged.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

CAPTURE_ANCHOR = (
    "              QueryProofAstTrace.capture("
    "name, batch.name, rule.ruleName, iteration, plan, result)\n"
)
INSERT_ANCHOR = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"

V8_OBJECT = r'''
/** Posthoc V8 observational test only; not a machine-checked SQL certificate.
 * The complete event is emitted ONLY for the explicitly delimited
 * empty, non-streaming LocalRelation/AttributeReference fragment.
 * The first UNKNOWN or event-budget exhaustion invalidates trace acceptance.
 */
private[rules] object QueryProofAstTraceV8 {
  import org.json4s.JsonAST._
  import org.json4s.jackson.JsonMethods.{compact, render}
  import org.apache.spark.sql.catalyst.expressions.AttributeReference
  import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}

  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val encode = java.util.Base64.getEncoder
  private def b64(value: String): String =
    encode.encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  /** A narrow, source-attached frame. Does not claim to include mutable tags,
   * cached rule state, data values, or changes invisible to fastEquals.
   */
  private def frame(plan: LogicalPlan): String = plan match {
    case relation: LocalRelation if !relation.isStreaming && relation.data.isEmpty =>
      if (relation.output.isEmpty || relation.output.size > 64) {
        throw new IllegalArgumentException("UNKNOWN_ATTRIBUTE_COUNT")
      }
      val outputs = relation.output.zipWithIndex.map {
        case (a: AttributeReference, index) =>
          val metadata = a.metadata.json
          val dataType = a.dataType.json
          if (metadata.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65536) {
            throw new IllegalArgumentException("UNKNOWN_OVERSIZED_METADATA")
          }
          if (a.name.length > 1024 || dataType.length > 16384 ||
              a.qualifier.size > 8 || a.qualifier.exists(_.length > 1024)) {
            throw new IllegalArgumentException("UNKNOWN_SOURCE_FIELD_LIMIT")
          }
          JObject(List(
            "path" -> JString("/output/" + index),
            "class" -> JString(a.getClass.getName),
            "name" -> JString(a.name),
            "datatype" -> JString(dataType),
            "nullable" -> JBool(a.nullable),
            "expr_id_number" -> JInt(a.exprId.id),
            "expr_id_jvm" -> JString(a.exprId.jvmId.toString),
            "qualifier" -> JArray(a.qualifier.map(JString(_)).toList),
            "metadata_exact" -> JString(metadata)))
        case _ =>
          throw new IllegalArgumentException("UNKNOWN_OUTPUT_EXPRESSION")
      }
      val payload = compact(render(JObject(List(
        "schema" -> JString("queryproof.v8.source-bound.localrelation.v1"),
        "source_plan_class" -> JString(relation.getClass.getName),
        "input_bag_kind" -> JString("empty"),
        "streaming" -> JBool(false),
        "outputs" -> JArray(outputs.toList)))))
      if (payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 131072) {
        throw new IllegalArgumentException("UNKNOWN_FRAME_SIZE")
      }
      payload
    case _ =>
      throw new IllegalArgumentException("UNKNOWN_PLAN_CLASS_OR_DATA")
  }

  private def reason(e: Throwable): String = e match {
    case e: IllegalArgumentException
        if e.getMessage != null && e.getMessage.startsWith("UNKNOWN_") =>
      e.getMessage
    case _ =>
      "UNKNOWN_CAPTURE_EXCEPTION_" + e.getClass.getSimpleName
  }

  def capture[TreeType <: TreeNode[_]](
      executor: String,
      batch: String,
      rule: String,
      iteration: Int,
      before: TreeType,
      after: TreeType): Unit = {
    if (!java.lang.Boolean.getBoolean("queryproof.ast.v8.enabled")) return
    (before, after) match {
      case (b: LogicalPlan, a: LogicalPlan) =>
        val query = System.getProperty("queryproof.query.id", "unlabelled")
        val prefix = Seq(query, executor, batch, rule, iteration.toString)
        val index = seen.getAndIncrement()
        if (index == 2500) {
          System.err.println("QUERYPROOF_AST_V8_UNKNOWN\t" +
            (prefix :+ "UNKNOWN_EVENT_BUDGET_EXCEEDED").map(b64).mkString("\t"))
        } else if (index < 2500) {
          try {
            val beforeFrame = frame(b)
            val afterFrame = frame(a)
            val fields = prefix ++ Seq(b.toJSON, a.toJSON, beforeFrame, afterFrame)
            System.err.println("QUERYPROOF_AST_V8\t" + fields.map(b64).mkString("\t"))
          } catch {
            case scala.util.control.NonFatal(e) =>
              System.err.println("QUERYPROOF_AST_V8_UNKNOWN\t" +
                (prefix :+ reason(e)).map(b64).mkString("\t"))
          }
        }
      case _ =>
        System.err.println("QUERYPROOF_AST_V8_UNKNOWN\t" +
          Seq("UNKNOWN_NON_LOGICAL_PLAN").map(b64).mkString("\t"))
    }
  }
}

'''

def patch_text(original: str) -> str:
    if original.count("  private val scopeIds = new java.util.concurrent.atomic.AtomicInteger(0)") != 1:
        raise ValueError("V8 expects pinned scope instrumentation in disposable checkout")
    if original.count("QUERYPROOF_AST_V8") != 0:
        raise ValueError("V8 is already installed: refusing double instrumentation")
    if original.count(CAPTURE_ANCHOR) != 1:
        raise ValueError("V8 source capture call changed: refusing unsafe patch")
    if original.count(INSERT_ANCHOR) != 1:
        raise ValueError("V8 object insertion anchor changed: refusing unsafe patch")
    updated = original.replace(
        CAPTURE_ANCHOR,
        CAPTURE_ANCHOR +
        "              QueryProofAstTraceV8.capture("
        "name, batch.name, rule.ruleName, iteration, plan, result)\n",
    )
    updated = updated.replace(INSERT_ANCHOR, V8_OBJECT + INSERT_ANCHOR)
    assert updated.count("QueryProofAstTraceV8.capture(") == 1
    return updated


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V8_EPHEMERAL_SOURCE_BOUND_EVENT_INSTRUMENTATION_READY")
