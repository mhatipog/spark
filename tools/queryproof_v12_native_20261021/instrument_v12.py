"""V12 disposable source-native attribute/metadata witness for real Spark rule calls.

This intentionally does NOT assert full Spark state completeness. It captures
path-indexed JVM attributes and exact native Metadata.empty observations,
plus raw official toJSON hashes *before* a real rule and after the rule.
A candidate with missing attestations is UNKNOWN, never silently accepted.

Install AFTER the V11 disposable overlays and BEFORE building the Spark JAR.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
START = "            val startTime = System.nanoTime()\n"
AFTER = "            val effective = !result.fastEquals(plan)\n"
INSERT = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"

OBJECT = r'''
/** Narrow native source observation for real InferFiltersFromConstraints calls.
 * NOT a trustworthy entire-plan semantic interpretation. Source-side metadata
 * is read directly from AttributeReference/Alias objects, bypassing lossy JSON.
 */
private[rules] object QueryProofNativeV12 {
  import org.json4s.JsonAST._
  import org.json4s.jackson.JsonMethods.{compact, render}
  import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
  import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)
  private final case class SourceBinding(
      name: String, datatype: String, nullable: Boolean,
      qualifier: Seq[String], metadata: org.apache.spark.sql.types.Metadata)
  private final case class SourceSnap(
      encoded: String, bindings: Map[String, SourceBinding])
  private val encoder = java.util.Base64.getEncoder
  private def b64(s: String): String =
    encoder.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  private def hash(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(b => "%02x".format(b & 0xff)).mkString
  }

  private def unknown(why: String): Nothing =
    throw new IllegalArgumentException("UNKNOWN_" + why)

  private def frame(plan: TreeNode[_]): SourceSnap = plan match {
    case root: LogicalPlan =>
      val attrs = scala.collection.mutable.ArrayBuffer.empty[JValue]
      val aliases = scala.collection.mutable.ArrayBuffer.empty[JValue]
      val bindings = scala.collection.mutable.Map.empty[String, SourceBinding]
      var nonemptyMetadata = 0
      def register(kind: String, a: org.apache.spark.sql.catalyst.expressions.NamedExpression): Unit = {
        val key = kind + ":" + a.exprId.id.toString + ":" + a.exprId.jvmId.toString
        val binding = SourceBinding(
          a.name, a.dataType.json, a.nullable, a.qualifier, a.metadata)
        bindings.get(key).foreach { existing =>
          if (existing != binding) unknown("CONFLICTING_NATIVE_EXPR_ID")
        }
        bindings(key) = binding
      }
      var tagsPresent = 0
      var planNodes = 0
      var expressionNodes = 0
      def tag(node: TreeNode[_]): Unit = {
        if (!node.isTagsEmpty) tagsPresent += 1
      }
      def appendAttribute(a: AttributeReference, path: String): Unit = {
        if (attrs.size >= 10000) unknown("ATTRIBUTE_BUDGET")
        if (a.name.length > 4096 || a.metadata.json.length > 65536 ||
            a.dataType.json.length > 16384) unknown("SOURCE_FIELD_BUDGET")
        if (!a.metadata.isEmpty) nonemptyMetadata += 1
        register("attribute", a)
        attrs += JObject(List(
          "path" -> JString(path),
          "expr_id" -> JInt(BigInt(a.exprId.id)),
          "jvm_id" -> JString(a.exprId.jvmId.toString),
          "class" -> JString(a.getClass.getName),
          "name_sha256" -> JString(hash(a.name)),
          "dtype" -> JString(a.dataType.json),
          "nullable" -> JBool(a.nullable),
          "metadata_empty" -> JBool(a.metadata.isEmpty),
          "metadata_sha256" -> JString(hash(a.metadata.json))))
      }
      def visitExpr(e: Expression, path: String): Unit = {
        expressionNodes += 1
        if (expressionNodes > 30000) unknown("EXPRESSION_BUDGET")
        tag(e)
        e match {
          case a: AttributeReference => appendAttribute(a, path)
          case a: Alias =>
            if (aliases.size >= 10000) unknown("ALIAS_BUDGET")
            if (a.metadata.json.length > 65536) unknown("ALIAS_METADATA_BUDGET")
            if (!a.metadata.isEmpty) nonemptyMetadata += 1
            register("alias", a)
            aliases += JObject(List(
              "path" -> JString(path),
              "expr_id" -> JInt(BigInt(a.exprId.id)),
              "jvm_id" -> JString(a.exprId.jvmId.toString),
              "metadata_empty" -> JBool(a.metadata.isEmpty),
              "metadata_sha256" -> JString(hash(a.metadata.json))))
          case _ =>
        }
        if (e.innerChildren.nonEmpty) unknown("EXPRESSION_INNER_CHILDREN")
        e.children.zipWithIndex.foreach { case (child, i) =>
          visitExpr(child, path + "/e" + i)
        }
      }
      def visitPlan(p: LogicalPlan, path: String): Unit = {
        planNodes += 1
        if (planNodes > 10000) unknown("PLAN_BUDGET")
        if (!p.resolved || p.innerChildren.nonEmpty) unknown("UNRESOLVED_OR_INNER_PLAN")
        tag(p)
        p.output.zipWithIndex.foreach {
          case (a: AttributeReference,i) => appendAttribute(a, path + "/out" + i)
          case _ => unknown("NON_ATTRIBUTE_OUTPUT")
        }
        p.expressions.zipWithIndex.foreach { case (e,i) =>
          visitExpr(e, path + "/expr" + i)
        }
        p.children.zipWithIndex.foreach {
          case (child: LogicalPlan,i) => visitPlan(child, path + "/plan" + i)
          case _ => unknown("NON_LOGICAL_PLAN_CHILD")
        }
      }
      val rawJson = root.toJSON
      visitPlan(root, "/root")
      val payload = JObject(List(
        "schema" -> JString("queryproof.v12.native-attribute-observer.v1"),
        "json_sha256" -> JString(hash(rawJson)),
        "source_plan_nodes" -> JInt(BigInt(planNodes)),
        "source_expression_nodes" -> JInt(BigInt(expressionNodes)),
        "source_attribute_occurrences" -> JInt(BigInt(attrs.size)),
        "source_alias_occurrences" -> JInt(BigInt(aliases.size)),
        "nonempty_metadata_occurrences" -> JInt(BigInt(nonemptyMetadata)),
        "tagged_nodes" -> JInt(BigInt(tagsPresent)),
        "attributes" -> JArray(attrs.toList),
        "aliases" -> JArray(aliases.toList)))
      val data = compact(render(payload))
      if (data.length > 4000000) unknown("NATIVE_FRAME_BUDGET")
      SourceSnap(data, bindings.toMap)
    case _ => unknown("NON_LOGICAL_PLAN")
  }

  private def reason(e: Throwable): String = e match {
    case x: IllegalArgumentException
        if x.getMessage != null && x.getMessage.startsWith("UNKNOWN_") =>
      x.getMessage
    case _ => "UNKNOWN_CAPTURE_EXCEPTION_" + e.getClass.getSimpleName
  }

  type Snapshot = Option[Either[String, SourceSnap]]
  def begin[TreeType <: TreeNode[_]](rule: String, plan: TreeType): Snapshot = {
    if (!java.lang.Boolean.getBoolean("queryproof.ast.v12.enabled") ||
        !rule.endsWith("InferFiltersFromConstraints")) None
    else Some(try { Right(frame(plan)) }
              catch { case scala.util.control.NonFatal(e) => Left(reason(e)) })
  }

  def finish[TreeType <: TreeNode[_]](
      executor: String, batch: String, rule: String, iteration: Int,
      pre: Snapshot, after: TreeType, effective: Boolean): Unit = {
    if (pre.isEmpty) return
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    val prefix = Seq(query, executor, batch, rule, iteration.toString,
                     effective.toString)
    val ordinal = seen.getAndIncrement()
    if (ordinal >= 2500) {
      if (ordinal == 2500) {
        System.err.println("QUERYPROOF_NATIVE_V12_UNKNOWN\t" +
          (prefix :+ "UNKNOWN_EVENT_BUDGET").map(b64).mkString("\t"))
      }
      return
    }
    pre.get match {
      case Right(preFrame) =>
        try {
          val postFrame = frame(after)
          // Actual JVM Metadata equality; a digest collision alone cannot pass.
          // Unobserved tag/cache/SQLConf changes remain outside the observation.
          val sourceFieldsExactlyUnchanged = preFrame.bindings == postFrame.bindings
          System.err.println("QUERYPROOF_NATIVE_V12\t" +
            (prefix ++ Seq(preFrame.encoded, postFrame.encoded,
                           sourceFieldsExactlyUnchanged.toString))
              .map(b64).mkString("\t"))
        } catch {
          case scala.util.control.NonFatal(e) =>
            System.err.println("QUERYPROOF_NATIVE_V12_UNKNOWN\t" +
              (prefix :+ reason(e)).map(b64).mkString("\t"))
        }
      case Left(why) =>
        System.err.println("QUERYPROOF_NATIVE_V12_UNKNOWN\t" +
          (prefix :+ why).map(b64).mkString("\t"))
    }
  }
}

'''

def patch_text(src):
    for marker in (START, AFTER, INSERT):
        if src.count(marker) != 1:
            raise ValueError("V12 expects exactly one source anchor " + repr(marker))
    if "QUERYPROOF_NATIVE_V12" in src:
        raise ValueError("V12 native observer already installed")
    src = src.replace(START, START + "            val qpV12Pre = QueryProofNativeV12.begin(rule.ruleName, plan)\n")
    src = src.replace(AFTER, AFTER +
        "            QueryProofNativeV12.finish(name, batch.name, rule.ruleName,\n"
        "              iteration, qpV12Pre, result, effective)\n")
    src = src.replace(INSERT, OBJECT + INSERT)
    return src

if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V12_NATIVE_ATTRIBUTE_BEFORE_AFTER_CAPTURE_READY")
