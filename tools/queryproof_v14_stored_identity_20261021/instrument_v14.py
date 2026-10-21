"""V14 disposable diagnostic: STORED versus DERIVED NamedExpression identity.

Hypothesis under test: V12's strict JVM-identity attestation fails on W17-W20
because LogicalPlan.output is re-derived on every call (Alias.toAttribute
allocates a fresh AttributeReference in Project/Aggregate), not because the
rule replaced any stored source object.

V14 splits the observation:
  stored  = NamedExpressions reachable from QueryPlan.expressions (fields
            physically held by the tree);
  derived = LogicalPlan.output occurrences.
and adds a SELF control: two observations of the same pre-rule plan object.

Diagnostic only. It accepts nothing, proves nothing about tags, caches or
SQLConf, and calling output/expressions may itself force lazy vals.
Install AFTER instrument_v12.py and BEFORE building the overlay.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
V12_BEGIN = "            val qpV12Pre = QueryProofNativeV12.begin(rule.ruleName, plan)\n"
V12_FINISH = "              iteration, qpV12Pre, result, effective)\n"
INSERT = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"
OBJECT = r'''
/** V14: stored-vs-derived NamedExpression identity diagnostic. Emits counts
 * only; never an acceptance. Tags, caches and SQLConf are not observed.
 */
private[rules] object QueryProofNativeV14 {
  import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
  import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
  final case class Fields(
      cls: String, exprId: Long, jvmId: String, name: String, dataType: String,
      nullable: Boolean, qualifier: Seq[String],
      metadata: org.apache.spark.sql.types.Metadata)
  type Ids = java.util.IdentityHashMap[NamedExpression, Fields]
  final case class Snap(stored: Ids, derived: Ids)
  type Snapshot = Option[Either[String, (Snap, String)]]
  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val encoder = java.util.Base64.getEncoder
  private def b64(s: String): String =
    encoder.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  private def unknown(why: String): Nothing =
    throw new IllegalArgumentException("UNKNOWN_V14_" + why)
  private def fields(a: NamedExpression): Fields = Fields(
    a.getClass.getName, a.exprId.id, a.exprId.jvmId.toString, a.name,
    a.dataType.json, a.nullable, a.qualifier, a.metadata)
  private def snap(root: LogicalPlan): Snap = {
    val stored = new Ids()
    val derived = new Ids()
    var budget = 0
    def put(m: Ids, a: NamedExpression): Unit = {
      budget += 1
      if (budget > 50000) unknown("BUDGET")
      val f = fields(a)
      val prior = m.put(a, f)
      if (prior != null && prior != f) unknown("IN_PLACE_MUTATION_DURING_CAPTURE")
    }
    def expr(e: Expression): Unit = {
      if (e.innerChildren.nonEmpty) unknown("EXPRESSION_INNER_CHILDREN")
      e match {
        case n: NamedExpression => put(stored, n)
        case _ =>
      }
      e.children.foreach(expr)
    }
    def plan(p: LogicalPlan): Unit = {
      if (!p.resolved || p.innerChildren.nonEmpty) unknown("UNRESOLVED_OR_INNER_PLAN")
      p.expressions.foreach(expr)
      p.output.foreach(a => put(derived, a))
      p.children.foreach(plan)
    }
    plan(root)
    Snap(stored, derived)
  }
  private def values(maps: Ids*): java.util.HashSet[Fields] = {
    val out = new java.util.HashSet[Fields]()
    maps.foreach(m => out.addAll(m.values()))
    out
  }
  /** missing,changed,fresh,freshWithUnseenFields */
  private def diff(pre: Ids, post: Ids, known: java.util.HashSet[Fields]): String = {
    var missing = 0
    var changed = 0
    val it = pre.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      val f = post.get(e.getKey)
      if (f == null) missing += 1 else if (f != e.getValue) changed += 1
    }
    var fresh = 0
    var unseen = 0
    val jt = post.entrySet().iterator()
    while (jt.hasNext) {
      val e = jt.next()
      if (!pre.containsKey(e.getKey)) {
        fresh += 1
        if (!known.contains(e.getValue)) unseen += 1
      }
    }
    Seq(missing, changed, fresh, unseen).mkString(",")
  }
  private def reason(e: Throwable): String = e match {
    case x: IllegalArgumentException
        if x.getMessage != null && x.getMessage.startsWith("UNKNOWN_V14_") => x.getMessage
    case _ => "UNKNOWN_V14_CAPTURE_EXCEPTION_" + e.getClass.getSimpleName
  }
  def begin[TreeType <: TreeNode[_]](rule: String, plan: TreeType): Snapshot = {
    val production = rule.endsWith("InferFiltersFromConstraints")
    val diagnostic = java.lang.Boolean.getBoolean("queryproof.ast.v12.test.enabled") &&
      (rule.endsWith("ChangeOnlyMetadataV12") || rule.endsWith("SetOnlyTagV12") ||
       rule.endsWith("TransientTagV12"))
    if (!java.lang.Boolean.getBoolean("queryproof.ast.v14.enabled") ||
        !(production || diagnostic)) None
    else Some(try {
      plan match {
        case p: LogicalPlan =>
          val first = snap(p)
          val second = snap(p)
          val known = values(first.stored, first.derived)
          val self = diff(first.stored, second.stored, known) + ";" +
            diff(first.derived, second.derived, known)
          Right((first, self))
        case _ => unknown("NON_LOGICAL_PLAN")
      }
    } catch { case scala.util.control.NonFatal(e) => Left(reason(e)) })
  }
  def finish[TreeType <: TreeNode[_]](
      executor: String, batch: String, rule: String, iteration: Int,
      pre: Snapshot, after: TreeType, effective: Boolean): Unit = {
    if (pre.isEmpty) return
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    val prefix = Seq(query, executor, batch, rule, iteration.toString, effective.toString)
    if (seen.getAndIncrement() >= 2500) return
    val line = pre.get match {
      case Left(why) => "QUERYPROOF_NATIVE_V14_UNKNOWN\t" + (prefix :+ why).map(b64).mkString("\t")
      case Right((preSnap, self)) =>
        try {
          after match {
            case p: LogicalPlan =>
              val post = snap(p)
              val known = values(preSnap.stored, preSnap.derived)
              "QUERYPROOF_NATIVE_V14\t" + (prefix ++ Seq(self,
                diff(preSnap.stored, post.stored, known),
                diff(preSnap.derived, post.derived, known))).map(b64).mkString("\t")
            case _ => unknown("NON_LOGICAL_PLAN")
          }
        } catch {
          case scala.util.control.NonFatal(e) =>
            "QUERYPROOF_NATIVE_V14_UNKNOWN\t" + (prefix :+ reason(e)).map(b64).mkString("\t")
        }
    }
    System.err.println(line)
  }
}

'''


def patch_text(src):
    for marker in (V12_BEGIN, V12_FINISH, INSERT):
        if src.count(marker) != 1:
            raise ValueError("V14 expects exactly one anchor (install V12 first) " + repr(marker))
    if "QueryProofNativeV14" in src:
        raise ValueError("V14 observer already installed")
    src = src.replace(V12_BEGIN, V12_BEGIN +
        "            val qpV14Pre = QueryProofNativeV14.begin(rule.ruleName, plan)\n")
    src = src.replace(V12_FINISH, V12_FINISH +
        "            QueryProofNativeV14.finish(name, batch.name, rule.ruleName,\n"
        "              iteration, qpV14Pre, result, effective)\n")
    src = src.replace(INSERT, OBJECT + INSERT)
    return src


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V14_STORED_DERIVED_IDENTITY_CAPTURE_READY")
