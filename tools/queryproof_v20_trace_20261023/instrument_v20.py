"""V20 disposable observer: hidden-tag NAMES present on the plan, per invocation.

For every rule invocation of every RuleExecutor it computes the set of
TreeNodeTag names on all nodes reachable from the result plan (logical
nodes, expressions, inner children such as subquery plans). It emits a line
whenever that set differs from the previous invocation of the same executor,
plus an INIT line (initial set and SQLConf.ansiEnabled) at its first
invocation. Tag VALUES are never emitted. Install after instrument_scopes.py.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
AFTER = "            val effective = !result.fastEquals(plan)\n"
INSERT = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"
OBJECT = r'''
/** V20: hidden-tag name observer (names only; values are never read out). */
private[rules] object QueryProofTagsV20 {
  import org.apache.spark.sql.catalyst.trees.TreeNodeTag
  private lazy val tagField = {
    val f = classOf[TreeNode[_]].getDeclaredFields.find(_.getName.endsWith("_tags")).getOrElse(
      throw new IllegalStateException("Cannot inspect Catalyst node tags"))
    f.setAccessible(true)
    f
  }
  private val last = new java.util.concurrent.ConcurrentHashMap[String, String]()
  private val encoder = java.util.Base64.getEncoder
  private def b64(s: String): String =
    encoder.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  def names(root: TreeNode[_]): String = {
    val seen = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val out = scala.collection.mutable.TreeSet.empty[String]
    def visit(n: TreeNode[_]): Unit = {
      if (seen.put(n, java.lang.Boolean.TRUE) == null) {
        val raw = tagField.get(n)
        if (raw != null) {
          raw.asInstanceOf[scala.collection.mutable.Map[TreeNodeTag[_], Any]].keys
            .foreach(k => out += k.name)
        }
        n.children.foreach { case t: TreeNode[_] => visit(t); case _ => }
        n.innerChildren.foreach { case t: TreeNode[_] => visit(t); case _ => }
        n match {
          case p: org.apache.spark.sql.catalyst.plans.QueryPlan[_] =>
            p.expressions.foreach(visit)
          case _ =>
        }
      }
    }
    visit(root)
    out.mkString(",")
  }
  private def emit(fields: Seq[String]): Unit =
    System.err.println("QUERYPROOF_V20_TAGS\t" + fields.map(b64).mkString("\t"))
  def observe[T <: TreeNode[_]](executor: String, batch: String, rule: String, iteration: Int,
      before: T, after: T, effective: Boolean): Unit = {
    if (!java.lang.Boolean.getBoolean("queryproof.v20.tags")) return
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    try {
      if (!last.containsKey(executor)) {
        val initial = names(before)
        last.put(executor, initial)
        val ansi = org.apache.spark.sql.internal.SQLConf.get.ansiEnabled.toString
        emit(Seq("INIT", query, executor, batch, rule, iteration.toString, ansi, initial))
      }
      val now = names(after)
      if (now != last.get(executor)) {
        last.put(executor, now)
        emit(Seq("CHANGE", query, executor, batch, rule, iteration.toString,
          effective.toString, now))
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        emit(Seq("UNKNOWN", query, executor, batch, rule, iteration.toString,
          e.getClass.getSimpleName, ""))
    }
  }
}

'''


def patch_text(src):
    for marker in (AFTER, INSERT):
        if src.count(marker) != 1:
            raise ValueError("V20 expects exactly one anchor " + repr(marker))
    if "QueryProofTagsV20" in src:
        raise ValueError("V20 observer already installed")
    src = src.replace(AFTER, AFTER +
        "            QueryProofTagsV20.observe(name, batch.name, rule.ruleName, iteration,\n"
        "              plan, result, effective)\n")
    return src.replace(INSERT, OBJECT + INSERT)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V20_TAG_OBSERVER_READY")
