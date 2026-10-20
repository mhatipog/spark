"""Describe hidden Catalyst JVM state by class, never by user query or value.

This is a third disposable overlay applied after scope and state instrumentation.
It emits separate private class-profile markers; prior H/K proof code remains pinned.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

PROFILE_METHOD = """
  private def classProfile(root: TreeNode[_]): String = {
    import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    import org.apache.spark.sql.types.{Metadata, StructField, StructType}

    val seen = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val counts = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
    def bump(category: String): Unit = {
      counts(category) = counts(category) + 1
    }
    def visit(value: Any, parent: String): Unit = value match {
      case null =>
      case m: Metadata =>
        if (!m.isEmpty) bump("M:" + parent)
      case n: TreeNode[_] =>
        if (seen.put(n, java.lang.Boolean.TRUE) == null) {
          val owner = n.getClass.getSimpleName
          if (!n.isTagsEmpty) bump("T:" + owner)
          n.productIterator.foreach(v => visit(v, owner))
          n.children.foreach(v => visit(v, owner))
          n match {
            case p: LogicalPlan =>
              p.expressions.foreach(v => visit(v, owner + ".expression"))
              if (p.resolved) p.output.foreach(v => visit(v, owner + ".output"))
            case _ =>
          }
        }
      case s: StructType => s.fields.foreach(v => visit(v, parent))
      case s: StructField =>
        visit(s.metadata, parent + ".StructField")
        visit(s.dataType, parent + ".StructField")
      case o: Option[_] => o.foreach(v => visit(v, parent))
      case m: scala.collection.Map[_, _] =>
        if (m.nonEmpty) bump("Q:" + parent + ".Map")
        m.foreach { case (key, data) =>
          visit(key, parent)
          visit(data, parent)
        }
      case s: Seq[_] =>
        if (s.nonEmpty && !s.forall {
            case _: Product | _: org.apache.spark.sql.types.DataType |
                _: String => true
            case _ => false
          }) bump("Q:" + parent + ".Seq")
        s.foreach(v => visit(v, parent))
      case p: Product =>
        if (seen.put(p.asInstanceOf[AnyRef], java.lang.Boolean.TRUE) == null) {
          p.productIterator.foreach(v => visit(v, parent))
        }
      case _ =>
    }
    visit(root, "ROOT")
    counts.toSeq.sortBy(_._1).map { case (key, value) =>
      key + "=" + value
    }.mkString(",")
  }

"""

ORIGINAL = (
    '    System.err.println("QUERYPROOF_STATE\\t" + fields.map(b64).mkString("\\t"))\n'
)
WITH_DETAIL = (
    ORIGINAL
    + '    val safeClasses = Seq(query) ++ context ++ plans.map(classProfile)\n'
    + '    System.err.println("QUERYPROOF_STATE_CLASS\\t" +\n'
    + '      safeClasses.map(b64).mkString("\\t"))\n'
)


def patch_text(source):
    if source.count("  private def stateMarker(") != 1:
        raise ValueError("Expected precisely one state observer")
    if source.count(ORIGINAL) != 1:
        raise ValueError("State class-diagnostic emit location drifted")
    source = source.replace(
        "  private def stateMarker(", PROFILE_METHOD + "  private def stateMarker(")
    return source.replace(ORIGINAL, WITH_DETAIL)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_EPHEMERAL_HIDDEN_STATE_CLASS_PROFILE_READY")
