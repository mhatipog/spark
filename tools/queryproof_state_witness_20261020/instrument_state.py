"""Add a private JVM state witness to a disposable, already scope-instrumented copy.

Neither the frozen H/K traces nor the repository's Catalyst source are changed.
The added Scala scans runtime tags, metadata and collection fields that plan JSON
can omit. A missing witness is not interpreted as an empty witness.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

STATE_METHODS = """
  private def stateCounts(root: TreeNode[_]): Seq[String] = {
    import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
    import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
    import org.apache.spark.sql.catalyst.plans.physical.Partitioning

    val visited = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    var tagged = 0
    var nonemptyMetadata = 0
    var opaqueFields = 0
    var nodes = 0

    def visit(value: Any): Unit = value match {
      case null =>
      case m: Metadata =>
        if (!m.isEmpty) nonemptyMetadata += 1
      case n: TreeNode[_] =>
        if (visited.put(n, java.lang.Boolean.TRUE) == null) {
          nodes += 1
          if (!n.isTagsEmpty) tagged += 1
          n.productIterator.foreach(visit)
          n.children.foreach(visit)
          n match {
            case a: AttributeReference => visit(a.metadata)
            case a: Alias => visit(a.metadata)
            case p: LogicalPlan =>
              p.expressions.foreach(visit)
              if (p.resolved) p.output.foreach(visit)
            case _ =>
          }
        }
      case s: StructType => s.fields.foreach(visit)
      case s: StructField =>
        visit(s.metadata)
        visit(s.dataType)
      case o: Option[_] => o.foreach(visit)
      case m: scala.collection.Map[_, _] =>
        if (m.nonEmpty) opaqueFields += 1
        m.foreach { case (key, data) =>
          visit(key)
          visit(data)
        }
      case s: Seq[_] =>
        if (s.nonEmpty && !s.forall {
            case _: Product | _: DataType | _: String | _: Partitioning => true
            case _ => false
          }) {
          opaqueFields += 1
        }
        s.foreach(visit)
      case p: Product =>
        if (visited.put(p, java.lang.Boolean.TRUE) == null) {
          p.productIterator.foreach(visit)
        }
      case _ =>
    }

    visit(root)
    Seq(tagged, nonemptyMetadata, opaqueFields, nodes).map(_.toString)
  }

  private def stateMarker(context: Seq[String], plans: Seq[TreeNode[_]]): Unit = {
    val encode = java.util.Base64.getEncoder
    def b64(s: String): String =
      encode.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    val fields = Seq(query) ++ context ++ plans.flatMap(stateCounts)
    System.err.println("QUERYPROOF_STATE\\t" + fields.map(b64).mkString("\\t"))
  }
"""

ADDITIONS = [
    (
        "  private val scopeIds = new java.util.concurrent.atomic.AtomicInteger(0)\n",
        "  private val scopeIds = new java.util.concurrent.atomic.AtomicInteger(0)\n"
        + STATE_METHODS,
    ),
    (
        '          Seq(query, kind, executor, batch, scopeId.toString, phase, plan.toJSON)\n'
        '            .map(b64).mkString("\\t"))\n',
        '          Seq(query, kind, executor, batch, scopeId.toString, phase, plan.toJSON)\n'
        '            .map(b64).mkString("\\t"))\n'
        '        stateMarker(Seq("SCOPE", kind, executor, batch, scopeId.toString, phase),\n'
        '          Seq(plan))\n',
    ),
    (
        '          Seq(query, executor, batch, rule, iteration.toString, before.toJSON, after.toJSON)\n'
        '            .map(b64).mkString("\\t"))\n',
        '          Seq(query, executor, batch, rule, iteration.toString, before.toJSON, after.toJSON)\n'
        '            .map(b64).mkString("\\t"))\n'
        '        stateMarker(Seq("EVENT", executor, batch, rule, iteration.toString),\n'
        '          Seq(before, after))\n',
    ),
]


def patch_text(source):
    if source.count("QUERYPROOF_SCOPE") != 2:
        raise ValueError("Expected the previously instrumented scope source")
    for old, new in ADDITIONS:
        if source.count(old) != 1:
            raise ValueError("State overlay anchor absent or duplicated: " + repr(old[:60]))
        source = source.replace(old, new)
    if source.count("stateMarker(") != 3:
        raise ValueError("State instrumentation incomplete")
    return source


if __name__ == "__main__":
    updated = patch_text(SOURCE.read_text())
    SOURCE.write_text(updated)
    print("QUERYPROOF_EPHEMERAL_JVM_STATE_WITNESS_READY")
