"""Instrument private tagged-state, metadata and output-frame fingerprints.

Run after disposable scope+state overlays. The tags themselves, metadata and
SQL literals never enter workflow output: only SHA-256 identifiers are traced
to a temporary stderr stream that is deleted after scoring.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

FRAME_METHODS = """
  private lazy val queryProofTagField = {
    val field = classOf[TreeNode[_]].getDeclaredFields.find(
      _.getName.endsWith("_tags")).getOrElse {
      throw new IllegalStateException("Cannot inspect Catalyst private node tags")
    }
    field.setAccessible(true)
    field
  }

  private def queryProofHash(data: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(data.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(b => f"\u0024{b & 0xff}%02x").mkString
  }

  private def queryProofValue(value: Any): String = value match {
    case null => "null"
    case s: scala.collection.Set[_] =>
      s.iterator.map(queryProofValue).toSeq.sorted.mkString("{", ",", "}")
    case m: scala.collection.Map[_, _] =>
      m.iterator.map { case (k, v) =>
        queryProofValue(k) + ":" + queryProofValue(v)
      }.toSeq.sorted.mkString("{", ",", "}")
    case s: Seq[_] => s.map(queryProofValue).mkString("[", ",", "]")
    case x => x.toString
  }

  private def queryProofNodeTags(node: TreeNode[_]): String = {
    import org.apache.spark.sql.catalyst.trees.TreeNodeTag
    val raw = queryProofTagField.get(node)
    if (raw == null) {
      ""
    } else {
      val tags = raw.asInstanceOf[
        scala.collection.mutable.Map[TreeNodeTag[_], Any]]
      tags.toSeq.map { case (key, value) =>
        key.name + ":" + queryProofValue(value)
      }.sorted.mkString("|")
    }
  }

  private def queryProofFrame(root: TreeNode[_]): Seq[String] = {
    import org.apache.spark.sql.catalyst.expressions.AttributeReference
    import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

    val visited = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val tags = scala.collection.mutable.ArrayBuffer.empty[String]
    val attributes = scala.collection.mutable.Map.empty[String, String]
    var conflict = false

    def visit(value: Any): Unit = value match {
      case null =>
      case n: TreeNode[_] =>
        if (visited.put(n, java.lang.Boolean.TRUE) == null) {
          if (!n.isTagsEmpty) {
            tags += n.getClass.getSimpleName + ":" +
              queryProofHash(queryProofNodeTags(n))
          }
          n match {
            case a: AttributeReference =>
              val key = queryProofHash(a.exprId.toString)
              val value = queryProofHash(a.metadata.json)
              attributes.get(key).foreach { prior =>
                if (prior != value) conflict = true
              }
              attributes(key) = value
            case p: LogicalPlan =>
              p.expressions.foreach(visit)
              if (p.resolved) p.output.foreach(visit)
            case _ =>
          }
          n.productIterator.foreach(visit)
          n.children.foreach(visit)
        }
      case o: Option[_] => o.foreach(visit)
      case s: Seq[_] => s.foreach(visit)
      case m: scala.collection.Map[_, _] =>
        m.foreach { case (k, v) =>
          visit(k)
          visit(v)
        }
      case p: Product =>
        if (visited.put(p.asInstanceOf[AnyRef], java.lang.Boolean.TRUE) == null) {
          p.productIterator.foreach(visit)
        }
      case _ =>
    }
    visit(root)

    val outputs = root match {
      case p: LogicalPlan if p.resolved =>
        p.output.map { a =>
          val row = Seq(a.exprId.toString, a.name, a.dataType.json,
            a.nullable.toString, a.metadata.json).mkString("\\u0000")
          queryProofHash(row)
        }.mkString(",")
      case _ => "UNRESOLVED"
    }
    Seq(tags.sorted.mkString(","), attributes.toSeq.sorted.map {
      case (key, value) => key + ":" + value
    }.mkString(","), outputs, conflict.toString)
  }

"""

MARKER = '    System.err.println("QUERYPROOF_STATE\\t" + fields.map(b64).mkString("\\t"))\n'
ENRICHED = (
    MARKER +
    '    val frames = Seq(query) ++ context ++ plans.flatMap(queryProofFrame)\n'
    '    System.err.println("QUERYPROOF_FRAME\\t" + frames.map(b64).mkString("\\t"))\n'
)


def patch_text(source):
    if source.count("  private def stateMarker(") != 1:
        raise ValueError("Missing earlier disposable state overlay")
    if source.count(MARKER) != 1:
        raise ValueError("Missing exact state marker anchor")
    source = source.replace(
        "  private def stateMarker(", FRAME_METHODS + "  private def stateMarker(")
    return source.replace(MARKER, ENRICHED)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_EPHEMERAL_HASHED_STATE_FRAME_READY")
