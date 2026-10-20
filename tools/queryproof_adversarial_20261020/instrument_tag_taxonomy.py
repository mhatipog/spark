"""Ephemeral, privacy-preserving tag-key/type census for Spark 4.2 Catalyst.

Must run AFTER scope, state, frame and input overlays. Emits only hashed tag
names, Java value-type names, type-shape booleans and counts, never tag values or SQL literals.
Do not infer tag noninterference merely from stable hashes.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

METHODS = r'''
  private def queryProofTagTaxonomy(root: TreeNode[_]): String = {
    import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    import org.apache.spark.sql.catalyst.trees.TreeNodeTag
    val visited = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val counts = scala.collection.mutable.Map.empty[(String, String, String), Int]
    def visit(value: Any): Unit = value match {
      case null =>
      case node: TreeNode[_] =>
        if (visited.put(node, java.lang.Boolean.TRUE) == null) {
          if (!node.isTagsEmpty) {
            val raw = queryProofTagField.get(node).asInstanceOf[
              scala.collection.mutable.Map[TreeNodeTag[_], Any]]
            raw.foreach { case (key, datum) =>
              val nameHash = queryProofHash(key.name)
              val kind = if (datum == null) "null" else datum.getClass.getName
              val shape = if (key.name == "dataset_id") datum match {
                case xs: scala.collection.mutable.HashSet[_] =>
                  if (xs.forall(_.isInstanceOf[Long])) "all-long-elements"
                  else "non-long-elements"
                case _ => "not-hashset"
              } else "not-examined"
              val label = (nameHash, kind, shape)
              counts(label) = counts.getOrElse(label, 0) + 1
            }
          }
          node.productIterator.foreach(visit)
          node.children.foreach(visit)
          node match {
            case plan: LogicalPlan =>
              plan.expressions.foreach(visit)
              if (plan.resolved) plan.output.foreach(visit)
            case _ =>
          }
        }
      case option: Option[_] => option.foreach(visit)
      case sequence: Seq[_] => sequence.foreach(visit)
      case mapping: scala.collection.Map[_, _] =>
        mapping.foreach { case (k, v) =>
          visit(k)
          visit(v)
        }
      case product: Product =>
        if (visited.put(product.asInstanceOf[AnyRef], java.lang.Boolean.TRUE) == null) {
          product.productIterator.foreach(visit)
        }
      case _ =>
    }
    visit(root)
    counts.toSeq.sorted.map { case ((key, kind, shape), number) =>
      key + ":" + kind + ":" + shape + ":" + number.toString
    }.mkString(";")
  }

'''

TAG_CALL = r'''    val taxonomy = Seq(query) ++ context ++ plans.map(queryProofTagTaxonomy)
    System.err.println("QUERYPROOF_TAG_TAXONOMY\t" + taxonomy.map(b64).mkString("\t"))
'''


def patch_text(source):
    anchor = "  private def stateMarker("
    if source.count(anchor) != 1:
        raise ValueError("Missing unique disposable JVM state observer")
    if source.count("queryProofLeafInputs") < 2:
        raise ValueError("Input observer must precede tag census")
    if "queryProofTagTaxonomy" in source:
        raise ValueError("Tag census already applied")
    source = source.replace(anchor, METHODS + anchor)
    lines = source.splitlines(keepends=True)
    after = [i for i, line in enumerate(lines)
             if 'System.err.println("QUERYPROOF_INPUT' in line]
    if len(after) != 1:
        raise ValueError("No unique input marker anchor")
    lines.insert(after[0] + 1, TAG_CALL)
    return "".join(lines)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_HASHED_TAG_TAXONOMY_OBSERVER_READY")
