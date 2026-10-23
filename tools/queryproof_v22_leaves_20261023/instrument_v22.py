"""V22 disposable observer: data identity of plan LEAVES, which TreeNode JSON erases.

TreeNode.toJSON serializes LogicalRDD with "rdd": null and omits
LocalRelation.data, so two leaves with different contents can print
identically. For every effective rule event this emits, for the plans before
and after, the preorder index of each leaf node with a fingerprint:
  LogicalRDD     -> rdd:<RDD.id>:<identityHashCode(rdd)>
  LocalRelation  -> local:<row count>:<sha256 of the rows' string forms>
  other leaves   -> other:<class name>
Install after instrument_v20.py; it uses the same anchor.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
AFTER = "            val effective = !result.fastEquals(plan)\n"
INSERT = "class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {\n"
OBJECT = r'''
/** V22: leaf data-identity observer (fingerprints only; row contents never emitted). */
private[rules] object QueryProofLeavesV22 {
  import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
  private val encoder = java.util.Base64.getEncoder
  private def b64(s: String): String =
    encoder.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  private def sha(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map("%02x".format(_)).mkString
  }
  private def fingerprint(p: LogicalPlan): String = p match {
    case l: LocalRelation =>
      "local:" + l.data.size + ":" + sha(l.data.map(_.toString).mkString("\u0001"))
    case other if other.getClass.getName == "org.apache.spark.sql.execution.LogicalRDD" =>
      val rdd = other.getClass.getMethod("rdd").invoke(other)
      val id = rdd.getClass.getMethod("id").invoke(rdd)
      "rdd:" + id + ":" + System.identityHashCode(rdd)
    case other => "other:" + other.getClass.getName
  }
  def leaves(root: LogicalPlan): String = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    var index = 0
    def visit(p: LogicalPlan): Unit = {
      if (p.children.isEmpty) out += (index.toString + "=" + fingerprint(p))
      index += 1
      p.children.foreach(visit)
    }
    visit(root)
    out.mkString(";")
  }
  def observe[T <: TreeNode[_]](executor: String, batch: String, rule: String, iteration: Int,
      before: T, after: T, effective: Boolean): Unit = {
    if (!effective || !java.lang.Boolean.getBoolean("queryproof.v22.leaves")) return
    val query = System.getProperty("queryproof.query.id", "unlabelled")
    val line = (before, after) match {
      case (b: LogicalPlan, a: LogicalPlan) =>
        try Seq("OK", query, executor, batch, rule, iteration.toString, leaves(b), leaves(a))
        catch { case scala.util.control.NonFatal(e) =>
          Seq("UNKNOWN", query, executor, batch, rule, iteration.toString, e.getClass.getSimpleName, "") }
      case _ => Seq("NOT_LOGICAL", query, executor, batch, rule, iteration.toString, "", "")
    }
    System.err.println("QUERYPROOF_V22_LEAVES\t" + line.map(b64).mkString("\t"))
  }
}

'''


def patch_text(src):
    for marker in (AFTER, INSERT):
        if src.count(marker) != 1:
            raise ValueError("V22 expects exactly one anchor " + repr(marker))
    if "QueryProofLeavesV22" in src:
        raise ValueError("V22 observer already installed")
    src = src.replace(AFTER, AFTER +
        "            QueryProofLeavesV22.observe(name, batch.name, rule.ruleName, iteration,\n"
        "              plan, result, effective)\n")
    return src.replace(INSERT, OBJECT + INSERT)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V22_LEAF_OBSERVER_READY")
