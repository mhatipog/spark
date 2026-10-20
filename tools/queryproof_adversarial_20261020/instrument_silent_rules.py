"""Observe *every* actual Catalyst rule invocation, including fastEquals no-ops.

Research-only patch applied after scope+state+frame+input overlays. Original
Spark and frozen H/K/L artifacts stay untouched. The observer snapshots before
calling a rule, so in-place mutations that return the same plan are visible.
Frame equality remains observational and may suffer known encoding collisions.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

METHODS = r'''
  def ruleSnapshot(plan: TreeNode[_]): Seq[String] = {
    if (!java.lang.Boolean.getBoolean("queryproof.ast.enabled") ||
        !plan.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]) {
      Seq.empty[String]
    } else {
      stateCounts(plan) ++ queryProofFrame(plan) ++ Seq(queryProofLeafInputs(plan))
    }
  }

  def accountRule(
      executor: String, batch: String, rule: String, iteration: Int,
      prior: Seq[String], after: TreeNode[_], effective: Boolean): Unit = {
    if (prior.nonEmpty) {
      try {
        val next = ruleSnapshot(after)
        require(prior.size == 9 && next.size == 9, "Unexpected witness width")
        val mismatch = prior != next
        val frameChanged = prior.slice(4, 8) != next.slice(4, 8)
        val inputChanged = prior(8) != next(8)
        val encode = java.util.Base64.getEncoder
        def b64(s: String): String =
          encode.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        val query = System.getProperty("queryproof.query.id", "unlabelled")
        val fields = Seq(query, executor, batch, rule, iteration.toString,
          effective.toString, mismatch.toString, frameChanged.toString,
          inputChanged.toString)
        System.err.println("QUERYPROOF_RULE_AUDIT\t" +
          fields.map(b64).mkString("\t"))
      } catch {
        case scala.util.control.NonFatal(e) =>
          System.err.println("QUERYPROOF_RULE_AUDIT_ERROR\t" + e.getClass.getName)
      }
    }
  }

'''

PRE_RULE = (
    "            val startTime = System.nanoTime()\n"
    "            val result = if ("
)
PRE_REPLACE = (
    "            val startTime = System.nanoTime()\n"
    "            val queryProofPrior = QueryProofAstTrace.ruleSnapshot(plan)\n"
    "            val result = if ("
)
POST_RULE = (
    "            val effective = !result.fastEquals(plan)\n\n"
    "            if (effective) {"
)
POST_REPLACE = (
    "            val effective = !result.fastEquals(plan)\n"
    "            QueryProofAstTrace.accountRule(name, batch.name, rule.ruleName,\n"
    "              iteration, queryProofPrior, result, effective)\n\n"
    "            if (effective) {"
)


def patch_text(source):
    anchor = "  private def stateMarker("
    if source.count(anchor) != 1:
        raise ValueError("Expected complete state/frame/input observer")
    for old in (PRE_RULE, POST_RULE):
        if source.count(old) != 1:
            raise ValueError("Missing unique real RuleExecutor rule invocation anchor")
    if "  def accountRule(" in source:
        raise ValueError("Already applied")
    source = source.replace(anchor, METHODS + anchor)
    source = source.replace(PRE_RULE, PRE_REPLACE)
    return source.replace(POST_RULE, POST_REPLACE)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_ALL_RULE_INVOCATIONS_OBSERVER_READY")
