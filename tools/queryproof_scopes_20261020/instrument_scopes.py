"""Instrument only a disposable GitHub Actions checkout with scope boundaries.

The pinned QueryProof tracer in Git history is never modified by this research
branch. Its instrumented copy is discarded with the CI runner.
"""
import hashlib
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
EXPECTED_BLOB = "a4ac5ba5f280164d9b0276703c06a288bc4309b5"

SCOPE_METHODS = """
  private val scopeIds = new java.util.concurrent.atomic.AtomicInteger(0)

  private def scopeMarker[TreeType <: TreeNode[_]](
      kind: String,
      executor: String,
      batch: String,
      scopeId: Int,
      phase: String,
      plan: TreeType): Unit = {
    if (java.lang.Boolean.getBoolean("queryproof.ast.enabled") &&
        plan.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]) {
      try {
        val encode = java.util.Base64.getEncoder
        def b64(s: String): String =
          encode.encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        val query = System.getProperty("queryproof.query.id", "unlabelled")
        System.err.println("QUERYPROOF_SCOPE\\t" +
          Seq(query, kind, executor, batch, scopeId.toString, phase, plan.toJSON)
            .map(b64).mkString("\\t"))
      } catch {
        case scala.util.control.NonFatal(e) =>
          System.err.println("QUERYPROOF_SCOPE_ERROR\\t" + e.getClass.getName)
      }
    }
  }

  def beginScope[TreeType <: TreeNode[_]](
      kind: String,
      executor: String,
      batch: String,
      plan: TreeType): Int = {
    val id = scopeIds.incrementAndGet()
    scopeMarker(kind, executor, batch, id, "BEGIN", plan)
    id
  }

  def endScope[TreeType <: TreeNode[_]](
      kind: String,
      executor: String,
      batch: String,
      scopeId: Int,
      plan: TreeType): Unit = {
    scopeMarker(kind, executor, batch, scopeId, "END", plan)
  }
"""

REPLACEMENTS = [
    (
        "  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)\n",
        "  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)\n"
        + SCOPE_METHODS,
    ),
    (
        "  def execute(plan: TreeType): TreeType = {\n    var curPlan = plan\n",
        '  def execute(plan: TreeType): TreeType = {\n'
        '    val qpExec = QueryProofAstTrace.beginScope("EXECUTOR", name, "", plan)\n'
        '    var curPlan = plan\n',
    ),
    (
        "    batches.foreach { batch =>\n      val batchStartPlan = curPlan\n",
        '    batches.foreach { batch =>\n'
        '      val qpBatch = QueryProofAstTrace.beginScope("BATCH", name, batch.name, curPlan)\n'
        '      val batchStartPlan = curPlan\n',
    ),
    (
        "      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)\n",
        '      QueryProofAstTrace.endScope("BATCH", name, batch.name, qpBatch, curPlan)\n'
        '      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)\n',
    ),
    (
        "    planChangeLogger.logMetrics(name, RuleExecutor.getCurrentMetrics()"
        " - beforeMetrics)\n\n"
        "    curPlan\n",
        "    planChangeLogger.logMetrics(name, RuleExecutor.getCurrentMetrics()"
        " - beforeMetrics)\n"
        '    QueryProofAstTrace.endScope("EXECUTOR", name, "", qpExec, curPlan)\n\n'
        "    curPlan\n",
    ),
]


def git_blob_sha(content):
    raw = content.encode("utf-8")
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def patch_text(source):
    if git_blob_sha(source) != EXPECTED_BLOB:
        raise ValueError("Refusing to change a non-pinned RuleExecutor source")
    for old, replacement in REPLACEMENTS:
        if source.count(old) != 1:
            raise ValueError("Unexpected RuleExecutor layout: " + repr(old[:60]))
        source = source.replace(old, replacement)
    if "QUERYPROOF_SCOPE" not in source:
        raise ValueError("Scope instrumentation absent")
    return source


if __name__ == "__main__":
    original = SOURCE.read_text()
    updated = patch_text(original)
    SOURCE.write_text(updated)
    print("QUERYPROOF_EPHEMERAL_SCOPE_INSTRUMENTATION_READY")
