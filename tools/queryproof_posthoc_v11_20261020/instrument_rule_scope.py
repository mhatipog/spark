"""QueryProof V11: scope tag-write events to actual RuleExecutor invocations.

Patches only the disposable, already source-pinned V10 TreeNode checkout and
V9 RuleExecutor overlay. Mutation events outside a verified active invocation
become UNKNOWN. Nested invocations use a thread-local LIFO stack. This is
research instrumentation and not proof of complete Catalyst effect coverage.
"""
from pathlib import Path

TREE = Path("artifacts/queryproof_v10_source/TreeNode.scala")
RULE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

SCOPE_EXTRA = r'''
  private case class Invocation(
      id: Long, executor: String, batch: String, rule: String, iteration: Int)
  private val scopeSerial = new java.util.concurrent.atomic.AtomicLong(0L)
  private val scopes = new ThreadLocal[List[Invocation]] {
    override def initialValue(): List[Invocation] = Nil
  }

  /** Enter a traced rule invocation; MUST be paired in a finally block. */
  def begin(executor: String, batch: String, rule: String, iteration: Int): Long = {
    if (!enabled) return 0L
    val number = scopeSerial.incrementAndGet()
    val context = Invocation(number, executor, batch, rule, iteration)
    scopes.set(context :: scopes.get())
    emit("QUERYPROOF_RULE_SCOPE_V11_BEGIN", Seq(
      System.getProperty("queryproof.query.id", "unlabelled"),
      number.toString, executor, batch, rule, iteration.toString))
    number
  }

  def end(number: Long): Unit = {
    if (number == 0L) return
    scopes.get() match {
      case top :: tail if top.id == number =>
        emit("QUERYPROOF_RULE_SCOPE_V11_END", Seq(
          System.getProperty("queryproof.query.id", "unlabelled"),
          number.toString, top.executor, top.batch, top.rule, top.iteration.toString))
        scopes.set(tail)
      case _ =>
        emit("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN", Seq(
          System.getProperty("queryproof.query.id", "unlabelled"),
          number.toString, "UNKNOWN_INVOCATION_STACK_MISMATCH"))
        scopes.remove()
    }
  }

'''

REPLACEMENTS_TREE = [
    (
        "  private val encoder = java.util.Base64.getEncoder\n",
        "  private val encoder = java.util.Base64.getEncoder\n" + SCOPE_EXTRA,
    ),
    (
        """  private def prefix(node: TreeNode[_]): Seq[String] =
    Seq(System.getProperty("queryproof.query.id", "unlabelled"),
      serial.incrementAndGet().toString, identity(node).toString,
      node.getClass.getName)
""",
        """  private def prefix(node: TreeNode[_]): Seq[String] =
    Seq(System.getProperty("queryproof.query.id", "unlabelled"),
      serial.incrementAndGet().toString, identity(node).toString,
      node.getClass.getName,
      scopes.get().headOption.map(_.id.toString).getOrElse("UNATTRIBUTED"))
""",
    ),
    (
        '    } else if (tag.name == "FAILED_TO_EVALUATE") {\n',
        '    } else if (tag.name == "FAILED_TO_EVALUATE" && scopes.get().nonEmpty) {\n',
    ),
    (
        '''        fields :+ "UNKNOWN_UNTRACKED_TAG_NAME")
''',
        '''        fields :+ (if (scopes.get().isEmpty) "UNKNOWN_OUTSIDE_RULE_SCOPE"
                   else "UNKNOWN_UNTRACKED_TAG_NAME"))
''',
    ),
]

RULE_START = "            val result = if (\n"
RULE_END = "            }\n            val runTime = System.nanoTime() - startTime\n"

def replace_once(source: str, old: str, new: str, name: str) -> str:
    if source.count(old) != 1:
        raise ValueError("REFUSE unexpected V11 source anchor: " + name)
    return source.replace(old, new)


def patch_treenode(source: str) -> str:
    if "QUERYPROOF_V10" not in source and "QUERYPROOF_TAG_MUTATION_V10" not in source:
        raise ValueError("V11 requires the pinned V10 source-hook overlay")
    if "QUERYPROOF_RULE_SCOPE_V11" in source:
        raise ValueError("V11 TreeNode already patched")
    for i, (before, after) in enumerate(REPLACEMENTS_TREE):
        source = replace_once(source, before, after, "TreeNode patch " + str(i))
    return source


def patch_rule_executor(source: str) -> str:
    if source.count("QueryProofAstTraceV9.finish(") != 1:
        raise ValueError("V11 requires the existing V9 invocation instrumentation")
    if "QueryProofTreeNodeTagEffects.begin(" in source:
        raise ValueError("V11 RuleExecutor already patched")
    source = replace_once(source, RULE_START, (
        '            val qpV11Scope =\n'
        '              org.apache.spark.sql.catalyst.trees.QueryProofTreeNodeTagEffects.begin(\n'
        '                name, batch.name, rule.ruleName, iteration)\n'
        '            val result = try {\n'
        '              if (\n'
    ), "before rule invocation")
    source = replace_once(source, RULE_END, (
        '              }\n'
        '            } finally {\n'
        '              org.apache.spark.sql.catalyst.trees.QueryProofTreeNodeTagEffects.end(qpV11Scope)\n'
        '            }\n'
        '            val runTime = System.nanoTime() - startTime\n'
    ), "after rule invocation")
    return source


if __name__ == "__main__":
    rewritten_tree = patch_treenode(TREE.read_text())
    rewritten_rule = patch_rule_executor(RULE.read_text())
    TREE.write_text(rewritten_tree)
    RULE.write_text(rewritten_rule)
    print("QUERYPROOF_V11_EPHEMERAL_RULE_SCOPED_NATIVE_TAG_HOOKS_READY")
