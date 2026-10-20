"""V14 cache instrumenter: source-pinned, disposable Spark TreeNode only.

Observes real _ineffectiveRules marks and hits (not a completeness claim).
Requires the V10 blob pin plus existing V11 RuleExecutor/TreeNode hooks.
"""
from pathlib import Path

TREE = Path("artifacts/queryproof_v10_source/TreeNode.scala")
OBJECT_ANCHOR = "// A functor that always returns true.\n"
MARK = """    ineffectiveRules.set(ruleId.id)
  }
"""
HIT = """    ineffectiveRules.get(ruleId.id)
  }
"""
OBJECT = r'''
/** QueryProof V14: EPHEMERAL production rule-pruning-cache observer.
 * Correlates marks and skips by exact JVM-local TreeNode identity.
 * Does not observe every non-TreeNode state field or prove SQL semantics.
 */
private[catalyst] object QueryProofRulePruningEffectsV14 {
  private val identities = new java.util.IdentityHashMap[AnyRef, java.lang.Long]()
  private val nextId = new java.util.concurrent.atomic.AtomicLong(0L)
  private val encoder = java.util.Base64.getEncoder

  def enabled: Boolean =
    java.lang.Boolean.getBoolean("queryproof.rule.pruning.v14.enabled")

  private def identity(node: AnyRef): Long = this.synchronized {
    val old = identities.get(node)
    if (old != null) old.longValue()
    else {
      val id = nextId.incrementAndGet()
      identities.put(node, java.lang.Long.valueOf(id))
      id
    }
  }

  private def b64(value: String): String = encoder.encodeToString(
    value.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  def observe(kind: String, node: TreeNode[_], ruleId: Int): Unit = {
    if (!enabled) return
    val fields = Seq(
      System.getProperty("queryproof.query.id", "unlabelled"),
      kind,
      ruleId.toString,
      node.getClass.getName,
      identity(node).toString)
    System.err.println(
      "QUERYPROOF_RULE_PRUNING_V14\t" + fields.map(b64).mkString("\t"))
  }
}

'''

def replace_once(source: str, old: str, new: str, anchor: str) -> str:
    if source.count(old) != 1:
        raise ValueError("REFUSE V14 source drift: " + anchor)
    return source.replace(old, new)


def patch(source: str) -> str:
    if "QUERYPROOF_RULE_SCOPE_V11_BEGIN" not in source:
        raise ValueError("REFUSE: requires source-pinned V11 TreeNode")
    if "QUERYPROOF_RULE_PRUNING_V14" in source:
        raise ValueError("REFUSE duplicate V14 cache patch")
    source = replace_once(source, OBJECT_ANCHOR, OBJECT + OBJECT_ANCHOR, "object")
    source = replace_once(
        source, MARK,
        """    ineffectiveRules.set(ruleId.id)
    QueryProofRulePruningEffectsV14.observe("MARK", this, ruleId.id)
  }
""", "cache mark")
    source = replace_once(
        source, HIT,
        """    val qpV14Cached = ineffectiveRules.get(ruleId.id)
    if (qpV14Cached) {
      QueryProofRulePruningEffectsV14.observe("SKIP", this, ruleId.id)
    }
    qpV14Cached
  }
""", "cache skip")
    return source


if __name__ == "__main__":
    TREE.write_text(patch(TREE.read_text()))
    print("QUERYPROOF_V14_SOURCE_PINNED_RULE_PRUNING_CACHE_HOOKS_READY")
