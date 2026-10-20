"""QueryProof V18 — source-pinned, observer-isolated TreeNode ROOT tag read hook.

Requires disposable official Spark 4.2.0 TreeNode already patched by V10/V11/V14.
Records real getTagValue reads of ROOT_REPARTITION on the source method.
Suppresses only reads injected by our V10 write observer's before/after
bookkeeping. No claim that it covers every possible hidden-state read.
"""
from pathlib import Path

TREE = Path("artifacts/queryproof_v10_source/TreeNode.scala")
ANCHOR = "// A functor that always returns true.\n"

GET = """  def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
    if (isTagsEmpty) {
      None
    } else {
      tags.get(tag).map(_.asInstanceOf[T])
    }
  }
"""

LOGGER = r'''
/** V18: EPHEMERAL ROOT_REPARTITION read-side observer only.
 * V10's extra bookkeeping calls to containsTag are muted to prevent
 * observer self-interference with the production read trace.
 */
private[catalyst] object QueryProofTagReadsV18 {
  private val mute = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
  private val encoder = java.util.Base64.getEncoder

  def silentContainsTag[T](node: TreeNode[_], tag: TreeNodeTag[T]): Boolean = {
    val saved = mute.get()
    mute.set(true)
    try node.containsTag(tag)
    finally mute.set(saved)
  }

  def record[T](node: TreeNode[_], tag: TreeNodeTag[T], present: Boolean): Unit = {
    if (!java.lang.Boolean.getBoolean("queryproof.tag.read.v18.enabled")) return
    if (mute.get() || tag.name != "ROOT_REPARTITION") return
    val fields = Seq(
      System.getProperty("queryproof.query.id", "unlabelled"),
      "READ",
      node.getClass.getName,
      QueryProofRulePruningEffectsV14.identityForRead(node).toString,
      tag.name,
      present.toString)
    System.err.println("QUERYPROOF_TAG_READ_V18\t" +
      fields.map(s => encoder.encodeToString(
        s.getBytes(java.nio.charset.StandardCharsets.UTF_8))).mkString("\t"))
  }
}

'''

def one(source: str, before: str, after: str, label: str) -> str:
    if source.count(before) != 1:
        raise ValueError("V18 fail-closed source drift: " + label +
                         "; found " + str(source.count(before)))
    return source.replace(before, after)

def patch(source: str) -> str:
    if "QUERYPROOF_RULE_SCOPE_V11_BEGIN" not in source:
        raise ValueError("V18 requires V11 rule scopes")
    if "QUERYPROOF_RULE_PRUNING_V14" not in source:
        raise ValueError("V18 requires V14 source cache hook")
    if "QUERYPROOF_TAG_READ_V18" in source:
        raise ValueError("V18 duplicate patch")
    source = one(source, ANCHOR, LOGGER + ANCHOR, "observer object")
    v14_anchor = "  def observe(kind: String, node: TreeNode[_], ruleId: Int): Unit = {\n"
    source = one(
        source, v14_anchor,
        "  def identityForRead(node: TreeNode[_]): Long = identity(node)\n\n" +
        v14_anchor, "shared JVM identity for V18 reads")
    source = one(
        source, GET,
        """  def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
    val qpV18Value: Option[T] = if (isTagsEmpty) {
      None
    } else {
      tags.get(tag).map(_.asInstanceOf[T])
    }
    QueryProofTagReadsV18.record(this, tag, qpV18Value.isDefined)
    qpV18Value
  }
""", "read source")
    # Original V10's before/after read attempts are NOT production code.
    # They must not contaminate V18 events or cause spurious provenance.
    before = ("val qpBefore = if (QueryProofTreeNodeTagEffects.enabled) "
              "containsTag(tag) else false")
    if source.count(before) != 2:
        raise ValueError("V18 expected two V10 induced pre-write containsTag calls")
    source = source.replace(
        before, "val qpBefore = if (QueryProofTreeNodeTagEffects.enabled) " +
        "QueryProofTagReadsV18.silentContainsTag(this, tag) else false")
    for op in ("SET", "UNSET"):
        old = '"%s", this, tag, qpBefore, containsTag(tag))' % op
        new = ('"%s", this, tag, qpBefore, ' % op +
               "QueryProofTagReadsV18.silentContainsTag(this, tag))")
        source = one(source, old, new, op + " post-write bookkeeping")
    return source

if __name__ == "__main__":
    TREE.write_text(patch(TREE.read_text()))
    print("QUERYPROOF_V18_OBSERVER_ISOLATED_SOURCE_READ_HOOK_READY")
