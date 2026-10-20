"""QueryProof V10: strictly pinned disposable Spark TreeNode.scala mutation overlay.

Only modifies the temporary Spark 4.2.0 source checkout used by CI.
Preserves the source/tree in mhatipog/spark and all W32/X frozen artifacts.

This is an *experimental* source-site tag event producer. It is not a
complete optimizer-state observer, and unsupported bulk mutation paths emit
UNKNOWN rather than being silently certified.
"""
import hashlib
from pathlib import Path

SOURCE = Path("artifacts/queryproof_v10_source/TreeNode.scala")
EXPECTED_SPARK_BLOB = "e82e6a30b9bba41e637b27cbab43f26d9d23ff9b"
OBJECT_ANCHOR = "// A functor that always returns true.\n"

SET_ANCHOR = """  def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    tags(tag) = value
  }
"""
UNSET_ANCHOR = """  def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    if (!isTagsEmpty) {
      tags -= tag
    }
  }
"""
COPY_ANCHOR = """    if (isTagsEmpty && !other.isTagsEmpty) {
      tags ++= other.tags
    }
"""
MERGE_ANCHOR = """    if (!other.isTagsEmpty) {
      // Merge all tags from the other node into this node.
      // Unlike copyTagsFrom which only copies when this node has no tags,
      // mergeTagsFrom will always merge tags regardless of existing state.
      // If both nodes have the same tag with different values, the value
      // from the other node will overwrite the existing value in this node.
      tags ++= other.tags
    }
"""

TAG_LOGGER = r'''
/** QueryProof V10 EPHEMERAL source-site tag-write observer.
 * Only the production ConstantFolding failure-tag presence bit is within
 * its supported observational domain. Arbitrary tag keys and bulk copying
 * emit UNKNOWN. Sequence/node identities are JVM-local and experiment-only.
 *
 * Observing a write is NOT a semantic-noninterference proof, and in-place
 * mutations outside these source sites are not covered.
 */
private[catalyst] object QueryProofTreeNodeTagEffects {
  private val serial = new java.util.concurrent.atomic.AtomicLong(0L)
  private val nextId = new java.util.concurrent.atomic.AtomicLong(0L)
  private val objectIds =
    new java.util.IdentityHashMap[AnyRef, java.lang.Long]()
  private val encoder = java.util.Base64.getEncoder

  def enabled: Boolean =
    java.lang.Boolean.getBoolean("queryproof.tag.effects.v10.enabled")

  private def b64(s: String): String =
    encoder.encodeToString(
      s.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  private def identity(node: AnyRef): Long = this.synchronized {
    val old = objectIds.get(node)
    if (old != null) old.longValue()
    else {
      val number = nextId.incrementAndGet()
      objectIds.put(node, java.lang.Long.valueOf(number))
      number
    }
  }

  private def emit(marker: String, fields: Seq[String]): Unit =
    System.err.println(marker + "\t" + fields.map(b64).mkString("\t"))

  private def prefix(node: TreeNode[_]): Seq[String] =
    Seq(System.getProperty("queryproof.query.id", "unlabelled"),
      serial.incrementAndGet().toString, identity(node).toString,
      node.getClass.getName)

  def record[T](
      operation: String,
      node: TreeNode[_],
      tag: TreeNodeTag[T],
      oldPresent: Boolean,
      newPresent: Boolean): Unit = {
    if (!enabled) return
    val fields = prefix(node) ++ Seq(
      operation, tag.name, oldPresent.toString, newPresent.toString)
    val n = serial.get()
    if (n > 2500) {
      if (n == 2501) {
        emit("QUERYPROOF_TAG_MUTATION_V10_UNKNOWN",
          fields :+ "UNKNOWN_EVENT_LIMIT_EXCEEDED")
      }
    } else if (tag.name == "FAILED_TO_EVALUATE") {
      emit("QUERYPROOF_TAG_MUTATION_V10", fields)
    } else {
      emit("QUERYPROOF_TAG_MUTATION_V10_UNKNOWN",
        fields :+ "UNKNOWN_UNTRACKED_TAG_NAME")
    }
  }

  def bulk(operation: String, node: TreeNode[_]): Unit = {
    if (enabled) {
      emit("QUERYPROOF_TAG_MUTATION_V10_UNKNOWN",
        prefix(node) ++ Seq(operation, "UNKNOWN_BULK_TAG_OPERATION"))
    }
  }
}

'''

def git_blob_sha(source: str) -> str:
    raw = source.encode("utf-8")
    return hashlib.sha1(
        b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()

def patch_text(source: str) -> str:
    found = git_blob_sha(source)
    if found != EXPECTED_SPARK_BLOB:
        raise ValueError(
            "REFUSE unpinned official Spark TreeNode source: " + found)
    replacements = [
        (OBJECT_ANCHOR, TAG_LOGGER + OBJECT_ANCHOR),
        (SET_ANCHOR, """  def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    val qpBefore = if (QueryProofTreeNodeTagEffects.enabled) containsTag(tag) else false
    tags(tag) = value
    if (QueryProofTreeNodeTagEffects.enabled) {
      QueryProofTreeNodeTagEffects.record(
        "SET", this, tag, qpBefore, containsTag(tag))
    }
  }
"""),
        (UNSET_ANCHOR, """  def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    val qpBefore = if (QueryProofTreeNodeTagEffects.enabled) containsTag(tag) else false
    if (!isTagsEmpty) {
      tags -= tag
    }
    if (QueryProofTreeNodeTagEffects.enabled) {
      QueryProofTreeNodeTagEffects.record(
        "UNSET", this, tag, qpBefore, containsTag(tag))
    }
  }
"""),
        (COPY_ANCHOR, """    if (isTagsEmpty && !other.isTagsEmpty) {
      QueryProofTreeNodeTagEffects.bulk("COPY_TAGS", this)
      tags ++= other.tags
    }
"""),
        (MERGE_ANCHOR, """    if (!other.isTagsEmpty) {
      // Merge all tags from the other node into this node.
      // Unlike copyTagsFrom which only copies when this node has no tags,
      // mergeTagsFrom will always merge tags regardless of existing state.
      // If both nodes have the same tag with different values, the value
      // from the other node will overwrite the existing value in this node.
      QueryProofTreeNodeTagEffects.bulk("MERGE_TAGS", this)
      tags ++= other.tags
    }
"""),
    ]
    for old, new in replacements:
        if source.count(old) != 1:
            raise ValueError("REFUSE unexpected Spark source anchor " + old[:65])
        source = source.replace(old, new)
    if source.count("QUERYPROOF_TAG_MUTATION_V10") != 3:
        raise ValueError("Unexpected mutation tracer markers")
    return source

if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_V10_OFFICIAL_SPARK_SOURCE_BLOB_PIN_VERIFIED")
    print("QUERYPROOF_V10_EPHEMERAL_NATIVE_TAG_MUTATION_HOOKS_READY")
