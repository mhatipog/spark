"""V12: seal the narrow V11 source-event channel on an ephemeral Spark overlay.

The upstream TreeNode SHA check is performed by the V10 patch step. This
instrumenter accepts exactly the V11 logger, refuses patch drift, and does
not claim to observe arbitrary Catalyst mutations or authenticate the JVM.
"""
from pathlib import Path

TREE = Path("artifacts/queryproof_v10_source/TreeNode.scala")
ANCHOR = '''  private def emit(marker: String, fields: Seq[String]): Unit =
    System.err.println(marker + "\\t" + fields.map(b64).mkString("\\t"))
'''

REPLACEMENT = '''  // V12 SEAL: only V11 logger events, not other Spark diagnostics.
  // The checksum catches accidental/intermediate transcript corruption.
  // A writer capable of replacing the transcript and seal can forge both.
  private val v12Digest = java.security.MessageDigest.getInstance("SHA-256")
  private var v12Count: Long = 0L
  private var v12Unknown: Long = 0L
  private var v12Sealed: Boolean = false

  private def emit(marker: String, fields: Seq[String]): Unit = this.synchronized {
    val line = marker + "\\t" + fields.map(b64).mkString("\\t")
    if (v12Sealed) {
      System.err.println("QUERYPROOF_V12_UNKNOWN_AFTER_SEAL")
    } else {
      v12Digest.update(
        (line + "\\n").getBytes(java.nio.charset.StandardCharsets.UTF_8))
      v12Count += 1
      if (marker.endsWith("_UNKNOWN")) v12Unknown += 1
      System.err.println(line)
    }
  }

  /** Called only after the final rule. Never a SQL-equivalence certificate. */
  def seal(): Unit = this.synchronized {
    if (v12Sealed) {
      System.err.println("QUERYPROOF_V12_UNKNOWN_DOUBLE_SEAL")
    } else {
      v12Sealed = true
      val hex = v12Digest.digest().map(b =>
        "%02x".format(b & 0xff)).mkString
      val fields = Seq(
        System.getProperty("queryproof.query.id", "unlabelled"),
        v12Count.toString,
        serial.get().toString,
        scopeSerial.get().toString,
        v12Unknown.toString,
        scopes.get().size.toString,
        hex)
      System.err.println(
        "QUERYPROOF_V12_SEAL\\t" + fields.map(b64).mkString("\\t"))
    }
  }
'''

def patch(source: str) -> str:
    if source.count(ANCHOR) != 1:
        raise ValueError("REFUSE: V11 logger anchor changed or already patched")
    for marker in ("QUERYPROOF_RULE_SCOPE_V11_BEGIN",
                   "QUERYPROOF_RULE_SCOPE_V11_END",
                   "QUERYPROOF_TAG_MUTATION_V11"):
        if marker not in source:
            raise ValueError("REFUSE: absent expected V11 marker " + marker)
    if "QUERYPROOF_V12_SEAL" in source:
        raise ValueError("REFUSE: V12 seal already exists")
    return source.replace(ANCHOR, REPLACEMENT)

if __name__ == "__main__":
    TREE.write_text(patch(TREE.read_text()))
    print("QUERYPROOF_V12_NARROW_SOURCE_EVENT_SEAL_PATCH_READY")
