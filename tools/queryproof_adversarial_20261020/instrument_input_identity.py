"""Versioned, disposable JVM input-identity observer for genuine RuleExecutor captures.

Apply after the existing scope, state and frame overlays. Each AST or SCOPE
marker is followed by STATE, FRAME, then INPUT with exactly matching context.
All tokens are stable JVM-reference identities, not Catalyst JSON equality.
Unknown leaves are deliberately marked UNKNOWN and must never certify.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")

METHODS = r'''
  private val queryProofInputIds =
    new java.util.IdentityHashMap[AnyRef, java.lang.Long]()
  private var queryProofNextInputId = 0L

  private def queryProofReferenceToken(ref: AnyRef): Long = synchronized {
    val prior = queryProofInputIds.get(ref)
    if (prior != null) prior.longValue()
    else {
      queryProofNextInputId += 1L
      val next = java.lang.Long.valueOf(queryProofNextInputId)
      queryProofInputIds.put(ref, next)
      next.longValue()
    }
  }

  private def queryProofLeafInputs(root: TreeNode[_]): String = {
    import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
    import org.apache.spark.sql.execution.LogicalRDD

    val leaves = scala.collection.mutable.ArrayBuffer.empty[String]
    root match {
      case plan: LogicalPlan =>
        plan.foreach {
          case p: LogicalPlan if p.children.isEmpty =>
            val cls = p.getClass.getName
            val inputToken = p match {
              case r: LogicalRDD =>
                "rdd#" + queryProofReferenceToken(r.rdd.asInstanceOf[AnyRef])
              case _: OneRowRelation =>
                "intrinsic#one-row"
              case _ =>
                "UNKNOWN"
            }
            leaves += cls + "|" + inputToken
          case _ =>
        }
      case _ =>
    }
    leaves.mkString("~")
  }
'''

FRAME_ANCHOR = (
    '    System.err.println("QUERYPROOF_FRAME\\\\t" + frames.map(b64).mkString("\\\\t"))\n'
)
INPUT_APPEND = (
    '    val inputs = Seq(query) ++ context ++ plans.map(queryProofLeafInputs)\n'
    '    System.err.println("QUERYPROOF_INPUT\\\\t" + inputs.map(b64).mkString("\\\\t"))\n'
)


def patch_text(source):
    if source.count("  private def stateMarker(") != 1:
        raise ValueError("Missing required state/frame overlay")
    if source.count(FRAME_ANCHOR) != 1:
        raise ValueError("Expected unique hashed frame output")
    if "queryProofLeafInputs" in source:
        raise ValueError("Input overlay already applied")
    result = source.replace("  private def stateMarker(",
                            METHODS + "  private def stateMarker(")
    return result.replace(FRAME_ANCHOR, FRAME_ANCHOR + INPUT_APPEND)


if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_EPHEMERAL_INPUT_IDENTITY_OBSERVER_READY")
