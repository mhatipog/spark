/* QueryProof native Spark 4.2.0 tag-only rule capture adversary.
 * Dev-only; no private data, SQL contents, or production Spark changes.
 *
 * This intentionally creates a rule whose result has the SAME object
 * identity and serialized plan, but DIFFERENT private mutable TreeNode tags.
 * The second rule changes and restores the tag within one invocation.
 *
 * It must not be mistaken for evidence of an optimizer correctness bug.
 * It is a counterexample to the assumption that the changed-plan event
 * stream records every relevant JVM mutation.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

object QueryProofNativeInPlaceTagWitness {
  private val testTag = TreeNodeTag[String]("qp-native-inplace-witness-20261020")

  object PersistentTagRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.setTagValue(testTag, "changed-inside-rule")
      plan
    }
  }

  object RestoreTagRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.setTagValue(testTag, "temporary-value")
      assert(plan.getTagValue(testTag).contains("temporary-value"))
      plan.unsetTagValue(testTag)
      plan
    }
  }

  object ChangePlanRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      Filter(Literal(true), plan)
  }

  object PersistentExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("NativePersistentTag", Once, PersistentTagRule))
  }

  object RestoreExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("NativeRestoreTag", Once, RestoreTagRule))
  }

  object StructuralExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("NativeStructuralControl", Once, ChangePlanRule))
  }

  private def capture(body: => LogicalPlan): (LogicalPlan, String) = {
    val save = System.err
    val bytes = new ByteArrayOutputStream()
    val recorder = new PrintStream(bytes, true, "UTF-8")
    System.setErr(recorder)
    try {
      val plan = body
      recorder.flush()
      (plan, new String(bytes.toByteArray, StandardCharsets.UTF_8))
    } finally {
      System.setErr(save)
      recorder.close()
    }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.query.id", "NATIVE_TAG_ONLY_DIAGNOSTIC")

    val p = OneRowRelation()
    assert(p.getTagValue(testTag).isEmpty)
    val before = p.toJSON
    val (changed, persistentTrace) = capture {
      PersistentExecutor.execute(p)
    }
    assert(changed.asInstanceOf[AnyRef] eq p.asInstanceOf[AnyRef])
    assert(changed.fastEquals(p))
    assert(changed.toJSON == before)
    assert(changed.getTagValue(testTag).contains("changed-inside-rule"))
    assert(!persistentTrace.contains("QUERYPROOF_AST\t"))
    println("QUERYPROOF_NATIVE_PERSISTENT_TAG_MUTATION_INVISIBLE_TO_CHANGED_RULE_STREAM")

    val clean = OneRowRelation()
    val cleanJSON = clean.toJSON
    val (restored, transientTrace) = capture {
      RestoreExecutor.execute(clean)
    }
    assert(restored.asInstanceOf[AnyRef] eq clean.asInstanceOf[AnyRef])
    assert(restored.toJSON == cleanJSON)
    assert(restored.getTagValue(testTag).isEmpty)
    assert(!transientTrace.contains("QUERYPROOF_AST\t"))
    println("QUERYPROOF_NATIVE_TRANSIENT_TAG_WRITE_INVISIBLE_TO_BOUNDARY_OBSERVERS")

    val (realChange, positiveTrace) = capture {
      StructuralExecutor.execute(OneRowRelation())
    }
    assert(realChange.isInstanceOf[Filter])
    assert(positiveTrace.contains("QUERYPROOF_AST\t"),
      "Positive control: instrumented RuleExecutor did not record real plan change")
    println("QUERYPROOF_NATIVE_POSITIVE_CONTROL_STRUCTURAL_RULE_WAS_CAPTURED")
  }
}
