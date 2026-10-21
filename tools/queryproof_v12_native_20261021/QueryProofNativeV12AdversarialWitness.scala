/*
 * V12 native falsification controls using real Spark 4.2 RuleExecutor:
 * (1) metadata-only rule rewrite with identical Spark toJSON must fail
 *     the exact ExprId native-field preservation attestation;
 * (2) in-place tag change on an unchanged plan demonstrates that value-
 *     field preservation is insufficient for hidden-state soundness;
 * (3) transient set/unset tag demonstrates that boundary frames can
 *     agree despite an actual intervening mutation.
 *
 * All three rules are TEST ONLY. No claim that Spark production is buggy.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse

object QueryProofNativeV12AdversarialWitness {
  private val alpha = new MetadataBuilder().putString("qp-value", "alpha").build()
  private val beta = new MetadataBuilder().putString("qp-value", "beta").build()
  private val nativeTag = TreeNodeTag[Boolean]("queryproof-test-only-native-tag")
  private val transientWrites = new AtomicInteger(0)

  object ChangeOnlyMetadataV12 extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case r: LocalRelation =>
        LocalRelation(r.output.map {
          case a: AttributeReference => a.withMetadata(beta)
          case _ => throw new IllegalArgumentException("non-AttributeReference source")
        })
      case _ => plan
    }
  }
  object SetOnlyTagV12 extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      assert(!plan.containsTag(nativeTag))
      plan.setTagValue(nativeTag, true)
      assert(plan.containsTag(nativeTag))
      plan
    }
  }
  object TransientTagV12 extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      assert(!plan.containsTag(nativeTag))
      plan.setTagValue(nativeTag, true)
      assert(plan.containsTag(nativeTag))
      transientWrites.incrementAndGet()
      plan.unsetTagValue(nativeTag)
      assert(!plan.containsTag(nativeTag))
      transientWrites.incrementAndGet()
      plan
    }
  }

  object MetadataExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12OnlyMetadata", Once, ChangeOnlyMetadataV12))
  }
  object TagExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12OnlyTag", Once, SetOnlyTagV12))
  }
  object TransientExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V12TransientTag", Once, TransientTagV12))
  }

  private def input(): LogicalPlan = {
    val a = AttributeReference("key", IntegerType, nullable = true, alpha)(
      ExprId(120013L, new UUID(0L, 16L)), Seq.empty)
    LocalRelation(Seq(a))
  }
  private def run(plan: LogicalPlan,
      executor: RuleExecutor[LogicalPlan]): (LogicalPlan, String) = {
    val previous = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      executor.execute(plan)
    } finally {
      sink.flush()
      System.setErr(previous)
      sink.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }
  private def records(log: String, marker: String): Seq[Array[String]] =
    log.split("\n").filter(_.startsWith(marker + "\t")).toSeq.map {
      line => line.trim.split("\t").drop(1).map(part =>
        new String(Base64.getDecoder.decode(part), StandardCharsets.UTF_8))
    }
  private def countFrame(frame: String, field: String): BigInt =
    (parse(frame) \ field) match {
      case JInt(value) => value
      case bad => throw new IllegalStateException("bad frame field: " + bad)
    }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v12.enabled", "true")
    System.setProperty("queryproof.ast.v12.test.enabled", "true")
    System.setProperty("queryproof.query.id", "V12_NATIVE_ADVERSARIAL_CONTROL")
    try {
      val before = input()
      val (after, metadataLog) = run(before, MetadataExecutor)
      assert(!before.fastEquals(after), "metadata-only rule must be effective")
      assert(before.toJSON == after.toJSON,
        "metadata-only variant must collide under official Spark TreeNode.toJSON")
      assert(before.output.head.metadata != after.output.head.metadata)
      val altered = records(metadataLog, "QUERYPROOF_NATIVE_V12")
      assert(altered.length == 1 && altered.head.length == 11,
        "native source observer absent on effective metadata-only rule")
      assert(altered.head(3).endsWith("ChangeOnlyMetadataV12"))
      assert(altered.head(5) == "true")
      assert(altered.head(8) == "false",
        "strict JVM object state erroneously admitted metadata mutation")
      assert(altered.head(10) == "false",
        "native ExprId value-field attestation falsely admitted metadata mutation")
      assert(altered.head(6) != altered.head(7),
        "native metadata hashes must differ across metadata-only rewrite")
      assert(records(metadataLog, "QUERYPROOF_NATIVE_V12_UNKNOWN").isEmpty)
      println("QUERYPROOF_V12_ACTUAL_JVM_METADATA_MUTATION_REJECTED")
      println("QUERYPROOF_V12_OFFICIAL_JSON_COLLISION_REPRODUCED")

      val tagBefore = input()
      val (tagAfter, tagLog) = run(tagBefore, TagExecutor)
      assert(tagBefore eq tagAfter, "tag-only rule should return the same JVM object")
      assert(tagAfter.containsTag(nativeTag))
      val tagEvents = records(tagLog, "QUERYPROOF_NATIVE_V12")
      assert(tagEvents.length == 1 && tagEvents.head.length == 11)
      assert(tagEvents.head(5) == "false",
        "tag-only rule must not be structurally effective")
      assert(tagEvents.head(8) == "true" && tagEvents.head(10) == "true",
        "tag-only rule should not change native NamedExpression values")
      assert(countFrame(tagEvents.head(6), "tagged_nodes") == BigInt(0))
      assert(countFrame(tagEvents.head(7), "tagged_nodes") == BigInt(1))
      println("QUERYPROOF_V12_TAG_EFFECT_NOT_EXCLUDED_BY_NATIVE_EXPRID_FIELDS")

      val transientBefore = input()
      val (transientAfter, transientLog) = run(transientBefore, TransientExecutor)
      assert(transientBefore eq transientAfter)
      assert(!transientAfter.containsTag(nativeTag))
      assert(transientWrites.get() == 2)
      val transientEvents = records(transientLog, "QUERYPROOF_NATIVE_V12")
      assert(transientEvents.length == 1 && transientEvents.head.length == 11)
      assert(transientEvents.head(5) == "false")
      assert(transientEvents.head(6) == transientEvents.head(7),
        "test-only transient effect must evade even source-level boundary snapshots")
      assert(transientEvents.head(8) == "true" && transientEvents.head(10) == "true")
      println("QUERYPROOF_V12_TRANSIENT_HIDDEN_EFFECT_EVADED_BOUNDARY_SNAPSHOTS")
      println("QUERYPROOF_V12_NATIVE_ADVERSARIAL_CONTROLS_COMPLETE")
    } finally {
      System.clearProperty("queryproof.ast.v12.test.enabled")
      System.clearProperty("queryproof.ast.v12.enabled")
      System.clearProperty("queryproof.ast.enabled")
      System.clearProperty("queryproof.query.id")
    }
  }
}
