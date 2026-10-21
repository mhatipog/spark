/*
 * V14 controls for the stored-vs-derived identity diagnostic.
 *
 * 1. Production InferFiltersFromConstraints on a synthetic aliased join:
 *    predicts V12 strict identity REJECTS while V14 stored identity is clean,
 *    and that V12's missing count equals V14's SELF derived re-allocation.
 * 2. V12 metadata-only adversary must be visible in V14 stored identity.
 * 3. Tag-only and transient-tag adversaries stay invisible (documented limit).
 * Test-only; never part of production Spark.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, LessThan}
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

object QueryProofNativeV14Controls {
  import QueryProofNativeV12AdversarialWitness.{MetadataExecutor, TagExecutor, TransientExecutor}

  object InferExecutor extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V14InferOnce", Once, InferFiltersFromConstraints))
  }

  private val jvm = new UUID(0L, 21L)
  private def attr(name: String, id: Long) =
    AttributeReference(name, IntegerType, nullable = true)(ExprId(id, jvm), Seq.empty)

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
  private def counts(csv: String): Seq[Int] = {
    val parts = csv.split(",").map(_.toInt).toSeq
    assert(parts.length == 4, "malformed V14 diff " + csv)
    parts
  }
  private def only(log: String, marker: String, arity: Int): Array[String] = {
    val found = records(log, marker)
    assert(found.length == 1 && found.head.length == arity,
      s"expected exactly one $marker record, got ${found.length}")
    found.head
  }
  private def report(tag: String, v14: Array[String]): Unit =
    println(s"QUERYPROOF_V14_CONTROL $tag self=${v14(6)} stored=${v14(7)} derived=${v14(8)}")

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v12.enabled", "true")
    System.setProperty("queryproof.ast.v12.test.enabled", "true")
    System.setProperty("queryproof.ast.v14.enabled", "true")
    System.setProperty("queryproof.query.id", "V14_STORED_IDENTITY_CONTROL")
    try {
      // 1. Real production rule; aliased projection above a strict inner join.
      val a = attr("a", 140001L)
      val b = attr("b", 140002L)
      val join = Join(LocalRelation(Seq(a)), LocalRelation(Seq(b)), Inner,
        Some(LessThan(a, b)), JoinHint.NONE)
      val before = Project(Seq(Alias(a, "pa")(exprId = ExprId(140003L, jvm)), b), join)
      val (after, log) = run(before, InferExecutor)
      assert(!before.fastEquals(after), "InferFiltersFromConstraints must add guards")
      val v12 = only(log, "QUERYPROOF_NATIVE_V12", 11)
      val v14 = only(log, "QUERYPROOF_NATIVE_V14", 9)
      report("infer_aliased_join", v14)
      val v12Missing = v12(9).split(",")(0).toInt
      val Seq(selfStored, selfDerived) = v14(6).split(";").toSeq.map(counts)
      assert(v12(8) == "false",
        "prediction: V12 strict identity rejects a legitimate aliased rewrite")
      assert(selfStored == Seq(0, 0, 0, 0), "stored identity unstable on SAME plan")
      assert(selfDerived(0) > 0, "prediction: Alias output re-allocated per output call")
      assert(selfDerived(0) == v12Missing,
        s"V12 missing $v12Missing not explained by self re-allocation ${selfDerived(0)}")
      val stored = counts(v14(7))
      assert(stored(0) == 0 && stored(1) == 0 && stored(3) == 0,
        "stored identity must be preserved by InferFiltersFromConstraints here")
      println("QUERYPROOF_V14_V12_STRICT_FAILURE_EXPLAINED_BY_DERIVED_REALLOCATION")

      // 2. Metadata-only adversary must be seen by stored identity.
      val alpha = new MetadataBuilder().putString("qp-value", "alpha").build()
      val k = AttributeReference("key", IntegerType, nullable = true, alpha)(
        ExprId(120013L, new UUID(0L, 16L)), Seq.empty)
      val (_, metaLog) = run(LocalRelation(Seq(k)), MetadataExecutor)
      val meta = only(metaLog, "QUERYPROOF_NATIVE_V14", 9)
      report("metadata_only", meta)
      val m = counts(meta(7))
      assert(m(0) + m(1) > 0 && m(3) > 0,
        "V14 stored identity failed to expose metadata-only replacement")
      println("QUERYPROOF_V14_METADATA_ADVERSARY_REJECTED_BY_STORED_IDENTITY")

      // 3. Tag adversaries: stored identity is blind by construction.
      for ((name, executor) <- Seq("tag_only" -> TagExecutor, "transient_tag" -> TransientExecutor)) {
        val (_, tagLog) = run(LocalRelation(Seq(attr("t", 140010L))), executor)
        val t = only(tagLog, "QUERYPROOF_NATIVE_V14", 9)
        report(name, t)
        assert(counts(t(7)) == Seq(0, 0, 0, 0),
          "unexpected: tag-only rule changed stored identity")
      }
      println("QUERYPROOF_V14_TAG_EFFECTS_REMAIN_OUTSIDE_STORED_IDENTITY")
      println("QUERYPROOF_V14_CONTROLS_COMPLETE")
    } finally {
      Seq("queryproof.ast.enabled", "queryproof.ast.v12.enabled",
        "queryproof.ast.v12.test.enabled", "queryproof.ast.v14.enabled",
        "queryproof.query.id").foreach(System.clearProperty)
    }
  }
}
