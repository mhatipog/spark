/* V15: Production-only hidden-state adversary. Real Spark
 * PropagateEmptyRelation itself sets/unsets ROOT_REPARTITION and consults
 * TreeNode's rule-pruning cache. No test-only tag/plan mutation.
 * Compare two RuleExecutor passes over the same unchanged plan.
 * No assertion of wrong SQL output or a Spark production bug.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

object QueryProofNativeV15ProductionRepartitionWitness {
  class Production extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Seq(Batch("V15RealPropagateEmptyRelation", Once, PropagateEmptyRelation))
  }

  def captured[T](body: => T): (T, String) = {
    val previous = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      body
    } finally {
      sink.flush()
      System.setErr(previous)
      sink.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  def records(log: String, marker: String): Seq[Seq[String]] = {
    log.linesIterator.filter(_.startsWith(marker + "\t")).map { line =>
      line.split("\t").drop(1).toSeq.map { field =>
        new String(Base64.getDecoder.decode(field), StandardCharsets.UTF_8)
      }
    }.toSeq
  }

  def forRepartition(log: String, kind: String): Seq[Seq[String]] =
    records(log, "QUERYPROOF_RULE_PRUNING_V14").filter { row =>
      row.length == 5 &&
      row(0) == "V15_PRODUCTION_ROOT_REPARTITION" &&
      row(1) == kind &&
      row(3).endsWith(".Repartition")
    }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "true")
    System.setProperty("queryproof.ast.v8.enabled", "true")
    System.setProperty("queryproof.ast.v9.enabled", "true")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.rule.pruning.v14.enabled", "true")
    System.setProperty("queryproof.query.id", "V15_PRODUCTION_ROOT_REPARTITION")

    val root = Repartition(2, shuffle = true, child = LocalRelation(Nil))
    val plan: LogicalPlan = root
    val structuralJSON = plan.toJSON
    assert(!root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION))

    val (first, firstLog) = captured {
      new Production().execute(plan)
    }
    assert(first.fastEquals(plan))
    assert(plan.toJSON == structuralJSON)
    assert(!root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION),
      "production rule did not remove root repartition tag on first run")
    val firstCacheMarks = forRepartition(firstLog, "MARK")
    assert(firstCacheMarks.nonEmpty, "no actual rule ineffective cache mark")
    val firstUnknown = records(firstLog, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN")
    assert(firstUnknown.length >= 2, "source tag observer must reject unsupported ROOT tag")
    assert(firstUnknown.exists(_.contains("SET")))
    assert(firstUnknown.exists(_.contains("UNSET")))
    println("QUERYPROOF_V15_PRODUCTION_TAG_SET_UNSET_FIRST_PASS")
    println("QUERYPROOF_V15_PRODUCTION_REPARTITION_RULE_CACHE_MARK")

    val (second, secondLog) = captured {
      new Production().execute(plan)
    }
    assert(second.fastEquals(plan))
    assert(plan.toJSON == structuralJSON)
    assert(root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION),
      "source tree unexpectedly did not retain tag after cached rule skip")
    assert(secondLog.contains("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t"))
    val secondCacheSkips = forRepartition(secondLog, "SKIP")
    assert(firstCacheMarks.exists(mark =>
      secondCacheSkips.exists(skip =>
        mark(2) == skip(2) && mark(4) == skip(4))),
      "could not pair production MARK and SKIP on exact root and rule")
    val secondUnknown = records(secondLog, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN")
    assert(secondUnknown.exists(_.contains("SET")))
    println("QUERYPROOF_V15_PRODUCTION_SAME_ROOT_RULE_MARK_THEN_SKIP")
    println("QUERYPROOF_V15_IDENTICAL_AST_DIFFERENT_HIDDEN_TAG_AFTER_REPLAY")
    println("QUERYPROOF_V15_ORIGINAL_V11_DOMAIN_CORRECTLY_REPORTS_UNKNOWN")
    println("QUERYPROOF_V15_NOT_ROW_BAG_MISMATCH_NOT_SPARK_BUG_CLAIM")
  }
}
