/* V16 exploratory: stock Apache Spark Optimizer.defaultBatches.
 * Executes a simple actual root user Repartition with empty LocalRelation
 * through the normal batch list, not a synthetic repeated-rule executor.
 * CatalogManager is null: no catalog-dependent rule should be exercised
 * on this catalog-free input. A failure is recorded as a genuine limit.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Repartition}

object QueryProofNativeV16StockOptimizerWitness {
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
  def records(log: String, marker: String): Seq[Seq[String]] =
    log.linesIterator.filter(_.startsWith(marker + "\t")).map { line =>
      line.split("\t").drop(1).toSeq.map { f =>
        new String(Base64.getDecoder.decode(f), StandardCharsets.UTF_8)
      }
    }.toSeq

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "false")
    System.setProperty("queryproof.ast.v8.enabled", "false")
    System.setProperty("queryproof.ast.v9.enabled", "false")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.rule.pruning.v14.enabled", "true")
    System.setProperty("queryproof.query.id", "V16_STOCK_SPARK_OPTIMIZER")

    // Normal Catalyst optimizer batch schedule; catalog-free simple input.
    val optimizer = new Optimizer(null) {}
    val placements = optimizer.defaultBatches.flatMap { batch =>
      batch.rules.filter(_.ruleName.contains("PropagateEmptyRelation")).map(_ => batch.name)
    }
    assert(placements.contains("LocalRelation early"))
    assert(placements.contains("LocalRelation"))
    println("QUERYPROOF_V16_STOCK_DEFAULT_OPTIMIZER_TWO_PRODUCTION_BATCHES")

    val root = Repartition(2, shuffle = true, child = LocalRelation(Nil))
    val oldJSON = root.toJSON
    val (result, log) = captured {
      optimizer.execute(root)
    }
    val begins = records(log, "QUERYPROOF_RULE_SCOPE_V11_BEGIN")
      .filter(x => x.length == 6 && x(4).contains("PropagateEmptyRelation"))
    val productionBatches = begins.map(_(3))
    println("QUERYPROOF_V16_REAL_PRODUCTION_RULE_INVOCATIONS=" + begins.size)
    println("QUERYPROOF_V16_REAL_PRODUCTION_RULE_BATCHES=" +
      productionBatches.mkString("|"))
    assert(productionBatches.contains("LocalRelation early"))
    assert(productionBatches.contains("LocalRelation"))

    val unknown = records(log, "QUERYPROOF_TAG_MUTATION_V11_UNKNOWN")
    val cache = records(log, "QUERYPROOF_RULE_PRUNING_V14")
    val set = unknown.count(_.contains("SET"))
    val unset = unknown.count(_.contains("UNSET"))
    val mark = cache.count(_.contains("MARK"))
    val skip = cache.count(_.contains("SKIP"))
    println("QUERYPROOF_V16_SOURCE_TAG_UNKNOWN_SET=" + set)
    println("QUERYPROOF_V16_SOURCE_TAG_UNKNOWN_UNSET=" + unset)
    println("QUERYPROOF_V16_REAL_CACHE_MARKS=" + mark)
    println("QUERYPROOF_V16_REAL_CACHE_SKIPS=" + skip)
    println("QUERYPROOF_V16_ROOT_IDENTITY_PRESERVED=" + (result eq root))
    println("QUERYPROOF_V16_ROOT_STRUCTURAL_JSON_PRESERVED=" + (result.toJSON == oldJSON))
    println("QUERYPROOF_V16_ROOT_HIDDEN_TAG_AFTER=" +
      root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION))
    println("QUERYPROOF_V16_STOCK_OPTIMIZER_EXECUTION_COMPLETE")
    println("QUERYPROOF_V16_NO_ROW_BAG_OR_SPARK_BUG_CLAIM")
  }
}
