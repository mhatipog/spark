/* QueryProof V18: first native test for rule-ID + node-ID paired cache events
 * inside a single real Optimizer.defaultBatches execution.
 * Full source-scoped logs enforce correct batch attribution of hidden-tag
 * operations rather than using global mutation counts.
 * The source-pinned TreeNode is temporarily instrumented in GitHub CI.
 */
package org.apache.spark.sql.catalyst.optimizer

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Repartition}

object QueryProofNativeV18StockOptimizerCacheJoin {
  def captured[T](body: => T): (T, String) = {
    val saved = System.err
    val bytes = new ByteArrayOutputStream()
    val sink = new PrintStream(bytes, true, "UTF-8")
    val result = try {
      System.setErr(sink)
      body
    } finally {
      sink.flush()
      System.setErr(saved)
      sink.close()
    }
    (result, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  def decode(line: String): Seq[String] =
    line.split("\t", -1).drop(1).toSeq.map { field =>
      new String(Base64.getDecoder.decode(field), StandardCharsets.UTF_8)
    }

  def events(lines: Seq[String], marker: String): Seq[Seq[String]] =
    lines.filter(_.startsWith(marker + "\t")).map(decode)

  case class Evidence(
      batch: String,
      invocation: String,
      firstLine: Int,
      lastLine: Int,
      lines: Seq[String]) {
    def records(marker: String): Seq[Seq[String]] = events(lines, marker)
  }

  def findScope(lines: IndexedSeq[String], batch: String): Evidence = {
    val begins = lines.zipWithIndex.filter { case (line, _) =>
      line.startsWith("QUERYPROOF_RULE_SCOPE_V11_BEGIN\t")
    }.map { case (line, i) => (decode(line), i) }
    val candidates = begins.filter { case (r, _) =>
      r.length == 6 && r(3) == batch &&
        r(4).contains("PropagateEmptyRelation")
    }
    assert(candidates.length == 1,
      "V18 expected exactly one actual production invocation in batch " + batch)
    val (record, start) = candidates.head
    val id = record(1)
    val ends = lines.zipWithIndex.filter { case (line, _) =>
      line.startsWith("QUERYPROOF_RULE_SCOPE_V11_END\t")
    }.map { case (line, i) => (decode(line), i) }.filter { case (r, _) =>
      r.length == 6 && r(1) == id && r(3) == batch
    }
    assert(ends.length == 1)
    val end = ends.head._2
    assert(end > start)
    Evidence(batch, id, start, end, lines.slice(start, end + 1))
  }

  def rootTagEvents(scope: Evidence): Seq[Seq[String]] =
    scope.records("QUERYPROOF_TAG_MUTATION_V11_UNKNOWN").filter { r =>
      r.length >= 10 && r(4) == scope.invocation &&
        r(3).endsWith(".Repartition") && r(6) == "ROOT_REPARTITION"
    }

  def rootCache(scope: Evidence, kind: String): Seq[Seq[String]] =
    scope.records("QUERYPROOF_RULE_PRUNING_V14").filter { r =>
      r.length == 5 && r(1) == kind && r(3).endsWith(".Repartition")
    }

  def rootReads(scope: Evidence): Seq[Seq[String]] =
    scope.records("QUERYPROOF_TAG_READ_V18").filter { r =>
      r.length == 6 && r(1) == "READ" &&
        r(2).endsWith(".Repartition") && r(4) == "ROOT_REPARTITION"
    }

  def main(args: Array[String]): Unit = {
    System.setProperty("queryproof.ast.enabled", "false")
    System.setProperty("queryproof.ast.v8.enabled", "false")
    System.setProperty("queryproof.ast.v9.enabled", "false")
    System.setProperty("queryproof.tag.effects.v10.enabled", "true")
    System.setProperty("queryproof.rule.pruning.v14.enabled", "true")
    System.setProperty("queryproof.tag.read.v18.enabled", "true")
    System.setProperty("queryproof.query.id", "V18_STOCK_OPTIMIZER_PAIRED")

    val optimizer = new Optimizer(null) {}
    val root = Repartition(2, shuffle = true, child = LocalRelation(Nil))
    val initialJSON = root.toJSON
    assert(!root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION))

    val (result, log) = captured { optimizer.execute(root) }
    val lines = log.linesIterator.toIndexedSeq
    val early = findScope(lines, "LocalRelation early")
    val late = findScope(lines, "LocalRelation")
    assert(early.lastLine < late.firstLine, "unexpected production batch order")

    val earlyTags = rootTagEvents(early)
    val lateTags = rootTagEvents(late)
    assert(earlyTags.map(_(5)) == Seq("SET", "UNSET"),
      "early production rule did not SET then UNSET ROOT_REPARTITION")
    assert(lateTags.map(_(5)) == Seq("SET"),
      "late production rule did not SET and retain ROOT_REPARTITION")
    assert(earlyTags.head(7) == "false" && earlyTags.head(8) == "true")
    assert(earlyTags.last(7) == "true" && earlyTags.last(8) == "false")
    assert(lateTags.head(7) == "false" && lateTags.head(8) == "true")
    println("QUERYPROOF_V18_EXACT_BATCH_ATTRIBUTED_TAG_SEQUENCE=SET,UNSET|SET")

    val marks = rootCache(early, "MARK")
    val skips = rootCache(late, "SKIP")
    val pairs = for {
      m <- marks
      s <- skips
      if m(2) == s(2) && m(4) == s(4)
    } yield (m, s)
    assert(pairs.nonEmpty,
      "no cross-batch matching cache MARK and SKIP for exact rule and root")
    println("QUERYPROOF_V18_EXACT_ROOT_RULE_CACHE_PAIR_COUNT=" + pairs.length)
    println("QUERYPROOF_V18_MATCHED_RULE_ID=" + pairs.head._1(2))
    println("QUERYPROOF_V18_MATCHED_ROOT_JVM_ID=" + pairs.head._1(4))
    val earlyReads = rootReads(early)
    val lateReads = rootReads(late)
    assert(earlyReads.nonEmpty, "missing actual early-batch ROOT_REPARTITION read")
    assert(earlyReads.exists(r => r(5) == "true" && r(3) == pairs.head._1(4)),
      "production read was not on same root with present ROOT_REPARTITION")
    assert(lateReads.isEmpty,
      "unexpected post-cache-skip ROOT_REPARTITION read in late production rule")
    println("QUERYPROOF_V18_REAL_ROOT_READS_EARLY=" + earlyReads.length)
    println("QUERYPROOF_V18_REAL_ROOT_READS_LATE=" + lateReads.length)
    println("QUERYPROOF_V18_READ_NODE_ID_EQUALS_CACHE_PAIR=" + pairs.head._1(4))
    println("QUERYPROOF_V18_OBSERVER_INDUCED_READS_MUTED=true")
    println("QUERYPROOF_V18_IN_ONE_UNMODIFIED_BATCH_SCHEDULE=true")

    assert(result eq root)
    assert(result.toJSON == initialJSON)
    assert(root.containsTag(PropagateEmptyRelation.ROOT_REPARTITION))
    println("QUERYPROOF_V18_IDENTITY_AND_AST_UNCHANGED_BUT_TAG_PRESENT")
    println("QUERYPROOF_V18_NO_SQL_BAG_DIFFERENCE_OR_SPARK_BUG_CLAIM")
  }
}
