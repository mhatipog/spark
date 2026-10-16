/*
 * Research-only, out-of-tree Spark 4.0/4.2 physical ordering probe.
 * Intentionally not an Apache Spark source modification.
 */
package org.queryproof

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, LogicalPlan, Offset, Sort, WithCTE}

/**
 * A conservative observation about ordering properties declared by Spark.
 * It is NOT a formal proof of the SQL result or of the correctness of AQE.
 */
object PhysicalOrderingProbe {
  private val mapper = new ObjectMapper()

  def inspect(qe: QueryExecution): String = {
    val fields = new java.util.LinkedHashMap[String, Object]()
    def field(k: String, v: String): Unit = fields.put(k, v)
    def flag(k: String, v: Boolean): Unit = fields.put(k, java.lang.Boolean.valueOf(v))
    def integer(k: String, v: Int): Unit = fields.put(k, java.lang.Integer.valueOf(v))

    try {
      val analyzed = qe.analyzed
      // Only unwrap operators that preserve the *global* result ordering:
      // CTE container, LIMIT, and OFFSET. Never traverse Project/Join/Exchange.
      def topGlobalSort(p: LogicalPlan, depth: Int = 0): Option[Sort] = {
        if (depth > 16) None else p match {
          case w: WithCTE => topGlobalSort(w.plan, depth + 1)
          case l: GlobalLimit => topGlobalSort(l.child, depth + 1)
          case l: LocalLimit => topGlobalSort(l.child, depth + 1)
          case o: Offset => topGlobalSort(o.child, depth + 1)
          case s: Sort if s.global => Some(s)
          case _ => None
        }
      }
      val requested = topGlobalSort(analyzed).map(_.order)
      field("requested_sort_source", topGlobalSort(analyzed).map(_.nodeName).getOrElse("unsupported"))
      field("analyzed_top_class", analyzed.getClass.getName)
      val outer: SparkPlan = qe.executedPlan
      val adaptive = outer match {
        case a: AdaptiveSparkPlanExec => Some(a)
        case _ => None
      }
      flag("aqe", adaptive.isDefined)
      val isFinal = adaptive.forall(_.isFinalPlan)
      flag("is_final_aqe_plan", isFinal)
      val finalPlan: SparkPlan = adaptive.map(_.executedPlan).getOrElse(outer)
      field("final_root_class", finalPlan.getClass.getName)
      field("final_root_name", finalPlan.nodeName)
      field("outer_root_class", outer.getClass.getName)
      field("final_root_tree", finalPlan.treeString)
      field("required_order", requested.map(_.mkString(" | ")).getOrElse(""))
      field("declared_output_order", finalPlan.outputOrdering.mkString(" | "))
      field("declared_partitioning", finalPlan.outputPartitioning.toString)
      val partitions = finalPlan.outputPartitioning.numPartitions
      integer("declared_num_partitions", partitions)
      requested match {
        case None =>
          field("verdict", "OUT_OF_SCOPE_NO_ROOT_GLOBAL_SORT")
        case Some(order) if !isFinal =>
          field("verdict", "UNKNOWN_AQE_NOT_FINAL")
        case Some(order) =>
          val compatible = SortOrder.orderingSatisfies(finalPlan.outputOrdering, order)
          val global = partitions == 1
          flag("spark_ordering_satisfies", compatible)
          flag("single_output_partition", global)
          field("verdict", if (compatible && global) {
            "DECLARED_GLOBAL_ORDER_SATISFIED"
          } else {
            "GLOBAL_ORDER_NOT_ESTABLISHED"
          })
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        field("verdict", "UNKNOWN_INSPECTION_ERROR")
        field("inspection_error", e.getClass.getName + ": " + e.getMessage)
    }
    mapper.writeValueAsString(fields)
  }
}
