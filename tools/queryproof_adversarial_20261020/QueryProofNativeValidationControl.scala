/*
 * Control pair shared between Spark's native structural validation and
 * QueryProof's local proof checkers. The rewrite is intentionally wrong:
 * it changes SELECT 1 into SELECT 2 while retaining output schema/ExprId.
 */
package org.apache.spark.sql.catalyst.rules

import java.io.PrintWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlanIntegrity, OneRowRelation, Project}

object QueryProofNativeValidationControl {
  def main(args: Array[String]): Unit = {
    val child = OneRowRelation()
    val id = NamedExpression.newExprId
    val before = Project(Seq(Alias(Literal(1), "x")(id)), child)
    val after = Project(Seq(Alias(Literal(2), "x")(id)), child)
    val schemaChange = Project(Seq(Alias(Literal("two"), "x")(id)), child)

    require(before.resolved && after.resolved && schemaChange.resolved,
      "All control plans must be resolved")
    require(before.output.map(_.dataType) == after.output.map(_.dataType),
      "The wrong rewrite must preserve the schema")
    require(before.output.map(_.exprId) == after.output.map(_.exprId),
      "The wrong rewrite must preserve expression identity")
    require(before.projectList.head.eval(InternalRow.empty) !=
      after.projectList.head.eval(InternalRow.empty),
      "The wrong rewrite must have a real semantic counterexample")

    val wrongVerdict = LogicalPlanIntegrity.validateOptimizedPlan(
      before, after, lightweight = false)
    require(wrongVerdict.isEmpty,
      "Native integrity validation unexpectedly flagged a schema-preserving value rewrite")

    val changedSchemaVerdict = LogicalPlanIntegrity.validateOptimizedPlan(
      before, schemaChange, lightweight = false)
    require(changedSchemaVerdict.nonEmpty,
      "Native integrity validation must reject this type-changing rewrite")

    val output = new PrintWriter("artifacts/queryproof_native_control_pairs.json")
    try {
      output.println(
        s"""{"invalid_value_change":{"old":${before.toJSON},"new":${after.toJSON}},"invalid_schema_change":{"old":${before.toJSON},"new":${schemaChange.toJSON}}}""")
    } finally {
      output.close()
    }
    println("QUERYPROOF_NATIVE_ACCEPTS_SEMANTIC_VALUE_CHANGE=CONFIRMED")
    println("QUERYPROOF_NATIVE_REJECTS_SCHEMA_CHANGE=CONFIRMED")
    println("QUERYPROOF_NATIVE_IS_STRUCTURAL_NOT_EQUIVALENCE_CHECKER=CONFIRMED")
  }
}
