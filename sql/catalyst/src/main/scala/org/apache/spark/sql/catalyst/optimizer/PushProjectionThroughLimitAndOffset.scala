/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Offset, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIMIT, OFFSET, PROJECT, SORT}

/**
 * Pushes Project operator through Limit operator.
 */
object PushProjectionThroughLimitAndOffset extends Rule[LogicalPlan] {
  private def producesJoin(e: NamedExpression): Boolean = {
    SubqueryExpression.hasCorrelatedSubquery(e) ||
      SubqueryExpression.hasInOrCorrelatedExistsSubquery(e)
  }

  private def canPush(projectList: Seq[NamedExpression]): Boolean = {
    // Do not infer that an input is unordered from a missing logical outputOrdering.
    // Keeping the projection above the boundary can prevent CollectLimitExec, including
    // on unordered inputs and for sorted OFFSET without LIMIT.
    projectList.forall(e => e.deterministic && !producesJoin(e))
  }

  private def hasGlobalSort(plan: LogicalPlan): Boolean = plan match {
    case Sort(_, true, _, _) => true
    case p: Project => hasGlobalSort(p.child)
    case f: Filter => hasGlobalSort(f.child)
    case l: LocalLimit => hasGlobalSort(l.child)
    case g: GlobalLimit => hasGlobalSort(g.child)
    case o: Offset => hasGlobalSort(o.child)
    case _ => false
  }

  // Lift the entire consecutive projection chain or leave it alone. Moving only a
  // lower project would invalidate references from an upper project to its aliases.
  private def lift(boundary: LogicalPlan, child: LogicalPlan): LogicalPlan = {
    val projects = Iterator.iterate(child) {
      case p: Project => p.child
      case other => other
    }.takeWhile(_.isInstanceOf[Project]).map(_.asInstanceOf[Project]).toList
    if (projects.nonEmpty &&
        projects.forall(_.projectList.forall(_.deterministic)) &&
        projects.exists(_.projectList.exists(producesJoin)) &&
        hasGlobalSort(projects.last.child)) {
      val sliced = boundary.withNewChildren(Seq(projects.last.child))
      projects.foldRight(sliced: LogicalPlan)((p, c) => p.copy(child = c))
    } else {
      boundary
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val lifted = plan.transformUpWithPruning(bits =>
      bits.containsPattern(PROJECT) && bits.containsPattern(SORT) &&
        bits.containsAnyPattern(OFFSET, LIMIT)) {
      case l @ LocalLimit(_, child: Project) if !l.isStreaming =>
        lift(l, child)
      case g @ GlobalLimit(_, child: Project) if !g.isStreaming =>
        lift(g, child)
      case o @ Offset(_, child: Project) if !o.isStreaming =>
        lift(o, child)
    }

    lifted.transformWithPruning(treeBits =>
      treeBits.containsPattern(PROJECT) && treeBits.containsAnyPattern(OFFSET, LIMIT)) {

      case p @ Project(projectList, limit @ LocalLimit(_, child))
          if canPush(projectList) =>
        limit.copy(child = p.copy(projectList, child))

      case p @ Project(projectList, g @ GlobalLimit(_, limit @ LocalLimit(_, child)))
          if canPush(projectList) =>
        g.copy(child = limit.copy(child = p.copy(projectList, child)))

      case p @ Project(projectList, offset @ Offset(_, child))
          if canPush(projectList) =>
        offset.copy(child = p.copy(projectList, child))
    }
  }
}
