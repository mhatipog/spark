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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** Regression for the loss of the global ordering contract during AQE replanning. */
class AQEOneRowSortRegressionSuite extends QueryTest with SharedSparkSession {

  test("SPARK-59126: global ORDER BY survives AQE OptimizeOneRowPlan") {
    val query =
      """
        |WITH top AS (
        |  SELECT id, CAST(id % 1 AS INT) AS k
        |  FROM range(0, 3, 1, 1)
        |  ORDER BY id ASC LIMIT 2 OFFSET 1
        |), rhs AS (
        |  SELECT id AS bid, CAST(id % 1 AS INT) AS k
        |  FROM range(0, 1, 1, 1)
        |)
        |SELECT top.id, top.k, rhs.bid
        |FROM top LEFT JOIN rhs USING (k)
        |ORDER BY top.id ASC, rhs.bid ASC
        |""".stripMargin
    val expected = Seq(Row(1L, 0, 0L), Row(2L, 0, 0L))

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        "spark.sql.optimizer.excludedRules" ->
          "org.apache.spark.sql.catalyst.optimizer.InlineCTE",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      // Compare sequences, not checkAnswer's unordered bag.
      // AQE replanning can be nondeterministic even with identical input data.
      (1 to 8).foreach { _ =>
        assert(sql(query).collect().toSeq == expected)
      }
    }
  }
}
