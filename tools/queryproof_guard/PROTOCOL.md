# Experimental AQE one-row sort guard

This research candidate is built from the **unchanged upstream OptimizeOneRowPlan.scala file** at Apache Spark v4.2.0, which is byte-identical to v4.0.1 at the time of research. The only behavioral change prevents the rule from deleting a Sort based on `child.maxRows <= 1` when a `LogicalQueryStage` exists inside that child.

Motivation: Source-derived Spark-native PlanChangeLogger proves that in the known SPARK-59126 test family, the adaptive reoptimization phase removes the final global Sort under **OptimizeOneRowPlan**, not **RemoveRedundantSorts**. Direct exclusion via `spark.sql.adaptive.optimizer.excludedRules` independently restores correctness (6/6 on one minimized case; 122/122 cases on each of 4.0.1 and 4.2.0).

The patch is compiled against official PySpark 4.0.1 and 4.2.0 wheel JARs inside *disposable GitHub Actions runners* and overlays only the temporary Catalyst JAR. No deployed system or existing upstream PR is modified.

A successful 61-query generated test is **not** a proof of soundness: all those cases share one known bug family and additional optimizer invariants/alternative AQE plans remain unexamined. The experimental guard is deliberately conservative and can disable valid one-row optimizations when query stages are present.

Separate alternative to evaluate: repair the cardinality estimation/lazy caching at LogicalQueryStage directly. Do not submit this patch upstream until a broader negative control corpus, Spark suite tests and a maintainer-compatible diagnosis of the actual incorrect maxRows have been established.


