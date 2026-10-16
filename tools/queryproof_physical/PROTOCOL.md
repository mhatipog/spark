# QueryProof physical ordering validation (first experiment)

Hypothesis: after AQE has completed, the **final physical plan's Spark-declared ordering and partitioning properties** may no longer satisfy the top-level global `ORDER BY` in the analyzed SQL query. Inspect via Spark's own `SortOrder.orderingSatisfies` API and require exactly one output partition before reporting a declared global ordering guarantee.

This is *metadata consistency checking*, NOT verification of physical execution, independent SQL semantics, or Spark AQE correctness. An unproven property does not imply an incorrect answer; a declared property can itself be buggy. We label cases **DECLARED_GLOBAL_ORDER_SATISFIED**, **GLOBAL_ORDER_NOT_ESTABLISHED**, **OUT_OF_SCOPE_NO_ROOT_GLOBAL_SORT**, or **UNKNOWN**.

The probe is compiled against official Spark wheels as an extra JAR. It does **not modify RuleExecutor or any official Spark JAR**. Verify controls: global sorts, a partition-local `SORT BY`, no `ORDER BY`, and top-k (not yet supported by the root-only extractor). Two published Spark versions (4.0.1 and 4.2.0) and eight configuration combinations per version; 5 repetitions each. Synthetic fixtures only.

Prior 128 executions had 32 incorrect orderings; the research question is whether the final-plan metadata exposes the missing guarantee, including runs with coincidentally correct output. Unsupported expression translations are UNPROVEN/UNKNOWN, never SAFE.
