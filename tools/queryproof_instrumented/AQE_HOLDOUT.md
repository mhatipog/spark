# Frozen holdout protocol

Historical independent candidate: [SPARK-57956](https://issues.apache.org/jira/browse/SPARK-57956), a documented AQE bug on Spark 4.1.2 where the top-level LIMIT may disappear.

- Frozen AST feature extraction: rank_ast.py from commit f77639fd709ef3ba1b503a8d0afc081c67f66b80. It MUST NOT be changed based on this result.
- Running environment: installed PySpark 4.2.0, Java 17. The issue's affected version was 4.1.2; 4.2.0 might not reproduce it.
- Collect same SQL with AQE on (3 repetitions) and off (1 control), and store actual physical and optimized plans.
- Independent cardinality oracle: a final LIMIT 1 permits at most one row, regardless of row order.
- An AQE failure not visible through RuleExecutor AST tracing is OUT OF SCOPE, not a false claim of successful localization.
- No upstream Spark PR changes are made.
