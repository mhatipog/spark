# QueryProof: original DuckDB projection cohort failed source compatibility

Run: https://github.com/mhatipog/spark/actions/runs/39311568245

This source-frozen cohort was attempted without case or SQL rewrites. On Spark
4.2.0, cases P05, P06, and P12 (DuckDB `CAST(... AS VARCHAR)` projections)
raised SQL execution errors, whereas the other fifteen executed and matched
SQLite as row multisets. Hence **the complete 18-query experiment FAILED**.
No post-hoc case exclusions can turn it into a successful preregistered holdout.

The incomplete replay recorded 54 optimizer events and one independently sourced
`CollapseProject` transition in P17, which the exact pure-projection checker
certified. This is a **partial diagnostic witness**, not counted as a fully
successful independent confirmation.

A separate new source/case cohort was subsequently frozen under
`tools/queryproof_external_duckdb_subquery_20261019/`, omitting dialect-specific
VARCHAR casts by source-selection policy **before** running those new queries.
The checker remains pinned to commit `9d0e65a079cd99a6067256030baa96639d26a76e`.
