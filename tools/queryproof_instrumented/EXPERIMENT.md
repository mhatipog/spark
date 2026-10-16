# QueryProof v0.5 — forced ablation research protocol

This branch changes only a disposable Spark 4.2.0 runner's Catalyst JAR; no upstream Spark code or customer environment is modified.

**Purpose:** If official Spark `spark.sql.optimizer.excludedRules` ignores a mandatory rule, a second, experimental intervention can bypass it in `RuleExecutor`. Candidates are selected from structural AST changes without supplying their names. Expected query answers are consulted only for evaluating the intervention.

**Important restrictions:** Mandatory rules may be required for executable, resolved SQL plans. If the query fails to analyze, compile or execute, the result is **INCONCLUSIVE**, never "fixed". A restored result under an intervention is not a formal proof of the original rule's defect; it may result from changes elsewhere in the optimizer.

**Experiment:** run the `QueryProof structured Catalyst AST experiment` workflow. It records structured AST events, blind candidate scores, official Spark rule exclusions, and optionally forced ablations. No production data should be supplied: full logical ASTs may reveal literal values.

**Acceptance:** the workflow must compile the instrumented Scala class, emit real AST events, generate ranked candidates without an error, and preserve raw JSON and logs. Interpretation of each case belongs in the research report; overall green CI is not evidence of bug detection.

**Known limitations:** simplified structural risk features; no SQL semantic equivalence prover; no automatic counterexample minimization; benchmark cases currently four executions from three bug families; query literals and sensitive data can appear in traces.
