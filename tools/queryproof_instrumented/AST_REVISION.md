# AST model revision (2026-10-16)

The previous AST ranking had three concrete issues in the development benchmark:
1. It only counted nodes on the main LogicalPlan path, overlooking subquery plans nested inside expressions.
2. It misclassified removal of a no-op Project as movement of a Project across a LIMIT.
3. It assigned a very high score to generic Sort removal, even when the Sort was not demonstrably required by the operator context.

v0.6 includes nested-expression AST traversal and checks for loss of explicit Sort beneath a LIMIT in a nested subquery. These are **candidate generation heuristics**, not SQL-correctness proofs. Six focused unit tests protect the structural distinctions.

The feature was designed after examining SPARK-58411; **therefore all reported improvements on SPARK-58411 and the other three development examples are in-sample**. We must evaluate on independent, unseen cases before making claims about generalization or detection accuracy.

The original 4 historical SQL test executions and their published expected answers remain unchanged. Interventions continue to be recorded separately from heuristic rankings.
