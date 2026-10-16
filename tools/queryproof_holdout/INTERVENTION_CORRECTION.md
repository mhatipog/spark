# Intervention validity correction — 2026-10-16

Earlier v0.7 optimizer exclusion experiments set \`spark.sql.optimizer.excludedRules\` *before* constructing each temporary source fixture. That is an experimental confounder: optimizer exclusions could alter how fixture-defining SQL is planned.

v0.8 changes all three exploratory intervention runners to construct the same fixture under the baseline configuration first, **then** set the excluded rule(s) immediately before executing the held-out query.

This correction does **not** change the preregistered holdout manifest, query expected outputs, optimizer trace plugin, or the v0.6 ranking. The original v0.7 results remain visible and should not be erased. Compare corrected runs to earlier ones before claiming any rule is causally responsible.

Even after this correction, the test relies on equivalent fixture definitions in separate sessions, not byte-for-byte immutable snapshot identifiers. That stronger isolation is a future step, particularly for Parquet-backed benchmarks.
