# AQE independent follow-up intervention

This is **not** part of the QueryProof ranking algorithm. It uses the candidate
identified by the already-frozen structural AST ranking on the SPARK-57956 held-out
query and probes a counterfactual with Spark's ordinary optimizer rule exclusion.

Comparisons: AQE on (default); AQE on plus exclusion of EliminateLimits; AQE off.
Each is repeated 8 times and interleaved to reduce simple order bias.
The oracle is the SQL language's LIMIT 1 cardinality invariant.

Interpretation:
- If a violation persists when the candidate is excluded, the simple causal
  hypothesis is not confirmed.
- If exclusions produce errors, record **INCONCLUSIVE**.
- If exclusions prevent all eight failures, this is suggestive and not a formal
  causal proof or robust rate estimate (N=8 is small).
- Check optimized/physical plans to verify exclusions were not silently ignored.
- This only exercises small synthetic input and one Spark release, not production.

No change is made to upstream Apache Spark or any customer environment.
