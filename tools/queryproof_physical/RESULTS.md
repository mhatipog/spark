# Physical ordering after AQE — validated Spark metadata experiment

**Research evidence from 2026-10-16. Not an upstream patch or a production correctness certificate.**

- Two **unmodified** released PySpark runtimes: 4.0.1, 4.2.0.
- External Scala inspection JAR; reads **analyzed logical** `Sort` obligation and **final** AQE `SparkPlan.outputOrdering` plus `outputPartitioning`.
- Uses Spark-native `SortOrder.orderingSatisfies`. Handles `WithCTE`, `GlobalLimit`, `LocalLimit` and `Offset` only; unsupported wrappers never receive a positive verdict.
- Global ordering requires compatible ordering and either one partition or compatible `RangePartitioning`. A multi-partition local sort does not suffice.
- 8 plan configurations × 5 repetitions × 2 Spark versions = **80 executions of one already known issue family** (SPARK-59126). This is **not** 80 independent bugs.

| Observed SQL output | Spark ordering declaration | Number of runs |
| --- | --- | ---: |
| Wrong order | Global ordering not established | **20** |
| Wrong order | Global ordering declared satisfied | **0** |
| Correct order | Global ordering declared satisfied | **40** |
| Correct order | Global ordering not established | **20** |

**40 candidate physical plans** initially declared compatible ordering, which was missing from the final adaptive plans. **20** returned incorrect row ordering; the other **20** returned the expected order without this analyzer being able to establish its general guarantee.

All positive ordering controls (simple, multi-key, explicit NULL policy, top-k) satisfied the declaration check, and partition-only SORT BY/no ORDER BY were treated as outside this scope. Both versioned verification jobs passed with no audit assertion failures.

### Verified provenance

- [Successful dual-version CI experiment](https://github.com/mhatipog/spark/actions/runs/38868161157)
- Source: `tools/queryproof_physical/PhysicalOrderingProbe.scala`, `run_probe.py`, `score_probe.py`.
- Results: workflow artifact archives per version, with JSON evidence and physical plans.

### Interpretation

`GLOBAL_ORDER_NOT_ESTABLISHED` is **UNKNOWN/UNPROVEN**, never an assertion that Spark returned an incorrect result. `DECLARED_GLOBAL_ORDER_SATISFIED` is only a consistency claim relative to Spark's own metadata, not an independent proof of engine correctness. This study is in-sample, built against a known bug family. No novel Spark bug or new general-purpose verifier has been demonstrated.

**Next strict go/no-go test:** freeze this metadata-based validator and evaluate on genuinely unseen AQE/order-dependent query families against differential baselines; separate unsupported cases, parser failures, and correct-but-unproven cases. Do not advertise a proof system until it beats relevant existing techniques on independent benchmarks.
