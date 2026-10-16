# QueryProof — experimental AQE OptimizeOneRowPlan correctness fix (2026-10-16)

**Known upstream bug:** [SPARK-59126](https://issues.apache.org/jira/browse/SPARK-59126). This is an experimental source-level guard validated on synthetic cases, **not** a formal optimizer soundness proof or yet an upstream contribution.

## Native Spark diagnosis

Spark 4.2.0's own `PlanChangeLogger` shows the **first loss of the final global Sort at the logical AQE `OptimizeOneRowPlan` rule**, after a `LogicalQueryStage` appears in the plan. Physical `RemoveRedundantSorts` eliminates a distinct nested Sort, not the top-level sort. The actual query returns two rows in the wrong order even though the rule's current guard requires `child.maxRows <= 1`.

No-JVM-modification ablation (6 runs per mode, Spark 4.2.0):
- AQE candidate: **0/6 correct**
- Disable only `spark.sql.execution.removeRedundantSorts`: **0/6 correct**
- Disable only `spark.sql.adaptive.optimizer.excludedRules=org.apache.spark.sql.catalyst.optimizer.OptimizeOneRowPlan`: **6/6 correct**
- Disable both: **6/6 correct**

Trace and raw plans: [native CI](https://github.com/mhatipog/spark/actions/runs/38874910487).

## Frozen corpus / placebo

61 SQL cases frozen in [commit c065ab1c](https://github.com/mhatipog/spark/blob/c065ab1c86a67558cf12f66fcb99c200d877a8f1/tools/queryproof_synthesis/candidate_manifest.json). Two executions per case per condition and version. The pure-Python oracle checks correct row bags and final ordering.

|  | Official PySpark | AQE rule excluded via config | Byte-identical original Scala recompiled in test JAR | Experimental stage-aware guard |
| --- | ---: | ---: | ---: | ---: |
| Spark 4.0.1 wrong order / 122 | 25 | **0** | 26 | **0** |
| Spark 4.2.0 wrong order / 122 | 25 | **0** | 24 | **0** |

There were no row-bag mismatches in these comparisons. The placebo test is crucial: merely recompiling and overlaying the original optimizer **does not** remove the bug, unlike the surgical guard.

- [Non-invasive rule-exclusion corpus](https://github.com/mhatipog/spark/actions/runs/38876141149)
- [Source guard corpus](https://github.com/mhatipog/spark/actions/runs/38876738539)
- [Original-source recompilation placebo](https://github.com/mhatipog/spark/actions/runs/38877244103)

## Candidate guard

See [experimental modified Spark optimizer source](OptimizeOneRowPlan.scala). It refuses to eliminate a Sort based on the maximum-one-row rule when the sort's child tree contains a `LogicalQueryStage`. Ordinary one-row plans without this runtime stage can still be optimized.

The source is compiled **against each official released Spark wheel separately** and patched only into a disposable CI wheel JAR. Nothing is pushed to the Apache Spark upstream repo; existing PRs are unaffected.

## Remaining issues — BEFORE upstream

1. Precisely establish why the runtime-stage bound becomes unsafe, without changing lazy-plan evaluation with instrumentation. Attempts to inspect `maxRows` eagerly produced an **observer effect** and cannot be used as causal evidence.
2. Add a Scala regression to the Apache Spark AQE test suite and run those tests on a full compiled Spark build.
3. Test different, unseen query families and benchmark performance costs of additional retained sorts versus fixing `LogicalQueryStage` cardinality at the source.
4. Coordinate with maintainers on SPARK-59126; upstream issue is already known.

**Decision:** sufficient evidence for a maintainer review of this diagnosis and a prospective surgical fix; insufficient evidence to merge the current guard or claim a novel generic optimizer verification method.
