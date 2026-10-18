# QueryProof rule provenance — October 18

This branch tests the difficult missing link between a semantic obligation and the Spark optimizer rule that transformed the relevant analyzed-plan expression.

The earlier QueryProof Catalyst interpreter independently witnessed wrong row bags for the two known historical issue families SPARK-58384 (nullable join under NOT) and SPARK-58442 (nullable IN under IS NULL) in Spark 4.0.1 and 4.2.0. The full five-element three-valued Boolean observation lattice was verified exhaustively, and observed requirements were automatically extracted from analyzed Catalyst trees (five known reference cases per version).

## Falsifiable goal

An instrumented **disposable Spark 4.2.0 JVM** records every effective Catalyst rule's before/after JSON AST in a bounded, synthetic six-case benchmark. The downstream scoring code uses observation demands computed from the *analyzed plan*, not manually supplied issue names, to rank transitions where a demanded operator disappeared.

This is a **structural candidate ranking**, not a semantic proof or an original bug discovery. Rule skipping/ablation and independently evaluated source semantics must subsequently corroborate any alleged faulty transition.

The six calibration cases B01/B02/B03 and N01/N02/N03 are already known, so a positive localization is *in-sample*. A new prospective evaluation corpus must be frozen before this approach can support academic generalization claims.

## Operational boundaries

The only JAR modifications occur on GitHub's disposable CI runner. Full-Spark build jobs are disabled on research branches; this workflow compiles the existing single Scala RuleExecutor overlay once on PySpark 4.2.0. Raw AST traces (which could contain SQL literals) remain local to the CI runner and are not uploaded as artifacts or printed to public Actions logs. Only synthetic data are used. Saved JSON includes operator counts, candidate rule names and SQL outputs, not raw full ASTs.

No changes are made to upstream Apache Spark PR #59445 or to the Spark fork's master branch.

**Precommitted protocol:** FROZEN_PROTOCOL.json. **Entry:** run_trace.py. **Triage:** triage.py.
