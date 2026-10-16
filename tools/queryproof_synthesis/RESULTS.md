# QueryProof — Generated Spark counterexamples (2026-10-16)

**Research outcome: reproducible variants of the already-known SPARK-59126 AQE ordering bug, not a newly discovered Spark bug.**

The generator produced and ran **61 SQL candidates per release** on unmodified PySpark 4.0.1 and 4.2.0: 1 historical calibration plus 60 frozen generated variations. Only CTE/LEFT JOIN candidates violated the explicit final `ORDER BY`: **11 generated cases** on 4.0.1 and **9** on 4.2.0, with **9 shared identifiers**. No observed row-bag mismatch. Pure Python independently computes the correct row bag.

A bounded greedy reducer started from a 6-input-row generated case and produced a **4-input-row SQL counterexample** (3 rows on the left, 1 on the right; 2 output rows). **It is not proven globally minimal.** Original correct order: `[[1,0,0],[2,0,0]]`; possible actual wrong order: `[[2,0,0],[1,0,0]]`.

## Independently repeated physical-plan effects

24 repetitions per setting *and version*, 192 total:

| Setting | 4.0.1 incorrect | 4.2.0 incorrect |
| --- | ---: | ---: |
| AQE enabled, InlineCTE excluded | 15/24 | 24/24 |
| Normal optimizer | 0/24 | 0/24 |
| AQE disabled | 0/24 | 0/24 |
| AQE enabled, InlineCTE excluded, broadcast disabled | 23/24 | 24/24 |

All 192 runs retained the correct row bag. Every wrong-order case had a final AQE plan lacking the declared global-ordering guarantee, while the correct-order runs retained it. These are correlated observations on **one bug family**, not a universal correctness theorem.

An **independent minimal reproducer without our JVM inspector** also succeeded on official PySpark wheels. Eight iterations per setting showed 4.0.1 candidate **6/8**, 4.2.0 candidate **7/8** wrong; both normal and AQE-off **0/8** wrong; both no-broadcast **8/8** wrong. Therefore the failure is *not limited to a broadcast join*. The initial physical plan retains the final `Sort`; failing finalized adaptive physical plans lose it. On 4.0.1, both the failing and correct final-plan variants appeared.

## Artifacts and test links

- Original **manifest frozen before implementation**: [c065ab1 — 61 cases](https://github.com/mhatipog/spark/blob/c065ab1c86a67558cf12f66fcb99c200d877a8f1/tools/queryproof_synthesis/candidate_manifest.json)
- [Generator and independent oracle](generate.py)
- [Greedy shrinker and minimized case parameters](minimized_witness.json)
- [Standalone official-wheel reproducer](standalone_repro.py)
- [Discovery CI](https://github.com/mhatipog/spark/actions/runs/38870418185)
- [Repeatability CI](https://github.com/mhatipog/spark/actions/runs/38871023619)
- [Official-wheel standalone CI](https://github.com/mhatipog/spark/actions/runs/38871673605)

**Scope:** empirical discovery and shrinking of inputs within a carefully hand-designed SQL family. Not automatic semantic obligation extraction from arbitrary physical plans, not a formal equivalence checker, no demonstration of a new optimizer bug or scientific novelty. Neither SQLancer nor other external provers were executed in this experiment.

**Next high-value research step:** extract the precise adaptive stage rewrite that removes the required Sort, create a property-based regression test independent of the known SQL template, then compare to appropriate existing Spark requirement checks and SQLancer's methods. Only subsequently attempt a narrowly justified upstream correction.

This research branch never modifies the existing Apache Spark upstream pull request and uses no customer data.
