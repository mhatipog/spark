# QueryProof: source-stage TopK membership witness — preliminary findings, 2026-10-17

**Research stage: in-sample characterization of an already reported bug family. Not a new bug or a new general-purpose checker.**

## Tested on untouched official PySpark wheels

Workflow: [39060511480](https://github.com/mhatipog/spark/actions/runs/39060511480), both matrix jobs passed.

We copied the six-input-row ages, identifiers and correlated count/EXISTS setup from the existing [Spark PR #59445](https://github.com/apache/spark/pull/59445) regression design. We attached a **second, independently obtained source-stage witness** based on a per-partition bounded top-`q` heap before evaluating the correlated subquery. Distinct ages made the selected names unambiguous.

| Runtime | AQE | LIMIT 3 (8 repeats) | OFFSET 3 (8 repeats) |
|---|---|---:|---:|
| Spark 4.0.1 | off | 8/8 membership violations | 0/8 |
| Spark 4.0.1 | on | 8/8 | 0/8 |
| Spark 4.2.0 | off | 8/8 | 8/8 |
| Spark 4.2.0 | on | 8/8 | 8/8 |

There were **no runtime errors or UNKNOWN cases** in these 64 runs. The certificate checks the *names* selected after the LIMIT/OFFSET; it does not check correlated count values. The input witness for `OFFSET 3` uses the top six source ages and takes positions four through six.

These are **repeated executions within one pre-existing correctness bug family**, with just two projection types (correlated scalar count / EXISTS), NOT independent bugs or an out-of-sample performance estimate.

## Independent mathematical check

The implementation uses `Top_q(A ∪ B)=Top_q(Top_q(A)∪Top_q(B))` for totally ordered distinct input identities. Four Python unit tests passed on each Spark version, including 75 seeded random disjoint partitionings × six q choices, deterministic offset membership, ambiguity UNKNOWN and explicit wrong-membership mutation.

## Specific next falsification

The initial result alone does **not** isolate correlated subquery pushdown: an improperly specified ordering oracle, a planner-independent issue, or a row-source mistake could also create apparent discrepancies.

Therefore a **mandatory independent control** is being run on the same Spark versions and source-stage certificate with:
- Plain CTE `SELECT name FROM s`
- Deterministic non-subquery projection
- Safe uncorrelated scalar projection
- Safe uncorrelated EXISTS projection

Only if these show correct selection while the correlated cases fail can we attribute the observed discrepancy specifically to correlation/join-producing projection behavior. The control run has been submitted at [workflow 39060749288](https://github.com/mhatipog/spark/actions/runs/39060749288); at the time of this note it remains queued and **no result is claimed**.

## Novelty boundary

The bounded top-k merge identity is established computer science, not a new theorem. The potentially useful research is compiling and tracking semantic obligations across adaptive optimizer transformations, then emitting explicit verifiable witnesses or UNKNOWN. No claim of scientific publication readiness or superiority to SQLancer/DQP/DistSQL is justified yet.
