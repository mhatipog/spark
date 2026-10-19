# QueryProof — Projection Composition and Aggregate Context: Scientific Checkpoint
**19 October 2026; Spark 4.2.0; branch**
`research/queryproof-certificate-boundary-hardening-20261019`

## What was established

Two new exact, *fail-closed* theorem families have been implemented:

1. **Pure projection composition.** For a fixed input relation R and total
   deterministic attribute/alias/literal expressions E,F,H, if the ordered
   output symbolic normal forms and resolved output ID/type/name descriptors
   match, then
   `Project(E, Project(F, R))` and `Project(H, R)` have identical output
   **row multisets**. Multiplicities and NULLs are preserved pointwise.
   The checker proves syntactic equality of every substituted atom; it
   accepts neither casts nor UDFs, random functions, unrecognized expressions,
   changed input subtrees, nor unknown project metadata.
   Unchanged Sort ancestors may be stripped **for bag semantics only**.
2. **Identity projection under integral aggregates.** A direct passthrough
   Project with exactly the same named columns, in the same order, as its
   *byte-identical* child Project can be removed under identical Aggregate
   ancestors containing only grouped AttributeReferences, direct aliases and
   integer SUM. The lower Project can contain an unchanged integer Add; its
   expression is not inlined or duplicated. The theorem is
   `A_2(A_1(Project_identity(T))) = A_2(A_1(T))` over defined logical
   row relations. No LIMIT, volatile aggregate, UDF, or arbitrary aggregate
   expression is admitted.

The new modules have 21 and 13 targeted positive/adversarial unit tests,
respectively; all pass in GitHub Actions, together with the existing suites.
Unrecognized Catalyst constructs return `UNKNOWN`, not `WRONG`.
The claims concern logical row-bag values, not physical row order, exact
wall-clock effects, full metadata equivalence, cost, or performance.

## Evidence: keep prospective and retrospective separate

| Cohort | SQL | Catalyst events | Named CollapseProject | Pure composition | Aggregate identity | Oracle/malformed |
|---|---:|---:|---:|---:|---:|---|
| PostgreSQL-derived historical X | 18 | 91 | 10 | 8 retrospective | 0 | 0 |
| DuckDB join historical D | 18 | 129 | 4 | 3 retrospective | 0 | 0 |
| DuckDB NULL/outer historical A | 18 | 100 | — | 0 retrospective | 0 | 0 |
| DuckDB subquery S, clean new source | 18 | 128 | 1 | 0 prospective | 1 **retrospective** after viewing shape | 0 |
| DuckDB grouping G, clean new source | 18 | 135 | 1 | 0 retrospective | 0 prospective | 0 |

The **original DuckDB projection P cohort FAILED its complete protocol**:
three DuckDB `CAST(... AS VARCHAR)` queries were rejected by Spark;
the remaining 15 ran without bag mismatches. P17 gave one diagnostic
`CollapseProject` certificate, **not credited as clean independent evidence**.
See `tools/queryproof_external_duckdb_projection_20261019/FAILURE_CHECKPOINT.md`.

Previous independent confirmation of the INNER JOIN guard theorem remains
**16 certified events in 36 independently sourced join SQL cases**. None of
the new retrospective project certificates may be added to that figure.
The two fully clean new 18-query source cohorts both returned zero *new
prospective* CollapseProject certificates. That is a real limitation, not
a negative optimizer correctness result.

## Why the gap is informative

In cohort S06, the actual rewrite was
`Aggregate(Aggregate(Project(identity, Project(Add, LogicalRDD))))`
to `Aggregate(Aggregate(Project(Add, LogicalRDD)))`.
It was only certifiable once a separate identity-elimination/aggregate
congruence theorem was developed. Cohort G has an additional, as-yet
unclassified `CollapseProject` structure. Prefer redacted shape analysis
and small independent lemmas over blanket acceptance of arbitrary contexts.

## Protocol discipline and next experiment

- No original five-checker historical union totals were rewritten.
- External SQL is copied verbatim from pinned DuckDB source, and source
  ordinals and fixture rows are frozen before each respective first run.
- `EQUIVALENT_IN_FRAGMENT` is a mathematical fragment-local result.
  A source-level executor comparison on finite rows is a separate oracle.
- The application of a theorem to an already-inspected trace is
  **development evidence**, not an out-of-design confirmation.
- For the next proof, align the precise old/new changed subtree and prove
  congruence only through a whitelisted deterministic relation context
  whose output schema and identities are stable. Require fresh source SQL
  and negative controls before recording new independent certificates.
- Do not open an upstream PR while coverage/generalization remains narrow.

CI: [aggregate-focused run](https://github.com/mhatipog/spark/actions/runs/39314330445).

Upstream design constraints: [Spark Optimizer.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala),
[alias metadata configuration](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala).
