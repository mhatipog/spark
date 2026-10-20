# QueryProof vs SQLSolver: preregistered miniature SQL boundary controls

Date: 2026-10-20. This is a **development comparator**, not a prospective M
cohort and not a statistically representative Spark workload.

The three one-line SQL pairs are pinned by git when this file and its fixture
files are committed, before SQLSolver is first run in our CI.

| ID | Old query | New query | Independent expected semantics |
|---|---|---|---|
| S1 | SELECT 1 AS x FROM t | SELECT 2 AS x FROM t | NOT equivalent: a nonempty t yields different values |
| S2 | SELECT k AS x FROM t | SELECT q.x FROM (SELECT k AS x FROM t) AS q | Equivalent under bag semantics |
| S3 | SELECT k, COUNT(*) AS n FROM t GROUP BY k,k | Same with GROUP BY k | Equivalent for deterministic key k, including duplicates, NULL, empty input |

- Table: t(k INT PRIMARY KEY, v INT). S1 has a counterexample whenever t
  contains at least one row. S2/S3 follow direct SQL semantics. A primary key
  constraint simplifies the schema; no result depends on uniqueness.
- SQLSolver clone: official public SJTU-IPADS/SQLSolver main HEAD at CI run,
  printed in logs. Java 17, Gradle 7.3.3 and its bundled Z3 libraries.
- SQLSolver outcomes are recorded as EQ/NEQ/UNKNOWN/TIMEOUT without posthoc
  query changes. Wrong definite outcomes require further investigation.
- This comparison tests SQL-level claims only. It does NOT translate arbitrary
  live Catalyst ASTs into SQL and does not establish QueryProof superiority.
- Spark native integrity and four QueryProof local checkers were separately
  compared on genuine Catalyst plans in the same branch's control run.
- No optimizer bug report, upstream pull request or paid capacity is involved.
