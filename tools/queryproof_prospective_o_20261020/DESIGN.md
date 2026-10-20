# O32 preregistration — a genuinely new first-run cohort

N32 executed 32 SQL cases but lost all private scores when the existing report writer required `frozen_verifier_commit`, missing from the N protocol; run 39421965245. This was **after first N SQL execution**, so any N rerun would be posthoc. We did not rerun N, retune v2, or reuse any N rows for O.

O consists of **32 entirely new deterministic synthetic SQL/data instances** from a different PRNG and seed, 23+17 rows per two table instances, with duplicates, NULLs, zero, negative values. Eight SQL families each have four datasets; 16 target direct/nested COUNT/MAX/MIN and 16 challenge join/DISTINCT/HAVING. The SQL shapes remain intentionally similar to validate learned fragment boundaries, not to estimate natural workload coverage.

The verifier/scorer source files are exactly pinned by Git blob IDs. Before *any* O SQL, the workflow must run a strict preflight verifying all direct protocol fields required by the existing report writer, pinned blobs, 32 unique cases, stratum counts and benchmark IDs. No threshold tuning after seeing O is permitted. Zero proofs is valid.

Primary metric: complete changed optimizer suffixes after FinishAnalysis with v2 local proof chains, plus separately reported frame, live RDD identity, all-rule drift and tag guard sets; intersection only if exact case sets are established. Finite Spark/SQLite rowbag oracle and zero instrumentation errors are mandatory integrity checks. Preserve unknown categories and posthoc/prospective distinction.

The new cohort is not meant to suggest general SQL equivalence proof or dominance over SQLSolver/Logos. It probes source-pinned *actual Catalyst trace* certificates.
