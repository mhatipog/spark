# R32: genuinely untouched join-focused holdout (preregistered)

Date 2026-10-20. Q development on M29/M33 (2/2 local observed join-pruning events certified, 20/20 adversarial AST mutations rejected) motivated this R test. The Q observations are **training**, not part of R; M/O scores stay immutable. Round P algebra was independently checked on 288,800 finite bag configurations but is not by itself a Spark theorem.

Freeze R32 before first SQL execution: new uint32 LCG seed **2026102031**, 32 new table-pair inputs (27 left + 19 right rows, including duplicates, NULL and shared keys), eight fixed SQL families four inputs each, **16 target** (INNER_COUNT, LEFT_COUNT, INNER_SUM, LEFT_SUM) and **16 challenge** (INNER_LT, LEFT_OR, DISTINCT_PAIR, HAVING_COUNT). Every family represents one SQL template, not independent SQL shapes; overall coverage percentages are sample-design descriptors only.

Source pinned: exact Q checker plus R scorer, unchanged v2 suffix scorer and prior JVM observer implementations. The workflow **must** run a preflight for all directly indexed protocol fields, blob SHAs, case IDs and stratum counts before any Spark SQL. A broken preflight before execution may be repaired without losing test blindness; any failure after first SQL must be disclosed and may not be reinterpreted as an intact first run (lesson from N32).

Primary: genuine changed `ColumnPruning` events on previously unseen input/data: proportion locally certified under exact inner/left identity projection law, with UNKNOWN retained. Separately report full optimizer suffix certificate coverage **without** incorporating Q; all other rule events remain UNKNOWN unless source-pinned prior checker certifies. Rowbag comparison of Spark 4.2.0 and SQLite is a finite oracle only, not a proof.

Zero certificates is valid. No threshold adjustment to R after observing its first output. We will not open an upstream PR.
