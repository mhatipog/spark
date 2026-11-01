# QueryProof direction change: storage-interface testing across engines (from 2026-11-01)

The random rounds against Spark alone end with round 8. Since then:

- **Spec:** the contracts are in `2026-11-01-storage-interface-contracts.md` (C1–C4).
- **Method:** `tools/queryproof_interface/` writes adversarial Parquet and ORC files from those
  specs and runs fixed predicate and aggregate templates per concept. There is no random SQL.
- **Writers** are a test axis of their own:
  - pyarrow (arrow-cpp) for Parquet and ORC;
  - Spark for parquet-mr and orc-java copies of the same rows.
- **Oracle:**
  - skipping on vs off within each engine;
  - skipping off vs the generator's Python answer, triaged separately as semantics.
- **Engines:**
  - Spark 4.2.1 and master;
  - DuckDB 2.0.0;
  - DataFusion (pip, latest);
  - Trino, in Docker on a hosted runner, if it fits in one job.
- **Runners:** hosted only, because the fork is public.
- **Baseline:** `run_meta.py` with random SQL, for the same CPU budget, on the engines it
  supports (Spark variants, DuckDB oracle).
- **Accounting:** every workflow run prints `QUERYPROOF_CPU <method> <engine> <seconds>`,
  covering job wall time × cores used. The results table counts distinct new bugs per
  concept × engine × method and bugs per CPU-hour.

## Stop rule

The checkpoint is about three weeks of CI effort, around **2026-11-22**.

- **Continue to a paper** only if both hold:
  - the spec-driven method clearly beats the random baseline in new bugs per CPU-hour, i.e.
    a bigger count, not a tie within one bug;
  - it finds at least one new bug outside Spark.

  The outline then goes in `research/queryproof/` (method, cross-engine bug table, baseline
  comparison).
- **Otherwise stop** and write a workshop or industry-track note around N1, N2, N4 and N5.

Frozen results from earlier rounds are not revised. Failed runs stay recorded as failed.
