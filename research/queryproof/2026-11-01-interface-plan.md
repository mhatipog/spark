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

## Pair three design (2026-11-01, not yet built)

Contract items no shape covers yet, picked from C1–C4 only:

- **C3 UINT_\* ordering:** uint8/16/32/64 columns with values around the signed maximum
  (127/128/255, 2^31−1/2^31/2^32−1, 2^63−1/2^63/2^64−1). Files are split so that some
  files' statistics cross the signed boundary. Parquet only: Arrow's ORC writer has no
  unsigned types. Needs uint types in `shapes.py` (`parse_type`, `to_arrow`, `sql_type`:
  Spark reads UINT_8→SMALLINT, UINT_16→INT, UINT_32→BIGINT, UINT_64→DECIMAL(20,0)), a
  per-shape `formats` switch in `writers.py` and `run_interface.py`, and the same types in
  `random_gen.TYPES` (matched baseline).
- **C2 E3:** a directory with no data file (`files=[]`).
- **C2 E2:** a 0-row row group next to a non-empty one (a pyarrow ParquetWriter that
  writes an empty batch first).
- **C2 templates:** GROUP BY over a predicate that prunes everything, and a scalar
  subquery over the global aggregates, compared with 0 / NULL.
- ±Infinity is already covered (O3).

Run with `all_error` on (both passes): spec3 against random seed 4 at a matched check
count.
