# QueryProof programme v3: sound data skipping across the open lakehouse (2026-11-14)

This document is written before any v3 run. Phase one (`2026-11-01-interface-results.md`) and
phase two (`2026-11-03-phase2-results.md`) stay frozen. v3 changes the unit of work from a
round of queries to a **bug family** and its instances. The goal is a programme with
results that last beyond one paper: new bugs, a conformance corpus upstream, spec
clarifications, and a nightly sentinel on engine main branches.

## 1. What the data so far says

| Evidence | Implication for v3 |
| --- | --- |
| The Coq phase (V13–V22) certified traces but found 0 bugs | Proofs are useful only where they generate oracles (§4.4); they are not a hunting tool |
| O4 transplants gave the best yield per core-hour (Z1 in 32 core-h, plus every transplanted bug seen in more readers) | Scale transplantation into **variant hunting** over bug families (§3) |
| The O1 auditor costs about 0.005 core-h per round and is exact (one claim, one column, one unit) | Extend it to ORC, Iceberg manifests and Delta logs (§4.1) |
| O3 cast templates: 75 core-h, 0 new, about 4,600 noise findings | Drop generic casts; keep casts only inside family F1 |
| The brute writer × reader matrix: 76 core-h, 3 new low-severity bugs; every wrong result already reported | Keep the matrix only as a substrate. Do not use it as the search strategy |
| N4/N5 were scooped by days; DataFusion and arrow-rs now fuzz pushdown on/off themselves | Race on **main branches and fresh features**, and turn new tracker reports into input within 24 h (§5) |
| Spec vs random: 5 vs 3 new at equal cost, yield per pair 3, 1, 0, 1 | That comparison will not carry a paper. Evaluate on **recall of known bugs** (§6) and on confirmed and fixed counts |
| Round D (table formats: 17 of the 153 study bugs) was cancelled | Table formats and deletes are the least-fuzzed layer, so they get the first priority |

## 2. Thesis

> Every data-skipping decision (partition, file, stripe/row group, page, bloom filter,
> dictionary, runtime filter, stats-answered aggregate) is a claim that a unit holds no
> qualifying row. It is sound only if the writer's metadata is sound **and** the reader maps
> the predicate into the metadata's domain conservatively. Bugs cluster in a small number of
> mechanism families, and each family recurs across engines, formats and skipping layers.

This gives the programme three contributions:
1. an empirical study and taxonomy (the families);
2. a method (family-driven variant hunting with exact oracles);
3. artifacts (conformance corpus, spec clarifications, nightly sentinel).

## 3. Bug families (the unit of work)

Each family is written as a **mechanism**, not a trigger. A mechanism has a parameter space,
and every cell of (parameters × format × skipping layer × reader) is an instance to test.

| Id | Mechanism | Members so far (ours in bold) | Uncovered instances to generate |
| --- | --- | --- | --- |
| F1 | A literal is mapped onto a coarser storage grid, and the rounding direction is wrong for the operator | **Z1, G, GT**, SPARK-60326, datafusion#22142, polars#22435, ORC-645, SPARK-60797 | TIMESTAMP_NANOS/INT96; Iceberg day/hour/month/truncate transforms; Delta generated partition columns; DECIMAL scale on page index; FLOAT literal vs DOUBLE stats; **truncated string bounds** (Parquet and Iceberg truncate(16): an upper bound must be incremented, never used as exact); collations |
| F2 | NULL at a parent level is not reflected in a child's statistics | N4/SPARK-60362, **T3**, arrow#51626, ClickHouse#119473, **C2** | List/map element nulls; Iceberg nested-field metrics; Delta nested stats JSON; ORC row index; shredded VARIANT fields |
| F3 | An empty unit has statistics that are taken as values | N5/SPARK-60487, SPARK-59607, **X2, W1, N2** | Iceberg 0-record data files; Delta `numRecords=0`; **all rows deleted by a deletion vector or delete file** while min/max still describe them; stats-answered COUNT/MIN/MAX with pending deletes |
| F4 | The statistics' sort order differs from the reader's comparison order | **S**, polars#30043, Trino UINT_32 family, PARQUET-1222, INT96 | ColumnOrder UNDEFINED; FLBA/decimal byte order; UTF-8 vs UTF-16 vs collation in every reader; unsigned types in Iceberg/Delta |
| F5 | A partition value's spelling or type differs between writer and reader | **X1**, datafusion#18083/#24951/#26492, SPARK-60797 | Iceberg partition evolution; Delta partition values as strings; Hive escaping; timestamp time zone in partition paths |
| F6 | An encoding edge case appears at a unit boundary | **TD, Y1**, arrow-rs#11018, duckdb#27454 | Bit width 0 at every width and type; page v2 + DELTA_* at row-group boundaries; skipping into the middle of a miniblock or run |
| F7 | A runtime or dynamic filter is built or applied unsoundly | **N1** | Trino dynamic filtering, DataFusion dynamic filter pushdown (TopN and join), Spark runtime bloom filters, DuckDB join filter pushdown; UNION, NULL keys, HOF keys |
| F8 | Special float values (NaN, ±0) in bounds | duckdb#25521, datafusion#26490/#26491/#26138 | Iceberg/Delta float metrics; page index; bloom filters across every writer |

**Fresh-feature priority.** New code has the most bugs and the fewest fuzzers. Within each
family, test these first: Parquet GEOMETRY/GEOGRAPHY bounding-box statistics, VARIANT
shredding statistics, Iceberg v3 (nanosecond timestamps, deletion vectors, row lineage,
variant), Delta type widening, deletion vectors and variant, and DataFusion and Trino dynamic
filters.

## 4. Oracles, ranked by precision and cost

1. **O1+ metadata auditor (writer side, no query).** Extend the Parquet auditor to:
   - ORC footers and row indexes;
   - Iceberg manifests (`lower_bounds`, `upper_bounds`, `null_value_counts`, `nan_value_counts`,
     `record_count` against the data files, minus deletes);
   - Delta `add.stats` JSON.

   A violation is a writer bug whatever any reader does.
2. **O7 skip-soundness oracle (new, reader side).** Record which units each reader skipped,
   then decode each skipped unit independently and evaluate the predicate on its rows. If a
   skipped unit holds a qualifying row, the reader is unsound, even when the final answer
   happens to be right because of masking. Sources:
   - DataFusion metrics (`row_groups_pruned_statistics`, `page_index_rows_pruned`,
     bloom filter counts);
   - Spark SQL metrics plus a parquet-java row-group filter call on the same predicate;
   - DuckDB `EXPLAIN ANALYZE`;
   - Trino operator stats.

   This catches masked bugs (P8 was masked by #18083) and names the layer.
3. **O8 stats-answer oracle.** Any answer computed from metadata (aggregate pushdown,
   `COUNT(*)` from manifests or from the Delta log, `MIN`/`MAX` from footers) must equal a
   full scan that applies every delete. This targets F3.
4. **Proof-derived oracle tables (the QueryProof heritage).** For F1, prove in Coq that
   skipping on a grid map *g* is sound for operator *op* iff the literal is rounded
   outward: `<` and `>=` use ceil, `<=` and `>` use floor, `=` checks grid membership, and
   `<>` and `NOT IN` never skip on an off-grid literal. Extract the table from the proof
   and use it as the oracle for every engine. This is where the proofs earn their place:
   in the paper, as the reason the oracle has no false positives.
5. The existing differential oracles: skipping on vs off, and the Python reference model.
   Radar-style answers that differ by writer.

## 5. Racing the trackers

- **Daily tracker feed.** Pull new Parquet, ORC, Iceberg, Delta, partition and statistics
  issues from the trackers in the bug study. Classify each into a family and transplant its
  trigger across every engine within 24 h. Other people's reports become our input.
- **Scoop alert.** Each candidate in the filing guide has its search queries (JIRA
  `text ~`, `gh search issues` and `gh search prs`). They run daily, and any hit is flagged
  before we spend more effort on that candidate.
- **Nightly mains sentinel.** Run the family corpus against Spark, DataFusion/arrow-rs,
  DuckDB, Polars, Trino, pyiceberg/iceberg-rust and delta-rs nightly builds. A regression
  found before release (W1 was a main-branch regression; datafusion#26490 entered in 52) is
  new by construction, and maintainers value it most.
- **Disclosure calendar instead of open-ended holding.** Findings are held until a fixed
  batch date (proposed: M1, see §8). Any candidate whose scoop alert fires, or that has a
  fix branch ready, is filed at once. Reviewers count *confirmed* and *fixed* bugs, and both
  take maintainer time we cannot compress at the end.

## 6. Evaluation that does not depend on luck

1. **Historical recall (primary).** Rebuild the 153-issue study (2026-08-01 onward) and
   commit it to the repo as `study/issues.jsonl`, because the scratchpad copy is gone. Then
   extend the study back to 2024. For each wrong-result bug with a known affected version,
   run the method against that version. Report recall per family and per oracle against:
   - the matched random generator;
   - the engines' own fuzzers (datafusion#25453, arrow-rs#10823);
   - a SQLancer-style query-only baseline.

   This number exists whether or not new bugs turn up.
2. **New bugs (secondary),** counted as found / confirmed / fixed, by severity (wrong rows
   > crash > read error > cosmetic), per project. The primary efficiency metric is
   confirmed bugs per core-hour, with bootstrap CIs over shards.
3. **Ablations:** auditor, O7, O8, family generalisation (vs replaying only the original
   trigger), fresh-feature priority, nightly vs release.
4. **Pre-registration** stays: each round has its own dated plan, and frozen results are
   never revised.

## 7. Automation to cut human cost

- **Reducer:** shrink rows, row groups, pages, columns, metadata fields and predicate atoms
  while the oracle still fires, so that every finding arrives minimal.
- **Auto-bisect:** releases from PyPI/Maven/Docker first, then nightlies.
- **Auto-dedup:** `known_issues.json` plus the daily tracker search, applied before triage.
- **Auto-draft:** turn a reduced case into a filing-guide section with a reproduction,
  versions, controls and the search log. A person reviews one representative per cluster.

## 8. Milestones and gates

| Milestone | By | Work | Gate |
| --- | --- | --- | --- |
| M0 | 11-17 | Families file `families.yaml` (mechanism, parameters, members); study rebuilt and committed; tracker feed + scoop alert running | Study ≥ 150 issues, each in a family or in "other" |
| M1 | 11-24 | F3 + F1 on table formats (Iceberg via pyiceberg/Spark/Trino/DuckDB; Delta via delta-rs/Spark/Trino/DuckDB/Polars) with O1+, O7, O8; fresh features | **≥ 4 new wrong-result bugs in ≥ 3 projects**, otherwise fall back to the workshop note (old stop rule). **Batch filing of everything held** |
| M2 | 12-08 | All eight families on all layers; nightly sentinel live; Coq F1 table extracted | Historical recall measured for every family |
| M3 | 12-22 | Upstream: corpus PR to apache/parquet-testing (plus Iceberg/Delta test data); spec clarification PRs or issues (optional `null_counts` with all-null pages, C2; empty-list footer, W1; truncated-bound semantics) | At least one artifact accepted or under review |
| M4 | Jan | Paper: study + method + recall + new bugs. Target a DB venue with rolling deadlines (VLDB) or the next SE testing venue (check dates when the paper is ready) | |

Compute: the fork is public, so the runs stay on GitHub-hosted runners as before. The
tracker feed is cheap and can run as a scheduled job in the fork.

## 9. What is out

- Optimizer-rule certification (V-series) as a hunting method. It stays as background and
  as the source of §4.4.
- Generic cast templates (O3).
- Brute rounds over the full matrix without a family hypothesis.
- Counting rediscoveries as results, except as recall in §6.1.
