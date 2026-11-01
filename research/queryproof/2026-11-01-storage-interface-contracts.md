# Storage-interface contracts: four shared concepts (2026-11-01)

**Thesis.** Wrong-result bugs cluster where two layers read one concept differently: the file
writer's statistics vs the reader's skipping, a pushed-down predicate vs the reader, an
aggregate rewrite vs the scan. Degenerate data separates the two readings, and random SQL
over random rows seldom produces it. The pilot evidence is two Spark bugs from round 7:

- **N4:** an ORC child column's statistics leave out rows where the parent struct is NULL.
- **N5:** a pushed-down COUNT over no file becomes a SUM over nothing.

Each concept below states:

- **W:** what the writer, or the format specification, guarantees.
- **R:** what each reader or optimizer assumes.
- **S:** data shapes that would separate W from R.

`tools/queryproof_interface/` generates the S shapes and the predicate and aggregate
templates directly from these sections. Its shape and template ids are cited in brackets.

**Epistemic status.** The W lines come from the Parquet format specification (parquet-format
2.10/2.11) and the ORC v1 specification, plus the known issues cited. Claims marked *(verify)*
are my reading and have not yet been checked against the code. The generator records the
statistics it actually wrote (via the file footer), so the oracle never relies on these
claims.

---

## C1. NULL semantics in statistics

**W.**
- **Parquet.** Column-chunk and page statistics belong to leaf columns.
  - A leaf value whose definition level is below the maximum counts as null in `null_count`.
    This includes values that are null because an ancestor struct or list is null *(verify per
    writer: parquet-mr, arrow-cpp)*.
  - `null_count` is optional. Old writers omit it, and a missing value means "unknown", not 0.
  - The page index (`ColumnIndex`) has `null_pages` (whether each page is all nulls) and an
    optional `null_counts`.
  - min/max are absent when every value is null.
- **ORC.** Each column has a PRESENT stream and its own statistics.
  - A child of a struct, list or map has an entry only for rows where the parent is present.
  - The child's `hasNull` and `numberOfValues` describe that child's own present stream, so a
    row whose parent is NULL leaves no trace in the child's statistics (N4).
  - For list elements and map keys and values, the statistics range over elements, not rows.

**R.**
- **Spark OrcFilters:** before the N4 fix branch, `IsNull(st.x)` was pushed as a leaf on
  `st.x`, which assumes the row-level semantics.
- **Spark ParquetFilters:** pushes `IsNull` and `IsNotNull` on nested fields; this is safe if
  W holds for the writer.
- **DuckDB, DataFusion, Trino:** row-group pruning on `IS NULL` uses `null_count`, so each is
  correct exactly when its assumption about a missing `null_count` and ancestor nulls matches
  the writer.
- **Trino:** its ORC reader prunes on dereferenced nested columns *(verify)*, so it is the
  closest candidate for an N4 analogue outside Spark.

**S.**
- [N1] A struct that is NULL in every row of a stripe or row group, so the child has 0 values.
- [N2] A struct NULL in some rows while the child is never NULL when the struct is present.
  This is the N4 shape.
- [N3] A struct present with the child NULL, against a struct that is NULL.
- [N4] A NULL list, an empty list and a list holding one NULL element.
- [N5] A map with a NULL value against a missing key.
- [N6] Two nesting levels (`a.b.c`) with NULL at each level.
- [N7] A Parquet file written with `null_count` omitted *(writer support to verify; otherwise
  skip)*.

Templates:
- `x IS NULL`, `x IS NOT NULL`, `x <=> NULL`, `NOT (x IS NOT NULL)`;
- `x IS NULL OR x = v`, `coalesce(x, v) = v`;
- `count(x)`, `count(*) WHERE x IS NULL`, `size(arr) = 0`, `arr IS NULL`.

## C2. Empty and fully pruned input

**W.**
- A Parquet file can hold 0 rows, with 0 row groups or with a row group of `num_rows = 0`.
- An ORC file can have 0 stripes.
- A directory can hold no data file at all, only `_SUCCESS`, or no file once partitions are
  pruned.
- Statistics of an all-NULL column carry counts but no min or max.

**R.**
- The SQL contract: a global `COUNT` over no rows is 0, `SUM`, `MIN` and `MAX` are NULL, and a
  grouped aggregate returns no row.
- Aggregate pushdown answers from metadata:
  - Spark DSv2 `{parquet,orc}.aggregatePushdown` (N5);
  - DataFusion's statistics-based `AggregateStatistics` rule *(verify which statistics it
    trusts as exact)*;
  - DuckDB `count(*)` from metadata.
- Each rewrite must keep the empty case:
  - COUNT becomes 0, not NULL;
  - MIN/MAX of an all-NULL column becomes NULL, not an error, and not a missing statistic read
    as 0;
  - COUNT(x) equals `num_rows − null_count` only when `null_count` is present.

**S.**
- [E1] A 0-row file with its schema.
- [E2] A 0-row row group next to a non-empty one *(writer to verify)*.
- [E3] A directory with no data file.
- [E4] A partition filter that prunes every file.
- [E5] An all-NULL column, both 1 row and many.
- [E6] One file with a single row against one file with several row groups.
- [E7] `null_count` missing in an all-NULL chunk.

Templates:
- `count(*)`, `count(x)`, `min(x)`, `max(x)`, `sum(x)`, all as a global aggregate;
- the same with `GROUP BY` and with a `WHERE` that prunes everything;
- a scalar subquery over each, compared with 0 or NULL.

## C3. Min/max ordering

**W.**
- **Parquet.** The ColumnOrder `TYPE_DEFINED_ORDER` sets the ordering per type:
  - BYTE_ARRAY/UTF8 compares unsigned bytes. The legacy `min`/`max` fields used signed bytes,
    so readers must ignore them for byte arrays (PARQUET-686).
  - DECIMAL stored as FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY compares signed two's complement.
  - UINT_* compares unsigned.
  - INT96 has no defined order.
  - FLOAT and DOUBLE:
    - writers must not put NaN into min or max;
    - a min of +0.0 must be written as −0.0 and a max of −0.0 as +0.0, or readers must widen
      them;
    - 2.11 adds `nan_count`.
  - Statistics may be truncated, and the 2.10 flags `is_min_value_exact` and
    `is_max_value_exact` say so. A truncated maximum is an upper bound, not a value.
- **ORC.** String minimum and maximum were truncated (ORC-203, lowerBound/upperBound), and
  older Java writers compared UTF-16 code units *(verify)*. Decimal and timestamp statistics
  have their own encodings (ORC-135 changed the timestamp time zone).

**R.**
- Any reader that prunes with `<`, `<=`, `=` or `BETWEEN`, or answers `MIN`/`MAX` from
  statistics, depends on the reader's ordering equalling the writer's.
- A bound must be used as a bound, not as an exact value. MIN/MAX pushdown that returns a
  truncated maximum is wrong.
- Collated or case-insensitive comparisons (Spark `UTF8_LCASE`) must not prune with binary
  statistics.

**S.**
- [O1] Characters above U+FFFF next to U+E000–U+FFFF. UTF-8 byte order and UTF-16 code-unit
  order disagree on these, e.g. U+FFFD sorts below U+1F600 in UTF-8 and above it in UTF-16.
- [O2] Strings longer than the truncation length that share a long prefix, and a maximum of
  bytes `0xFF…` that cannot be rounded up.
- [O3] NaN only, NaN mixed with numbers, −0.0 and +0.0 alone and mixed, ±Infinity.
- [O4] Negative decimals stored as FIXED_LEN_BYTE_ARRAY, INT32 or INT64, and decimals of
  maximal precision.
- [O5] Dates and timestamps before 1582 and before 1970, plus INT96.
- [O6] Case variants under `UTF8_LCASE`.

Templates:
- `x < v`, `x <= v`, `x = v`, `x > v`, `x BETWEEN v1 AND v2`, `x IN (…)`;
- `min(x)`, `max(x)`;
- each with v at, just below and just above every value in the file;
- for floating point, v ∈ {NaN, −0.0, 0.0, ±Inf}.

## C4. Dictionary and bloom-filter skipping

**W.**
- **Parquet dictionary.**
  - The dictionary page holds only the values of the dictionary-encoded pages.
  - A writer that falls back to plain encoding partway through a chunk leaves values that
    are not in the dictionary.
  - `encoding_stats` (or `encodings`) tells the reader whether every data page is
    dictionary-encoded.
- **Parquet bloom filter.** A split-block filter over xxHash64 of the *plain encoding of the
  physical type*:
  - INT32 hashes 4 bytes and INT64 8;
  - FLOAT and DOUBLE hash the IEEE bits, so −0.0 and +0.0 hash differently and NaN has many
    encodings;
  - DECIMAL hashes its physical encoding.
- **ORC bloom filter.** Per row-index stride. It hashes long values for integer types, double
  bits, and UTF-8 bytes for strings. ORC-101 replaced the default-charset BloomFilter with
  BloomFilterUtf8.

**R.**
- A reader can skip a row group by the dictionary or the bloom filter only for `=` and `IN`,
  and only if it:
  - hashes or looks up the literal in the file's physical type and encoding, not in the
    table's logical type after type widening (e.g. INT32 in the file read as BIGINT);
  - treats −0.0 = +0.0 and handles NaN;
  - does not use the dictionary when a chunk fell back to plain encoding;
  - does not apply a binary-hash filter to a collated or case-insensitive equality.

**S.**
- [D1] A dictionary that falls back mid-chunk (a tiny dictionary page size limit).
- [D2] A bloom filter on INT32 read under a BIGINT schema, on FLOAT read as DOUBLE, and on
  DECIMAL(5,2) read as DECIMAL(10,2) (Spark 4 type widening).
- [D3] A bloom filter or dictionary over −0.0, +0.0 and NaN.
- [D4] A bloom filter on a string column queried with `UTF8_LCASE` equality.
- [D5] A dictionary on, against off, against page v2, with the same values.
- [D6] An ORC bloom filter on string, decimal and timestamp columns.

Templates: `x = v` and `x IN (…)` with v inside, outside and on each bloom or dictionary
boundary value; the same under a widened read schema.

---

## Oracle

The oracle requires the following, with each file read under the declared schema:

1. **Within an engine:** the same query must return the same bag with every skipping
   mechanism on as with all of them off. The mechanisms are pushdown, statistics,
   dictionary, bloom, page index and aggregate pushdown, and the switches are listed per
   engine in `tools/queryproof_interface/engines.py`.
2. **Across engines:** the result with skipping off must equal the logical answer computed by
   the generator in plain Python from the rows it wrote. With skipping off, a disagreement is
   a reader or semantics difference, and it is triaged separately; it is not counted as a
   skipping bug.

A finding counts as **new** only after it is:

- reduced to the smallest shape;
- reproduced on a released version;
- checked against the issue tracker of the engine and of the writer library.

## Baseline

Baseline: `run_meta.py` with random SQL over random rows, for the same CPU budget and on
the same engines where possible: Spark variants, plus DuckDB as a cross-engine oracle.
The measure is distinct new bugs per CPU-hour, broken down by method and engine.

The stop rule is in `research/queryproof/2026-11-01-interface-plan.md`.
