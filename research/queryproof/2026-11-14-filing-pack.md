# Filing pack (2026-11-14)

This replaces the ordering in `UPSTREAM_FILING_GUIDE.md`; that file stays as the long record
(run ids, scope tables, negative controls). The texts below are the short versions to paste.

Every "novel" verdict below is from a re-search done on 2026-11-14:
- Apache JIRA, using full-text `text ~` queries;
- `gh search issues` and `gh search prs` on each repository;
- the current code on each project's main branch.

Run the repro once more yourself and repeat the search right before you file. Several areas
(Spark timestamp pushdown, pyiceberg float handling, delta-rs statistics) are being worked
on by other people this month.

## 1. Ranking

Impact is a judgement of how likely real users hit the bug, not only how bad it is when they do.

### File

| Order | Id | Project | What | Severity | Real-world exposure | Code still wrong on main |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | A1 | Arrow C++ (pyarrow ORC writer) | any struct with nulls below a struct without nulls (also through list/map, e.g. struct<list<struct>>) shifts child values | **silent corruption on write** | medium-high (the usual shape of nested JSON-like data) | yes (26.0.1) |
| 2 | Z1 | Spark | Parquet pushdown floors a sub-ms literal on TIMESTAMP_MILLIS; `<`, `<>`, `NOT IN` drop rows | wrong rows, default config | low-medium | yes (`timestampToMillis`) |
| 3 | T3 | Trino | ORC `a.b.c IS NULL` skips rows where the middle ROW is NULL | wrong rows, default config | medium | yes |
| 4 | X1 | DataFusion | `part_col = v` lists one directory spelling; pyarrow TIMESTAMP partitions return nothing | wrong rows, default config | medium-high | yes (`evaluate_partition_prefix`) |
| 5 | X2 | DataFusion | MIN/MAX of a partition column counts directories holding only 0-row files | wrong rows, default config | medium | yes (`with_statistics`) |
| 6 | TD | Trino | DELTA_BINARY_PACKED TINYINT/SMALLINT, bit-width-0 miniblock: "byte overflow" | query failure on valid file | medium | yes (`toByteExact`) |
| 7 | G | Spark | DATE/TIMESTAMP literal in 1582-10-05..14 pushed as a different value | wrong rows, default config | low (historical dates) | yes; fix branch ready |
| 8 | S | ORC Java | SARG compares strings in UTF-16 order; statistics are UTF-8 order | wrong rows in Spark, default config | low | yes |
| 9 | N1 | Spark | DPP filter left in a non-partitioned UNION branch; fails without codegen | query failure | low-medium | yes; fix branch ready |
| 10 | W1 | arrow-rs | regression: 0-row fastparquet files fail ("Expected list element type") | read failure | low-medium | yes (55.0.0) |
| 11 | N2 | Spark | CBO `evaluateBinaryForNumeric` NaN when ndv = 0 | planning failure (CBO only) | low | yes; fix branch ready |
| 12 | C2 | Trino | column index with only null pages and no `null_counts` → "Corrupted column index" | query failure | low (ClickHouse-written files) | yes |
| 13 | — | iceberg-rust | comment: ask for a 0.10.x backport of #3323 (day transform), because pyiceberg pins `pyiceberg-core<0.11` | data written to the wrong partition | low-medium | fixed in 0.11.0, not reachable from pyiceberg |

### Do as a PR, not an issue

| Id | Why |
| --- | --- |
| Y1 (datafusion-python `created_by` has no version) | A one-line default change. An issue for it is noise; a 3-line PR with a test is welcome. |

### Optional comments (only if they add information the ticket lacks)

| Ticket | Value |
| --- | --- |
| SPARK-59607 | It lists only 5.0.0 and Parquet; we show 3.5–4.2.1 and ORC. Useful to whoever backports. Keep it to 4 lines. |
| SPARK-57499 | is_variant_null on V2 still fails on branch-4.2. Post only after checking branch-4.2 again. |

### Lost (someone reported or fixed it first): do not file

| Id | Lost to | Note |
| --- | --- | --- |
| N4 | SPARK-60362 (10-19), PR apache/spark#59747 | |
| N5 | SPARK-60487 (10-22), PR #59763 | |
| SPARK-57956 backport comment | backported to 4.2.2/4.1.5/4.0.6 on 11-05 | moot, drop it |
| I1 pyiceberg: float column, double literal; pruning uses the unrounded value, so `EqualTo("v", 0.1)` returns nothing | apache/iceberg-python#4108 (open, 10-14) | a +1 with the one-line repro is fine if #4108 stalls |
| I3 pyiceberg: NaN in an identity-partitioned float column, ZeroDivisionError on append | iceberg-python#4030 (merged 09-30, not in 0.12.0) | |
| Iceberg day transform floors wrong before 1970 (iceberg-rust `pyiceberg-core` 0.10.1) | iceberg-rust#3315 / #3323 | only the backport comment (row 13) is left |
| D1 delta-rs decimal statistics written through f64 (`-1.000000000000000001` → `-1.0`, wrong rows in delta-rs, Polars, DuckDB) | fixed on main 11-01 (a3b0ccf1cd, d0c31d5d71), unreleased | |
| D2 NaN left out of Parquet/Delta max, engines that sort NaN last skip it | datafusion#15812, duckdb#25521 | |
| NaN read back as the statistics constant | datafusion#26490, delta-rs#5007 | |

### Not worth filing (borderline spam)

| Finding | Reason |
| --- | --- |
| pyiceberg month/year projection raises OverflowError for `d < DATE '0001-01-01'` | edge of the type range, no wrong rows; reads as fuzzer output |
| pyiceberg `truncate[4]` on `decimal(9,2)` refuses `-9999999.99` (needs precision 10) | spec corner at the type minimum; no user will hit it |
| pyiceberg `NOT IN {-0.0}` differs between file layouts | real but tiny. #4324 and #4331 just reworked signed zero there; mention it on those PRs at most |
| N3 VARIANT under `factoryMode=NO_CODEGEN` | internal test mode, by design |
| Any of the above bundled into a "found N bugs" issue | always noise |

## 2. How to file without reading as automated output

Maintainers in these projects now look at this directly. DataFusion's lead maintainer has
started labelling PRs from authors who opened PRs in more than 20 repositories in 30 days
(apache/datafusion#23839). This account already has recent PRs in microsoft/fabric-cicd,
openaq, apache/iceberg, apache/arrow and pola-rs/polars. Filing 13 reports across 7 projects
in one evening would look like a sweep, however good each report is.

- **Pace:** at most one report per project per day, about two weeks in all. Start with the
  items that have the most impact and the clearest root cause.
- **One root cause per report.** No lists of "related findings" and no counts of bugs found.
- **Run every repro yourself** on a clean environment, and paste your own output. Do not
  paste output you have not seen.
- **Answer follow-ups yourself, the same day if you can.** A report that goes quiet after a
  question is what gets a reporter labelled. If you cannot defend a suggested fix, leave the
  fix out and describe only the behaviour.
- **Be honest about tooling.** The texts say the bug was found by differential testing,
  which is true. Spark's PR template asks whether generative AI was used, and the ASF
  generative-tooling policy applies to code you submit. Answer it truthfully on the fix PRs
  for N1, N2 and G. Plain writing is the goal, not hiding how the bug was found.
- **Do not open PRs** for N4/N5, or from the `*-test-only-control` branches.
- Squash the three G commits before the PR. Rename `[SPARK-XXXXX]` once the JIRA exists.

Suggested calendar (move it freely, but keep the one-per-project-per-day rule):

| Day | Reports |
| --- | --- |
| 1 | Z1 (Spark JIRA), A1 (Arrow), TD (Trino) |
| 2 | X1 (DataFusion), T3 (Trino) |
| 3 | G (Spark JIRA) |
| 4 | X2 (DataFusion), S (ORC JIRA) |
| 5 | N1 (Spark JIRA), W1 (arrow-rs) |
| 7 | C2 (Trino), iceberg-rust backport comment |
| 8 | N2 (Spark JIRA); fix PRs for G and N1 once their JIRAs have been looked at |
| later | Y1 PR; optional SPARK-59607 comment |

Spark needs an ASF JIRA account (selfserve.apache.org/jira-account.html). Approval can take a
day, so request it first.

## 3. Texts

### A1: apache/arrow, Bug report, components C++ and Python

**Title:** `[C++][ORC] Writing struct<struct<…>> with a null inner struct shifts the child values`

````
Writing a nested struct to ORC loses data when the inner struct has a null but the outer struct has none. Every child value after the null row is shifted by one, and the first shifted value is written as 0 (or "" for strings).

```python
import pyarrow as pa, pyarrow.orc as orc
inner = pa.struct([("c", pa.int32())])
t = pa.table({"o": pa.array([{"i": {"c": 1}}, {"i": None}, {"i": {"c": 3}}], pa.struct([("i", inner)]))})
orc.write_table(t, "/tmp/a.orc")
orc.read_table("/tmp/a.orc").to_pylist()
# [{'o': {'i': {'c': 1}}}, {'o': {'i': None}}, {'o': {'i': {'c': 0}}}]   expected c = 3
```

The ORC Java reader (Spark) returns the same wrong values, so the file itself is wrong.

The same happens wherever a struct with nulls sits below another struct that has no nulls in the batch, also through a list or a map:

```python
S = pa.struct([("c", pa.int32())])
pa.array([{"l": [None, {"c": 1}, {"c": 2}]}], pa.struct([("l", pa.list_(S))]))   # reads back [None, {c: 0}, {c: 1}]
pa.array([{"m": [(1, None), (2, {"c": 2})]}], pa.struct([("m", pa.map_(pa.int32(), S))]))   # reads back (2, {c: 0})
pa.array([[{"i": {"c": 1}}, {"i": None}, {"i": {"c": 3}}]], pa.list_(pa.struct([("i", S)])))   # reads back c = 0
```

Not affected: a top-level list<struct> or map<k, struct> with null elements, list<list<…>> and struct<list<int>> with null lists, and any case where the outer struct also has a null in the batch. A round trip over a few thousand random nested schemas outside that shape found no other differences.

Seen on pyarrow 12.0.1 through 26.0.1 (Linux and Windows).

It looks like a struct's null mask is not passed on to its children when the enclosing struct reports no nulls, so the child gets an extra value for each null row. The writer side is WriteStructBatch in cpp/src/arrow/adapters/orc/util.cc.

Found with a differential test harness (the same rows read back by several engines).
````

### Z1: Spark JIRA, Bug, SQL, label `correctness`, Major

**Summary:** `Parquet filter pushdown on TIMESTAMP_MILLIS columns truncates sub-millisecond literals and skips matching rows`

```
ParquetFilters.timestampToMillis floors the literal to whole milliseconds. That is safe for =, >, >=, <=, because Spark re-evaluates the filter after the scan, but not for < and <> (and NOT IN, which becomes notEq). Row groups whose min, or whose only value, equals the floored literal are skipped. Default configuration.

{code:python}
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
for t in ["1900-01-01 00:00:00", "2020-01-01 00:00:00"]:   # one file each
    spark.sql(f"SELECT TIMESTAMP'{t}' AS t").coalesce(1).write.mode("append").parquet("/tmp/z1")
df = spark.read.parquet("/tmp/z1")
df.where("t < TIMESTAMP'1900-01-01 00:00:00.000001'").count()    # 0, expected 1
df.where("t <> TIMESTAMP'2020-01-01 00:00:00.000001'").count()   # 1, expected 2
{code}

Correct with spark.sql.parquet.filterPushdown=false, with TIMESTAMP_MICROS files, and with literals on the millisecond grid. Same result with the V2 source and with the vectorized reader off.

Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master (b2fc1bef1b).

The ORC equivalent was SPARK-60326 / SPARK-60436. For Parquet, valueCanMakeFilterOn already refuses sub-microsecond bounds; refusing literals that are not on the millisecond grid for ParquetTimestampMillisType would fix it (or round lt up and skip notEq for them).

Found with a differential test harness (the same files with pushdown on and off).
```

### T3: trinodb/trino, Bug report

**Title:** `Hive ORC: IS NULL on a field of a nested ROW skips rows where the intermediate ROW is NULL`

````
For `o ROW(i ROW(c INTEGER), z INTEGER)` in an ORC file, `WHERE o.i.c IS NULL` does not return a row where `o` is present and `o.i` is NULL. `WHERE o.i IS NULL` does return it, a NULL top-level `o` is handled correctly, and the same rows in Parquet are fine.

```python
import pyarrow as pa, pyarrow.orc as orc
T = pa.struct([("i", pa.struct([("c", pa.int32())])), ("z", pa.int32())])
orc.write_table(pa.table({"k": pa.array([2, 4], pa.int32()),
                          "o": pa.array([{"i": None, "z": 1}, {"i": {"c": 7}, "z": None}], T)}), "/data/t/part-0.orc")
```
```sql
CREATE TABLE hive.s.t (k integer, o row(i row(c integer), z integer))
  WITH (external_location = 'local:///data/t', format = 'ORC');
SELECT k FROM hive.s.t WHERE o.i.c IS NULL;   -- no rows, expected 2
SELECT k FROM hive.s.t WHERE o.i IS NULL;     -- 2
```

Trino 484, Hive connector. The result changes once any row in the stripe has an explicit NULL in `c`, so this looks like stripe/row-group skipping: ORC keeps a child's statistics only for rows where its parent is present, so `c` reports hasNull = false. The reader seems to account for a NULL top-level column but not for a NULL intermediate ROW. Spark had the one-level version of this (SPARK-60362).

Found with a differential test harness (the same files across engines, skipping on and off).
````

### X1: apache/datafusion, Bug report

**Title:** `Equality filter on a partition column skips directories that spell the value differently`

````
`evaluate_partition_prefix` turns `part_col = literal` into a listing prefix built from the literal's `to_string()`, so only that one spelling is listed. Directories holding the same typed value under another valid spelling are skipped. The most common case is a TIMESTAMP partition written by pyarrow:

```python
import datetime as dt, pyarrow as pa, pyarrow.dataset as ds, datafusion
t = pa.table({"k": pa.array([1, 2], pa.int32()),
              "t": pa.array([dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)] * 2, pa.timestamp("us", tz="UTC"))})
ds.write_dataset(t, "/tmp/x1", format="parquet", partitioning=["t"], partitioning_flavor="hive")
ctx = datafusion.SessionContext()
ctx.register_parquet("t", "/tmp/x1", table_partition_cols=[("t", pa.timestamp("us", tz="UTC"))], file_extension=".parquet")
ctx.sql("SELECT k FROM t WHERE t = TIMESTAMP '2020-01-02 03:04:05+00:00'").show()   # empty, expected 1, 2
ctx.sql("SELECT k FROM t WHERE t >= TIMESTAMP '2020-01-02 03:04:05+00:00'").show()  # 1, 2
```

The same happens for an INT partition directory `p=01` (only `p=1` is listed) and a DECIMAL `d=1.5`.

DataFusion 55.0.0. The code on main is unchanged. The existing guard only covers percent-encoded spellings. Using the prefix only for string partition columns, whose value is its spelling, would avoid this; other types would then be filtered after parsing, as range predicates already are.
````

### X2: apache/datafusion, Bug report

**Title:** `MIN/MAX of a partition column includes partitions whose files have 0 rows`

````
`PartitionedFile::with_statistics` sets exact `min = max = partition_value` for partition columns regardless of `num_rows`. With a 0-row file in a partition, the aggregate-statistics rule answers MIN/MAX from those statistics without reading data:

```python
import os, pyarrow as pa, pyarrow.parquet as pq, datafusion
for d, rows in (("q=1", [1]), ("q=7", [])):
    os.makedirs(f"/tmp/x2/{d}"); pq.write_table(pa.table({"k": pa.array(rows, pa.int32())}), f"/tmp/x2/{d}/f.parquet")
ctx = datafusion.SessionContext()
ctx.register_parquet("t", "/tmp/x2", table_partition_cols=[("q", pa.int32())], file_extension=".parquet")
ctx.sql("SELECT count(*), min(q), max(q) FROM t").show()   # 1, 1, 7 — expected 1, 1, 1
```

Correct with `datafusion.execution.collect_statistics = false` or with `GROUP BY q`. Empty files in partitions are common (empty tasks, filtered writes).

DataFusion 55.0.0. On main, #26310 changed the NULL-partition case, but a non-null value still gets exact min/max when `num_rows` is `Exact(0)`. Those could be `Absent` for empty files.
````

### TD: trinodb/trino, Bug report

**Title:** `Parquet: DELTA_BINARY_PACKED TINYINT/SMALLINT fails with "byte overflow" when a miniblock has bit width 0`

````
```python
import pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pa.table({"b": pa.array([-128, 127], pa.int8())}), "/data/t/part-0.parquet",
               use_dictionary=False, column_encoding={"b": "DELTA_BINARY_PACKED"}, data_page_version="2.0")
```
```sql
CREATE TABLE hive.s.t (b tinyint) WITH (external_location = 'local:///data/t', format = 'PARQUET');
SELECT count(*), sum(b) FROM hive.s.t;   -- GENERIC_INTERNAL_ERROR: byte overflow, expected 2, -1
```

SMALLINT with [-32768, 32767] fails with "short overflow". pyarrow, DuckDB, DataFusion and Spark read the file. Trino 476, 480 and 484.

`DeltaPackingUtils.unpackDelta` calls `toByteExact(minDelta)` / `toShortExact(minDelta)` for bit-width-0 miniblocks. The deltas of an INT32 column are int32 (here 255); only their running sum wraps back into range. The bit-width > 0 path narrows without the check, which is why random data reads fine. A plain `(byte) minDelta` / `(short) minDelta` cast should fix it.
````

### G: Spark JIRA, Bug, SQL, label `correctness`, Major

**Summary:** `Filter pushdown with a DATE or TIMESTAMP literal between 1582-10-05 and 1582-10-14 skips matching rows`

```
Those days exist in Spark's proleptic Gregorian calendar but not in java.sql.Date's hybrid calendar. DataSourceStrategy converts the literal with convertToScala, so the pushed filter carries 1582-10-15 (DATE) or a time ten days later (TIMESTAMP), and row groups are skipped. Default configuration.

{code:python}
import datetime
for d in ["1582-10-04", "1582-10-15", "1582-10-16"]:   # one file each
    spark.createDataFrame([(datetime.date.fromisoformat(d),)], "d DATE").coalesce(1).write.mode("append").parquet("/tmp/gap")
df = spark.read.parquet("/tmp/gap")
df.where("d > DATE'1582-10-10'").count()    # 1, expected 2
df.where("d <> DATE'1582-10-10'").count()   # 2, expected 3
{code}

PushedFilters shows GreaterThan(d,1582-10-15). ORC behaves the same, also with spark.sql.datetime.java8API.enabled=true (OrcFilters converts LocalDate back to java.sql.Date). TIMESTAMP_MICROS Parquet columns are affected as well, including for equality, because the stored value is the gap value itself.

Reproduced on 3.5.9, 4.0.5, 4.1.4, 4.2.1 and master. I have a patch that skips pushdown for literals in the gap, with tests, and can open a PR.

Found with a differential test harness (pushdown on vs off).
```

### S: ORC JIRA, Bug, Java, Major

**Summary:** `SARG evaluation compares STRING literals in UTF-16 order against UTF-8-ordered statistics`

```
StringStatisticsImpl keeps min/max in UTF-8 byte order (Text.compareTo). RecordReaderImpl.getBaseObjectForComparison turns the predicate literal into a java.lang.String, and the range check then uses String.compareTo (UTF-16 code units). The orders disagree for supplementary characters vs U+E000..U+FFFF: in UTF-8, U+1F600 sorts after U+E000; in UTF-16 its high surrogate 0xD83D sorts before it. Row groups are skipped for predicates they satisfy.

Through Spark 4.2.1 (ORC 2.3.0), one file per row:
{code:python}
for k, s in [(1, "\U0001F600"), (2, ""), (3, "￿"), (4, "\U00010000"), (5, "z")]:
    spark.createDataFrame([(k, s)], "k INT, s STRING").coalesce(1).write.mode("append").orc("/tmp/astral")
spark.read.orc("/tmp/astral").where("s > ''").select("k").show()   # 3 only, expected 1, 3, 4
{code}

Correct with spark.sql.orc.filterPushdown=false. Files written by the C++ writer are pruned the same way, so it is the reader side. Comparing STRING literals and bounds as UTF-8 bytes (e.g. as Text) would fix it.
```

### N1: Spark JIRA, Bug, SQL, Major

**Summary:** `DPP predicate pushed into a non-partitioned UNION branch fails with "IN dynamicpruning#N has not finished" without whole-stage codegen`

```
{code:sql}
CREATE TABLE p (x INT, k INT) USING parquet PARTITIONED BY (k);
INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, NULL);
CREATE TABLE n (k INT, v INT) USING parquet;
INSERT INTO n VALUES (1, 10), (2, 20), (5, 50);
CREATE TABLE d (k INT) USING parquet;
INSERT INTO d VALUES (1), (2), (7);
SET spark.sql.codegen.wholeStage=false;
SELECT u.k FROM (SELECT k FROM p UNION ALL SELECT k FROM n) u JOIN d ON u.k = d.k WHERE d.k IN (1, 2);
-- IllegalArgumentException: requirement failed: input[0, int, true] IN dynamicpruning#64 has not finished
{code}

Since SPARK-39217 the DPP filter is pushed into every UNION branch. On the non-partitioned branch it stays in a FilterExec. InSubqueryExec does not broadcast DPP results (driver-only), so the interpreted filter evaluates it in a task with no result. With codegen it is evaluated on the driver and works.

On 3.5.9–4.2.1 the same failure happens with default settings when the non-partitioned branch's key is a higher-order function (e.g. aggregate(arr, 0, (a, e) -> a + e)); that variant passes on master. The codegen-off repro still fails on master (f090eaca12).

Dropping DPP predicates from branches whose scan is not partitioned on the key (e.g. in CleanupDynamicPruningFilters) fixes it; I have a patch with a test.
```

### W1: apache/arrow-rs, Bug report

**Title:** `parquet: 0-row files from fastparquet fail with "Expected list element type of Struct but got Byte" (regression)`

````
fastparquet writes the empty `row_groups` list of FileMetaData with the one-byte header 0x00. #8826 reported this and #8827 accepted it; the list validation added in #9924 checks the element type even when the list is empty, so these files are rejected again.

```python
import pandas as pd, fastparquet, datafusion
fastparquet.write("/tmp/w1.parquet", pd.DataFrame({"k": pd.array([], dtype="Int32")}))
ctx = datafusion.SessionContext()
ctx.register_parquet("t", "/tmp/w1.parquet")
ctx.sql("SELECT count(*) FROM t").show()
# DataFusion 55.0.0: Parquet error: Expected list element type of Struct but got Byte. 54.0.0 reads it.
```

Files with rows from the same writer open fine. Skipping the element-type check for size-0 lists would restore the old behaviour. (Polars fixed the same thing in pola-rs/polars#27749.)
````

### N2: Spark JIRA, Bug, SQL/Optimizer, Minor

**Summary:** `CBO: FilterEstimation.evaluateBinaryForNumeric throws NumberFormatException when distinctCount is 0`

```
{code:sql}
CREATE TABLE p (b INT, c INT, a INT) USING parquet PARTITIONED BY (a);
INSERT INTO p VALUES (1, 1, 1), (-1, 3, -3);
CREATE TABLE t (k INT, v INT) USING parquet;   -- empty
ANALYZE TABLE p COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;
SET spark.sql.cbo.enabled=true;
SELECT p.a FROM p JOIN t ON p.a = t.k WHERE p.a < 1 AND t.k > 0;
-- java.lang.NumberFormatException: Character N is neither a decimal digit number ...
--   at ...FilterEstimation.evaluateBinaryForNumeric(FilterEstimation.scala:562)
{code}

p.a < 1 plus the inferred p.a > 0 leaves a column stat with distinctCount 0 but min/max set. At the boundary case, percent = 1.0 - 1.0 / ndv is -Infinity, ceil(0 * -Infinity) is NaN, and the BigDecimal conversion throws. SPARK-23799 fixed the same division in evaluateInSet; evaluateBinaryForNumeric needs the same ndv == 0 guard. 3.5.9 through master; I have a patch with a test.
```

### C2: trinodb/trino, Bug report

**Title:** `Parquet column index with only null pages and no null_counts fails as "Corrupted column index"`

````
`null_counts` is optional in ColumnIndex. When it is absent and every page is a null page, `TupleDomainParquetPredicate.getDomain` skips all pages, passes empty min/max lists to the typed `getDomain`, and `SortedRangeSet` throws "cannot use empty rangeList", reported as ParquetCorruptionException.

ClickHouse writes such indexes for a `Nullable(Tuple)` that is NULL on every row of a page:

```python
import chdb
sql = "SELECT toInt32(number) AS k, CAST(NULL, 'Nullable(Tuple(x Nullable(Int32)))') AS st FROM numbers(2)"
open("/data/t/part-0.parquet", "wb").write(chdb.query(sql, "Parquet").bytes())
```
```sql
CREATE TABLE hive.s.t (k integer, st row(x integer)) WITH (external_location = 'local:///data/t', format = 'PARQUET');
SELECT k FROM hive.s.t WHERE st.x IS NULL;   -- fails, expected 2 rows
```

Works with `hive.parquet_use_column_index = false`, and for the same rows written by pyarrow (which writes null_counts). Trino 484, code unchanged on master. #18985 hit the same exception but was closed without a file. Returning `Domain.onlyNull(type)` when every page is a null page would fix it.
````

### iceberg-rust: comment on apache/iceberg-rust#3315 (closed)

```
Thanks for the fix in #3323. pyiceberg still pins `pyiceberg-core>=0.10.1,<0.11.0` (it has to match the DataFusion FFI ABI), so pyiceberg users still get the old day transform and keep writing pre-1970 rows into the wrong partition. For example, `1899-12-31 23:59:59.5` lands in `day=1900-01-01`, and `ts = TIMESTAMP '1899-12-31 23:59:59.5'` then returns nothing in pyiceberg, Polars and DuckDB. Would a 0.10.x patch release with #3323 be possible? Tables written before the fix also keep the bad partition values, which may be worth a note in the release notes.
```

### Y1: PR to apache/datafusion-python

Change the default in `ParquetWriterOptions.__init__` from `created_by="datafusion-python"`
to `f"datafusion-python version {__version__}"`, and add a test that reads `created_by` back.
PR description, two sentences:

```
The default created_by has no version, so parquet-java's PARQUET-246 check cannot parse it and refuses DELTA_BYTE_ARRAY columns (e.g. Spark with the vectorized reader off). This uses the "<app> version <x>" form the format asks for.
```
