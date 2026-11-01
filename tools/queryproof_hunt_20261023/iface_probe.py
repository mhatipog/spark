"""Interface hunt leads (run 41283678653), minimal and Spark-written:

G  DATE literals inside the Julian/Gregorian gap (1582-10-05 .. 1582-10-14) in pushed filters
S  ORC string statistics with characters above U+FFFF (UTF-8 vs UTF-16 order)
Each query runs with filter pushdown on and off; a difference is printed as WRONG with the
pushed filters. Prints IFACEPROBE lines and an IFACEPROBE_SUMMARY line.
"""
import json
import os
import sys
import tempfile

from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
         # 3.5 refuses to write dates before 1582-10-15 without a rebase mode
         .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PYSPARK", spark.version)
base = tempfile.mkdtemp(prefix="qp_ifaceprobe_")
wrong = 0


def write_rows(fmt, name, rows, ddl):
    """One file per row, so statistics are per value."""
    loc = os.path.join(base, f"{name}_{fmt}")
    for r in rows:
        spark.createDataFrame([r], ddl).coalesce(1).write.mode("append").format(fmt).save(loc)
    return loc


def check(tag, fmt, loc, ddl, sql, extra=None):
    global wrong
    out = {}
    for mode, conf in (("off", {f"spark.sql.{fmt}.filterPushdown": "false"}), ("on", {})):
        conf = {**conf, **(extra or {})}
        for k, v in conf.items():
            spark.conf.set(k, v)
        spark.read.format(fmt).schema(ddl).load(loc).createOrReplaceTempView("t")
        q = spark.sql(sql)
        rows = sorted(json.dumps([str(x) for x in r]) for r in q.collect())
        plan = q._jdf.queryExecution().executedPlan().toString()
        pushed = [p.split("]")[0] + "]" for p in plan.split("PushedFilters: ")[1:]]
        out[mode] = (rows, pushed)
        for k in conf:
            spark.conf.unset(k)
    bad = out["on"][0] != out["off"][0]
    wrong += bad
    print("IFACEPROBE", tag, fmt, "WRONG" if bad else "same", json.dumps(sql, ensure_ascii=True),
          "off", out["off"][0], "on", out["on"][0], "pushed", json.dumps(out["on"][1], ensure_ascii=True))


# G: dates around the gap; the stored values are valid proleptic Gregorian dates
dates = [(1, "1582-10-04"), (2, "1582-10-15"), (3, "1582-10-16"), (4, "1600-01-01")]
date_rows = [(k, __import__("datetime").date.fromisoformat(d)) for k, d in dates]
for fmt in ("parquet", "orc"):
    loc = write_rows(fmt, "dates", date_rows, "k INT, d DATE")
    for lit in ("1582-10-05", "1582-10-10", "1582-10-14", "1582-10-15"):
        for op in (">", ">=", "<", "=", "<>"):
            check("G", fmt, loc, "k INT, d DATE", f"SELECT k FROM t WHERE d {op} DATE '{lit}'")
    check("G_java8", fmt, loc, "k INT, d DATE", "SELECT k FROM t WHERE d > DATE '1582-10-14'",
          {"spark.sql.datetime.java8API.enabled": "true"})

# GT (pair two, run 41302131902): TIMESTAMP literals inside the gap, against Parquet files that
# were not written by Spark (pyarrow, microseconds, UTC), so no rebase metadata is involved
try:
    import datetime as _dt

    import pyarrow as pa
    import pyarrow.parquet as pq
    U = _dt.timezone.utc
    ts_rows = [(1, _dt.datetime(1582, 10, 4, 12, tzinfo=U)), (2, _dt.datetime(1582, 10, 10, 12, tzinfo=U)),
               (3, _dt.datetime(1582, 10, 15, tzinfo=U)), (4, _dt.datetime(1582, 10, 16, tzinfo=U))]
    loc = os.path.join(base, "ts_pyarrow")
    os.makedirs(loc)
    for k, v in ts_rows:
        pq.write_table(pa.table({"k": pa.array([k], pa.int32()), "t": pa.array([v], pa.timestamp("us", tz="UTC"))}),
                       os.path.join(loc, f"part-{k}.parquet"))
    for lit in ("1582-10-05 00:00:00", "1582-10-10 12:00:00", "1582-10-14 23:59:59.999999", "1582-10-15 00:00:00"):
        for op in (">", ">=", "<", "=", "<>"):
            check("GT", "parquet", loc, "k INT, t TIMESTAMP", f"SELECT k FROM t WHERE t {op} TIMESTAMP '{lit}'")
    for tag, extra in (("GT_java8", {"spark.sql.datetime.java8API.enabled": "true"}),
                       ("GT_v2", {"spark.sql.sources.useV1SourceList": ""})):
        check(tag, "parquet", loc, "k INT, t TIMESTAMP", "SELECT k FROM t WHERE t = TIMESTAMP '1582-10-10 12:00:00'",
              extra)
    # Spark-written TIMESTAMP_MICROS with the proleptic calendar (the 4.x write default)
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    loc = write_rows("parquet", "ts_spark", [(k, v) for k, v in ts_rows], "k INT, t TIMESTAMP")
    spark.conf.unset("spark.sql.parquet.outputTimestampType")
    check("GT_spark_written", "parquet", loc, "k INT, t TIMESTAMP",
          "SELECT k FROM t WHERE t = TIMESTAMP '1582-10-10 12:00:00'")
except ImportError as exc:
    print("IFACEPROBE GT skipped", exc)

# S: strings above U+FFFF against U+E000..U+FFFF
strs = [(1, "\U0001f600"), (2, ""), (3, "￿"), (4, "\U00010000"), (5, "z")]
for fmt in ("orc", "parquet"):
    loc = write_rows(fmt, "strs", strs, "k INT, s STRING")
    for lit in ("", "￿", "\U0001f600", "\U00010000"):
        for op in (">", ">=", "<", "<=", "="):
            check("S", fmt, loc, "k INT, s STRING", f"SELECT k FROM t WHERE s {op} '{lit}'")
    if fmt == "orc":  # the statistics ORC wrote and how its Java API orders them
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(loc)
        it = path.getFileSystem(conf).listFiles(path, False)
        while it.hasNext():
            p = it.next().getPath()
            if p.getName().endswith(".orc"):
                r = jvm.org.apache.orc.OrcFile.createReader(p, jvm.org.apache.orc.OrcFile.readerOptions(conf))
                st = r.getStatistics()[2]
                print("IFACEPROBE_ORCSTATS", json.dumps([st.getMinimum(), st.getMaximum()], ensure_ascii=True),
                      "writer", r.getSoftwareVersion())
print("IFACEPROBE_SUMMARY", spark.version, "differences", wrong)
spark.stop()
sys.exit(0)
