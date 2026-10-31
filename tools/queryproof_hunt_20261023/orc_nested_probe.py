"""ORC nested-field predicate pushdown with NULL structs (round 7 lead).

Usage: python orc_nested_probe.py
Writes the same rows as ORC and as Parquet, plain and partitioned, and filters on
st.x IS NULL / IS NOT NULL / = value with ORC filter pushdown on and off, V1 and V2.
A row whose whole struct is NULL has st.x NULL and must pass `st.x IS NULL`.
"""
import json
import shutil
import tempfile

from pyspark.sql import SparkSession

ROWS = [(1, None), (1, (None, "a")), (2, (1, "b")), (2, None), (3, (2, None)), (3, (None, None))]
FILTERS = ["st.x IS NULL", "st.x IS NOT NULL", "st.x = 1", "st.y IS NULL", "st IS NULL",
           "st.x IS NULL AND k = 1", "coalesce(st.x, -1) = -1"]
CONFIGS = {
    "orc_v1": {},
    "orc_v1_no_pushdown": {"spark.sql.orc.filterPushdown": "false"},
    "orc_v1_no_nested_pushdown": {"spark.sql.optimizer.nestedPredicatePushdownSupportedFileSources": ""},
    "orc_v1_nested_parquet_only": {"spark.sql.optimizer.nestedPredicatePushdownSupportedFileSources": "parquet"},
    "orc_v1_no_vectorized": {"spark.sql.orc.enableVectorizedReader": "false"},
    "orc_v2": {"spark.sql.sources.useV1SourceList": ""},
    "orc_v2_no_pushdown": {"spark.sql.sources.useV1SourceList": "", "spark.sql.orc.filterPushdown": "false"},
}


def rows(df):
    """Rows keyed by column name (a partitioned read puts the partition column last)."""
    return sorted(json.dumps(r.asDict(recursive=True), sort_keys=True) for r in df.collect())


def print_orc_stats(spark, path):
    """File-level column statistics as the ORC writer recorded them (hasNull per column)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    for st in jvm.org.apache.hadoop.fs.Path(path).getFileSystem(conf).listStatus(jvm.org.apache.hadoop.fs.Path(path)):
        if st.getPath().getName().endswith(".orc"):
            reader = jvm.org.apache.orc.OrcFile.createReader(st.getPath(), jvm.org.apache.orc.OrcFile.readerOptions(conf))
            print("ORCSCHEMA", reader.getSchema().toString())
            for i, cs in enumerate(reader.getStatistics()):
                print(f"ORCSTATS column {i}: " + cs.toString().replace("\n", " "))


def main():
    base = tempfile.mkdtemp(prefix="qp_orcn_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate())
    print("SPARK_VERSION", spark.version)
    df = spark.createDataFrame(ROWS, "k INT, st STRUCT<x: INT, y: STRING>")
    full = df
    for layout in ("plain", "partitioned", "one_file_k2"):
        # one_file_k2: a single file whose only NULL st.x comes from a NULL parent struct.
        df = full.where("k = 2") if layout == "one_file_k2" else full
        for fmt in ("orc", "parquet"):
            w = df.coalesce(1).write.format(fmt).mode("overwrite")
            (w.partitionBy("k") if layout == "partitioned" else w).save(f"{base}/{layout}_{fmt}")
        if layout == "one_file_k2":
            print_orc_stats(spark, f"{base}/{layout}_orc")
            for cname in ("orc_v1", "orc_v1_no_nested_pushdown", "orc_v2"):
                for k, v in CONFIGS[cname].items():
                    spark.conf.set(k, v)
                plan = spark.read.schema(df.schema).orc(f"{base}/{layout}_orc").where("st.x IS NULL") \
                    ._jdf.queryExecution().executedPlan().toString()
                for line in plan.split("\n"):
                    if "Scan" in line or "Filter" in line:
                        print(f"ORCPLAN {cname}: {line.strip()[:400]}")
                for k in CONFIGS[cname]:
                    spark.conf.unset(k)
        for f in FILTERS:
            expected = rows(df.where(f))
            for cname, conf in CONFIGS.items():
                for k, v in conf.items():
                    spark.conf.set(k, v)
                try:
                    got = rows(spark.read.schema(df.schema).orc(f"{base}/{layout}_orc").where(f))
                    res = "ok" if got == expected else f"WRONG got {len(got)} rows, expected {len(expected)}"
                except Exception as exc:  # noqa: BLE001
                    res = "ERROR " + str(exc).split("\n")[0][:120]
                print(f"ORCNESTED {layout:11s} {f:26s} {cname:26s} {res}")
                for k in conf:
                    spark.conf.unset(k)
            pq = rows(spark.read.schema(df.schema).parquet(f"{base}/{layout}_parquet").where(f))
            print(f"ORCNESTED {layout:11s} {f:26s} {'parquet_default':26s} {'ok' if pq == expected else 'WRONG'}")
    spark.stop()
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
