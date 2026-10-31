"""ORC nested-field predicate pushdown with NULL structs (round 7 lead).

Usage: python orc_nested_probe.py
Writes the same rows as ORC and as Parquet, plain and partitioned, and filters on
st.x IS NULL / IS NOT NULL / = value with ORC filter pushdown on and off, V1 and V2.
A row whose whole struct is NULL has st.x NULL and must pass `st.x IS NULL`.
"""
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
    "orc_v1_no_vectorized": {"spark.sql.orc.enableVectorizedReader": "false"},
    "orc_v2": {"spark.sql.sources.useV1SourceList": ""},
    "orc_v2_no_pushdown": {"spark.sql.sources.useV1SourceList": "", "spark.sql.orc.filterPushdown": "false"},
}


def main():
    base = tempfile.mkdtemp(prefix="qp_orcn_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate())
    print("SPARK_VERSION", spark.version)
    df = spark.createDataFrame(ROWS, "k INT, st STRUCT<x: INT, y: STRING>")
    for layout in ("plain", "partitioned"):
        for fmt in ("orc", "parquet"):
            w = df.coalesce(1).write.format(fmt).mode("overwrite")
            (w.partitionBy("k") if layout == "partitioned" else w).save(f"{base}/{layout}_{fmt}")
        for f in FILTERS:
            expected = sorted(str(r) for r in df.where(f).collect())
            for cname, conf in CONFIGS.items():
                for k, v in conf.items():
                    spark.conf.set(k, v)
                try:
                    got = sorted(str(r) for r in spark.read.schema(df.schema).orc(f"{base}/{layout}_orc").where(f).collect())
                    res = "ok" if got == expected else f"WRONG got {len(got)} rows, expected {len(expected)}"
                except Exception as exc:  # noqa: BLE001
                    res = "ERROR " + str(exc).split("\n")[0][:120]
                print(f"ORCNESTED {layout:11s} {f:26s} {cname:26s} {res}")
                for k in conf:
                    spark.conf.unset(k)
            pq = sorted(str(r) for r in spark.read.schema(df.schema).parquet(f"{base}/{layout}_parquet").where(f).collect())
            print(f"ORCNESTED {layout:11s} {f:26s} {'parquet_default':26s} {'ok' if pq == expected else 'WRONG'}")
    spark.stop()
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
