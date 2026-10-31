"""Minimize the V2 file source INTERNAL_ERROR "Invalid call to exprId on unresolved object".

Usage: python v2_probe.py
Writes Parquet files for tv(k, v VARIANT) and tb(a, b, c), reads them by path through the
V1 and the V2 file source (spark.sql.sources.useV1SourceList=''), and runs increasingly
large pieces of hunt case F038_00028.
"""
import shutil
import tempfile

from pyspark.sql import SparkSession

SUB = ("SELECT d3.k AS f0, CASE WHEN is_variant_null(d3.v) THEN 1 ELSE 0 END AS f1, "
       "try_variant_get(d3.v, '$.a', 'int') AS f2, try_variant_get(d3.v, '$.b', 'string') AS f3 FROM tv d3")
QUERIES = {
    "get_int": "SELECT try_variant_get(v, '$.a', 'int') FROM tv",
    "is_null": "SELECT k, CASE WHEN is_variant_null(v) THEN 1 ELSE 0 END FROM tv",
    "sub_projection": f"SELECT * FROM ({SUB}) d4",
    "sub_filter": f"SELECT * FROM ({SUB}) d4 WHERE d4.f1 = 0",
    "not_exists_corr": f"SELECT d2.a FROM tb d2 WHERE NOT EXISTS (SELECT 1 FROM ({SUB}) d4 WHERE d4.f1 = d2.c)",
    "exists_corr": f"SELECT d2.a FROM tb d2 WHERE EXISTS (SELECT 1 FROM ({SUB}) d4 WHERE d4.f1 = d2.c)",
    "in_sub": f"SELECT d2.a FROM tb d2 WHERE d2.c IN (SELECT d4.f1 FROM ({SUB}) d4)",
    "join": f"SELECT d2.a, d4.f2 FROM tb d2 JOIN ({SUB}) d4 ON d4.f1 = d2.c",
    "not_exists_one_get": ("SELECT d2.a FROM tb d2 WHERE NOT EXISTS (SELECT 1 FROM (SELECT d3.k AS f0, "
                           "try_variant_get(d3.v, '$.a', 'int') AS f2 FROM tv d3) d4 WHERE d4.f2 = d2.c)"),
    "not_exists_is_null_only": ("SELECT d2.a FROM tb d2 WHERE NOT EXISTS (SELECT 1 FROM (SELECT "
                                "CASE WHEN is_variant_null(d3.v) THEN 1 ELSE 0 END AS f1 FROM tv d3) d4 "
                                "WHERE d4.f1 = d2.c)"),
    "not_exists_unused_get": ("SELECT d2.a FROM tb d2 WHERE NOT EXISTS (SELECT 1 FROM (SELECT d3.k AS f0, "
                              "try_variant_get(d3.v, '$.a', 'int') AS f2 FROM tv d3) d4 WHERE d4.f0 = d2.c)"),
}


def main():
    wh = tempfile.mkdtemp(prefix="qp_v2_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    print("SPARK_VERSION", spark.version)
    tv_path, tb_path = f"{wh}/tv_files", f"{wh}/tb_files"
    spark.sql("""SELECT * FROM VALUES (1, parse_json('{"a": 1, "b": "x"}')), (2, parse_json('{"a": -2}')),
                 (0, parse_json('null')), (3, parse_json('{"b": "y"}')) AS t(k, v)""").write.parquet(tv_path)
    spark.sql("SELECT * FROM VALUES (1, 2, 0), (2, 3, 1), (3, 4, NULL), (NULL, 1, 5) AS t(a, b, c)") \
        .write.parquet(tb_path)
    for label, v1list in (("V1", None), ("V2", "")):
        if v1list is None:
            spark.conf.unset("spark.sql.sources.useV1SourceList")
        else:
            spark.conf.set("spark.sql.sources.useV1SourceList", v1list)
        spark.read.parquet(tv_path).createOrReplaceTempView("tv")
        spark.read.parquet(tb_path).createOrReplaceTempView("tb")
        for name, sql in QUERIES.items():
            try:
                res = "ok " + str(sorted([tuple(r) for r in spark.sql(sql).collect()], key=str))[:70]
            except Exception as exc:  # noqa: BLE001
                res = "ERROR " + str(exc).split("\n")[0][:150]
            print(f"V2PROBE {label} {name:24s} {res}")
    spark.conf.set("spark.sql.sources.useV1SourceList", "")
    spark.read.parquet(tv_path).createOrReplaceTempView("tv")
    spark.read.parquet(tb_path).createOrReplaceTempView("tb")
    spark.conf.set("spark.sql.pyspark.jvmStacktrace.enabled", "true")
    try:
        spark.sql(QUERIES["not_exists_corr"]).collect()
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        i = msg.find("JVM stacktrace")
        print("V2PROBE_TRACE", msg[i:i + 4000] if i >= 0 else msg[:2000])
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)


if __name__ == "__main__":
    main()
