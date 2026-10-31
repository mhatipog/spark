"""Which setting makes Parquet VARIANT reads fail with ScalarCastHelper CLASS_NOT_OVERRIDE_EXPECTED_METHOD?

Usage: python variant_probe.py
"""
import shutil
import tempfile

from pyspark.sql import SparkSession

CONFIGS = {
    "default": {},
    "wholestage_off": {"spark.sql.codegen.wholeStage": "false"},
    "factory_no_codegen": {"spark.sql.codegen.factoryMode": "NO_CODEGEN"},
    "both": {"spark.sql.codegen.wholeStage": "false", "spark.sql.codegen.factoryMode": "NO_CODEGEN"},
    "codegen_fallback_only": {"spark.sql.codegen.factoryMode": "FALLBACK"},
}
QUERIES = {
    "try_variant_get_string": "SELECT try_variant_get(v, '$.b', 'string') FROM tv",
    "try_variant_get_int": "SELECT try_variant_get(v, '$.a', 'int') FROM tv",
    "variant_get_int": "SELECT variant_get(v, '$.a', 'int') FROM tv WHERE k = 1",
    "select_variant": "SELECT v FROM tv",
    "filter_on_variant": "SELECT k FROM tv WHERE try_variant_get(v, '$.a', 'int') > 0",
}


def main():
    wh = tempfile.mkdtemp(prefix="qp_var_")
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.warehouse.dir", wh).getOrCreate())
    print("SPARK_VERSION", spark.version)
    spark.sql("""CREATE TABLE tv USING parquet AS SELECT * FROM VALUES
                 (1, parse_json('{"a": 1, "b": "x"}')), (2, parse_json('{"a": -2}')),
                 (3, parse_json('{"b": "y"}')) AS t(k, v)""")
    for cname, conf in CONFIGS.items():
        for k, v in conf.items():
            spark.conf.set(k, v)
        for qname, sql in QUERIES.items():
            try:
                res = "ok " + str([tuple(r) for r in spark.sql(sql).collect()])[:80]
            except Exception as exc:  # noqa: BLE001
                res = "ERROR " + str(exc).split("\n")[0][:150]
            print(f"VARIANT_RESULT {cname:22s} {qname:24s} {res}")
        for k in conf:
            spark.conf.unset(k)
    spark.stop()
    shutil.rmtree(wh, ignore_errors=True)


if __name__ == "__main__":
    main()
