"""Real Spark 4.2 effect/optimization-order negative controls.

The first pair deliberately records that compile-time foldable errors can
occur before dead projection pruning. Row-dependent expressions then ask
whether projected-away runtime errors are observable. These are NOT assertions
of SQL equivalence or of an optimizer bug.
"""
import json
from pyspark.sql import SparkSession

def execute(spark, sql):
    try:
        return {"status": "VALUE",
                "rows": [list(row) for row in spark.sql(sql).collect()]}
    except Exception as exc:
        return {"status": "EXCEPTION", "type": type(exc).__name__}

spark = (SparkSession.builder.master("local[2]")
         .appName("QueryProof Round N effect-boundary negative control")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.ansi.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("OFF")
try:
    constant_direct_division = execute(spark, "SELECT 1 / 0 AS unsafe")
    constant_dead_division = execute(spark,
        "SELECT COUNT(*) FROM (SELECT 1 / 0 AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    constant_direct_error = execute(spark, "SELECT raise_error('round_n_probe') AS unsafe")
    constant_dead_error = execute(spark,
        "SELECT COUNT(*) FROM (SELECT raise_error('round_n_probe') AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    dependent_direct_division = execute(spark,
        "SELECT i / (i-i) AS unsafe FROM VALUES (1), (2) AS t(i)")
    dependent_dead_division = execute(spark,
        "SELECT COUNT(*) FROM (SELECT i / (i-i) AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    dependent_direct_error = execute(spark,
        "SELECT raise_error(CAST(i AS STRING)) AS unsafe "
        "FROM VALUES (1), (2) AS t(i)")
    dependent_dead_error = execute(spark,
        "SELECT COUNT(*) FROM (SELECT raise_error(CAST(i AS STRING)) AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    result = {
        "status": "OBSERVABLE_EFFECT_BOUNDARY_DEVELOPMENT",
        "spark_version": spark.version,
        "constant_1_division": [constant_direct_division["status"],
                                constant_dead_division["status"]],
        "constant_raise_error": [constant_direct_error["status"],
                                 constant_dead_error["status"]],
        "row_dependent_division": [dependent_direct_division["status"],
                                   dependent_dead_division],
        "row_dependent_raise_error": [dependent_direct_error["status"],
                                      dependent_dead_error],
        "interpretation": "Constant-foldable errors can precede pruning; row-dependent discarded computations can change error observation",
    }
    print("QUERYPROOF_ROUND_N_ERROR_BOUNDARY", json.dumps(result, sort_keys=True), flush=True)
    assert constant_direct_division["status"] == "EXCEPTION"
    assert constant_dead_division["status"] == "EXCEPTION"
    assert constant_direct_error["status"] == "EXCEPTION"
    assert constant_dead_error["status"] == "EXCEPTION"
    assert dependent_direct_division["status"] == "EXCEPTION"
    assert dependent_direct_error["status"] == "EXCEPTION"
    assert dependent_dead_division == {"status": "VALUE", "rows": [[2]]}, result
    assert dependent_dead_error == {"status": "VALUE", "rows": [[2]]}, result
finally:
    spark.stop()
