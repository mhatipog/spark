"""Real Spark 4.2 ANSI/effect boundary: a proof *exclusion* control.

These are not claimed as equivalent query pairs or Spark optimizer defects.
They show why a proof of defined rowbag semantics cannot automatically be
upgraded to equality of *observable errors* when computed columns are unused.
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
spark.sparkContext.setLogLevel("ERROR")
try:
    direct_division = execute(spark, "SELECT 1 / 0 AS unsafe")
    dead_division = execute(spark,
        "SELECT COUNT(*) FROM (SELECT 1 / 0 AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    direct_error = execute(spark, "SELECT raise_error('round_n_probe') AS unsafe")
    dead_error = execute(spark,
        "SELECT COUNT(*) FROM (SELECT raise_error('round_n_probe') AS unsafe "
        "FROM VALUES (1), (2) AS t(i)) AS z")
    result = {
        "status": "OBSERVABLE_EFFECT_BOUNDARY_DEVELOPMENT",
        "spark_version": spark.version,
        "division_if_observed": direct_division["status"],
        "division_if_not_observed": dead_division,
        "raise_error_if_observed": direct_error["status"],
        "raise_error_if_not_observed": dead_error,
        "interpretation": "Discarding computations can erase observable errors; not a counterexample to a defined-bag liveness lemma",
    }
    print("QUERYPROOF_ROUND_N_ERROR_BOUNDARY", json.dumps(result, sort_keys=True), flush=True)
    assert direct_division["status"] == "EXCEPTION", result
    assert direct_error["status"] == "EXCEPTION", result
    assert dead_division == {"status": "VALUE", "rows": [[2]]}, result
    assert dead_error == {"status": "VALUE", "rows": [[2]]}, result
finally:
    spark.stop()
