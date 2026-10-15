"""Run the real, unmodified Spark regression queries with instrumented RuleExecutor.
Do not treat passing GitHub Actions as proof of correctness: inspect observations.
"""
import importlib, os, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import queryproof_spark_probe as probe

original_run=probe.run
def instrumented_run(spark,query):
    case_label = "unknown"
    for name,c in probe.cases.items():
        if query == c["sql"]:
            case_label=name
            break
        if query == c["control"]:
            case_label=name+":control"
            break
    spark._jvm.java.lang.System.setProperty("queryproof.ast.enabled","true")
    spark._jvm.java.lang.System.setProperty("queryproof.query.id",case_label)
    return original_run(spark,query)
probe.run = instrumented_run
if __name__=="__main__":
    sys.exit(probe.main())
