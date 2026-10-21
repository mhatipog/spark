"""Run each cohort query in its own Spark driver with the AST trace overlay.
Usage: python capture_cohort.py COHORT.json OUT_DIR
Writes OUT_DIR/<id>.stderr (raw trace) and OUT_DIR/<id>.rows.json
({"rows": [...]} or {"error": "..."}). One JVM per query, as in V15.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

CHILD = r'''
import json, sys
from pyspark.sql import SparkSession
case = json.loads(sys.argv[1])
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
for name, t in case["tables"].items():
    schema = ", ".join(f"{c['name']} int" for c in t["columns"])
    spark.createDataFrame([tuple(r) for r in t["rows"]], schema).createOrReplaceTempView(name)
try:
    rows = [list(r) for r in spark.sql(case["sql"]).collect()]
    out = {"rows": rows}
except Exception as exc:
    out = {"error": type(exc).__name__ + ": " + str(exc)[:300]}
print("QUERYPROOF_ROWS " + json.dumps(out))
spark.stop()
'''


def main(cohort, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for case in json.loads(Path(cohort).read_text())["cases"]:
        env = dict(os.environ)
        overlay = os.environ.get("QUERYPROOF_OVERLAY")    # local runs; CI patches the jar
        env["PYSPARK_SUBMIT_ARGS"] = (
            (f'--driver-class-path "{overlay}" ' if overlay else "") +
            f'--driver-java-options "-Dqueryproof.ast.enabled=true '
            f'-Dqueryproof.query.id={case["id"]}" pyspark-shell')
        with open(out / f"{case['id']}.stderr", "w", encoding="utf8") as err:
            r = subprocess.run([sys.executable, "-I", "-c", CHILD, json.dumps(case)],
                               stderr=err, stdout=subprocess.PIPE, text=True, env=env)
        line = next((l for l in r.stdout.splitlines() if l.startswith("QUERYPROOF_ROWS ")), None)
        result = json.loads(line[len("QUERYPROOF_ROWS "):]) if line else \
            {"error": f"NO_RESULT exit={r.returncode}"}
        (out / f"{case['id']}.rows.json").write_text(json.dumps(result))
        print(case["id"], "error" if "error" in result else len(result["rows"]))


if __name__ == "__main__":
    main(*sys.argv[1:3])
