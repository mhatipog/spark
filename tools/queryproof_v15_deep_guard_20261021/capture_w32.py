"""Capture raw QUERYPROOF_AST trace lines for W32 cases (overlay or patched jar).
Usage: python capture_w32.py CASES.json OVERLAY_DIR_OR_EMPTY OUT_DIR [IDS...] (default: all)
Writes OUT_DIR/<case>.stderr (raw JVM stderr) per case, one JVM per case.
"""
import json, os, subprocess, sys
from pathlib import Path

cases_path, overlay, out_dir, *ids = sys.argv[1:]
out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
child = r'''
import json, sys
from pyspark.sql import SparkSession
case = json.loads(sys.argv[1])
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled","false").getOrCreate()
for name, t in case["tables"].items():
    schema = ", ".join(f"{c['name']} int" for c in t["columns"])
    spark.createDataFrame([tuple(r) for r in t["rows"]], schema).createOrReplaceTempView(name)
rows = spark.sql(case["sql"]).collect()
print("ROWS", len(rows))
spark.stop()
'''
manifest = {c["id"]: c for c in json.loads(Path(cases_path).read_text())["cases"]}
ids = ids or list(manifest)
for cid in ids:
    case = manifest[cid]
    env = dict(os.environ)
    classpath = f'--driver-class-path "{overlay}" ' if overlay else ""
    env["PYSPARK_SUBMIT_ARGS"] = (
        classpath +
        f'--driver-java-options "-Dqueryproof.ast.enabled=true -Dqueryproof.query.id={cid}" '
        "pyspark-shell")
    with open(out / f"{cid}.stderr", "w", encoding="utf8") as err:
        r = subprocess.run([sys.executable, "-I", "-c", child, json.dumps(case)],
                           stderr=err, stdout=subprocess.PIPE, text=True, env=env)
    print(cid, r.returncode, r.stdout.strip())
