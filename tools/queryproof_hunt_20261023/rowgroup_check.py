"""Round 8 check: do the pqsmall/pqv2 copies really hold several row groups per file?

The round 8 run (41280429257) wrote QUERYPROOF_ROWGROUPS to stderr, which the workflow keeps
only on failure, so this rebuilds the copies the same way for the first datasets of the same
generator seed and prints the counts for every table.
"""
import json
import os
import subprocess
import sys
import tempfile

os.environ["QP_VARIANTS"] = "round8"
here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import run_meta  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

out = os.path.join(tempfile.mkdtemp(), "cases.json")
subprocess.run([sys.executable, "-B", os.path.join(here, "gen_feat.py"), "20261051", "60", out, "2"], check=True,
               cwd=here)
cases = json.load(open(out))["cases"]
wh = tempfile.mkdtemp(prefix="qp_rg_")
spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", wh).getOrCreate())
sys.stderr = sys.stdout  # log_row_groups prints to stderr
seen = set()
for case in cases:
    for d, dataset in enumerate(case["datasets"]):
        if (case["group"], d) in seen or len(seen) >= 4:
            continue
        seen.add((case["group"], d))
        run_meta.write_tables(spark, dataset)
        run_meta.write_format_copies(spark, dataset, os.path.join(wh, "_formats"))
        for fmt in ("pqsmall", "pqv2"):
            for name, (loc, _) in run_meta.FORMAT_PATHS[fmt].items():
                run_meta.log_row_groups(spark, fmt, name, loc)
spark.stop()
