"""Second-engine hunt: the same generated cases on DuckDB, against SQLite, plus
DuckDB with its optimizer disabled (PRAGMA disable_optimizer) as a metamorphic oracle.

Usage: python hunt_duckdb.py CASES.json OUT.json
Spark-only constructs (ROLLUP, INTERSECT/EXCEPT ALL, LATERAL, LEFT ANTI JOIN) are
still run: DuckDB supports them, and the optimizer-off comparison applies.
"""
import json
import sqlite3
import sys
import time
from collections import Counter

import duckdb

from run_hunt import bag, sqlite_rows


def duck_rows(con, sql):
    try:
        return {"rows": [list(r) for r in con.execute(sql.replace("<=>", "IS NOT DISTINCT FROM")).fetchall()]}
    except duckdb.Error as exc:
        return {"error": type(exc).__name__ + ": " + str(exc).split("\n")[0][:240]}


def load(con, dataset):
    for name, t in dataset.items():
        cols = [c["name"] for c in t["columns"]]
        con.execute(f"CREATE OR REPLACE TABLE {name} ({', '.join(c + ' INTEGER' for c in cols)})")
        if t["rows"]:
            con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(cols))})", t["rows"])


def main(cases_path, out_path):
    doc = json.loads(open(cases_path).read())
    con = duckdb.connect()
    results, t0 = [], time.time()
    for case in doc["cases"]:
        rec = {"id": case["id"], "family": case["family"], "sql": case["sql"], "runs": []}
        for d, dataset in enumerate(case["datasets"]):
            load(con, dataset)
            con.execute("PRAGMA enable_optimizer")
            full = duck_rows(con, case["sql"])
            con.execute("PRAGMA disable_optimizer")
            noopt = duck_rows(con, case["sql"])
            con.execute("PRAGMA enable_optimizer")
            lite = sqlite_rows(dataset, case["sql"])
            run = {"dataset": d, "full": full.get("error", "ok"), "noopt": noopt.get("error", "ok"),
                   "sqlite": lite.get("error", "ok")}
            run["full_vs_noopt"] = bag(full["rows"]) == bag(noopt["rows"]) if "rows" in full and "rows" in noopt else None
            run["full_vs_sqlite"] = bag(full["rows"]) == bag(lite["rows"]) if "rows" in full and "rows" in lite else None
            if False in (run["full_vs_noopt"], run["full_vs_sqlite"]) or ("rows" in full) != ("rows" in noopt):
                run["rows"] = {"full": full, "noopt": noopt, "sqlite": lite}
            rec["runs"].append(run)
        results.append(rec)
    runs = [(r, x) for r in results for x in r["runs"]]
    summary = {"engine": "duckdb " + duckdb.__version__, "sqlite": sqlite3.sqlite_version,
               "queries": len(results), "runs": len(runs), "seconds": round(time.time() - t0, 1),
               "full_vs_noopt_mismatch": sum(1 for _, x in runs if x["full_vs_noopt"] is False),
               "full_vs_sqlite_mismatch": sum(1 for _, x in runs if x["full_vs_sqlite"] is False),
               "error_disagreement": sum(1 for _, x in runs if (x["full"] == "ok") != (x["noopt"] == "ok")),
               "mismatch_by_family": dict(Counter(r["family"] for r, x in runs
                                                  if False in (x["full_vs_noopt"], x["full_vs_sqlite"]))),
               "duck_errors": dict(Counter(x["full"][:80] for _, x in runs if x["full"] != "ok"))}
    with open(out_path, "w", newline="\n") as f:
        json.dump({"summary": summary, "results": results}, f, indent=1)
    print("QUERYPROOF_HUNT_DUCKDB", json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:3])
