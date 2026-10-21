"""Independent SQLite result-bag oracle for the cohort (sanity check only).
Usage: python oracle_sqlite.py COHORT.json ROWS_DIR OUT.json
It compares unordered result bags. It is not a test of the checker: it guards against
a broken data pipeline. `<=>` is translated to SQLite `IS`.
"""
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path


def bag(rows):
    return Counter(tuple(r) for r in rows)


def main(cohort, rows_dir, out):
    doc = json.loads(Path(cohort).read_text())
    report = {"cases": 0, "matched": 0, "mismatched": [], "spark_errors": [], "sqlite_errors": []}
    for case in doc["cases"]:
        report["cases"] += 1
        spark = json.loads((Path(rows_dir) / f"{case['id']}.rows.json").read_text())
        if "error" in spark:
            report["spark_errors"].append({"case": case["id"], "error": spark["error"]})
            continue
        db = sqlite3.connect(":memory:")
        for name, t in case["tables"].items():
            cols = [c["name"] for c in t["columns"]]
            db.execute(f"CREATE TABLE {name} ({', '.join(c + ' INTEGER' for c in cols)})")
            db.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(cols))})", t["rows"])
        try:
            expected = db.execute(case["sql"].replace("<=>", "IS")).fetchall()
        except sqlite3.Error as exc:
            report["sqlite_errors"].append({"case": case["id"], "error": str(exc)})
            continue
        if bag(expected) == bag(spark["rows"]):
            report["matched"] += 1
        else:
            report["mismatched"].append(case["id"])
    Path(out).write_text(json.dumps(report, indent=1))
    print("QUERYPROOF_V17_ORACLE", json.dumps({k: (v if isinstance(v, int) else len(v))
                                              for k, v in report.items()}))


if __name__ == "__main__":
    main(*sys.argv[1:4])
