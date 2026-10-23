"""Independent SQLite differential on the endpoints of every certified trace.
Usage: python validate_traces.py TRACE_REPORT.json OUT.json
"""
import json
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_v19_wide_20261022"))
from differential import differs, ILL_SCOPED  # noqa: E402

LABEL = {True: "DIFFERS", False: "AGREES", None: "OUTSIDE", ILL_SCOPED: "ILL_SCOPED"}


def main(report, out):
    r = json.loads(Path(report).read_text())
    rows = [{"case": e["case"], "differential": LABEL[differs(e["first"], e["last"], seed=i)]}
            for i, e in enumerate(r["certified_endpoints"])]
    result = {"certified_endpoints": len(rows),
              "by_class": dict(Counter(x["differential"] for x in rows)),
              "differing": [x["case"] for x in rows if x["differential"] in ("DIFFERS", "ILL_SCOPED")]}
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_V20_ENDPOINTS", json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main(*sys.argv[1:3])
