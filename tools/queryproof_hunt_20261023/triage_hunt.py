"""Triage hunt shards into bug candidates.

Usage: python triage_hunt.py OUT.json HUNT_OUT.json... [--events EVENTS_REPORT.json KERNEL.v]
Candidates:
  RESULT     FULL differs from MINIMAL, NOCODEGEN or SQLite on some dataset
  ERROR      the Spark configurations disagree on success (a crash under a
             supported configuration)
  REWRITE    (with --events) an uncertified optimizer rewrite whose OLD and NEW plans
             differ on some random database in the independent SQLite differential
             (200 trials), with leaves and opaque operators unchanged, so the
             difference cannot come from the opaque interpretation
Everything else is counted, not reported.
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_v22_leaves_20261023"))
import differential as D  # noqa: E402

SPARK_CONFIGS = ("full", "minimal", "nocodegen")


def opaque_signature(node, acc):
    if node[0] == "op":
        acc.append((node[1], tuple(node[2]), tuple(node[3])))
        opaque_signature(node[4], acc)
    elif node[0] in ("filter", "project"):
        opaque_signature(node[2], acc)
    elif node[0] == "join":
        opaque_signature(node[3], acc)
        opaque_signature(node[4], acc)
    return acc


def main(out, hunts, events=None, kernel=None):
    shards = [json.loads(Path(p).read_text()) for p in hunts]
    results = [r for h in shards for r in h["results"]]
    sql = {r["id"]: r["sql"] for r in results}
    candidates, counts = [], Counter()
    for r in results:
        for x in r["runs"]:
            if False in (x["full_vs_minimal"], x.get("full_vs_nocodegen"), x["full_vs_sqlite"]):
                candidates.append({"kind": "RESULT", "case": r["id"], "family": r["family"],
                                   "dataset": x["dataset"], "sql": r["sql"],
                                   **{k: x.get(k) for k in ("full_vs_minimal", "full_vs_nocodegen",
                                                            "full_vs_sqlite")},
                                   "rows": x.get("rows")})
            status = {k: x[k] for k in SPARK_CONFIGS if k in x}
            if len({v == "ok" for v in status.values()}) > 1:
                candidates.append({"kind": "ERROR", "case": r["id"], "family": r["family"],
                                   "dataset": x["dataset"], "sql": r["sql"], **status})
    ev = json.loads(Path(events).read_text()) if events else {"event_index": []}
    terms = dict(re.findall(r"Definition (ev_[on]\d+) := (.*?)\.\n", Path(kernel).read_text())) if kernel else {}
    for i, m in enumerate(ev["event_index"]):
        counts["events"] += 1
        if m["accepted"]:
            counts["certified"] += 1
            continue
        o, n = terms[f"ev_o{i}"], terms[f"ev_n{i}"]
        try:
            po, pn = D.parse(o), D.parse(n)
        except (D.Outside, IndexError):
            counts["unparsed"] += 1
            continue
        if sorted(opaque_signature(po, [])) != sorted(opaque_signature(pn, [])):
            counts["opaque_changed"] += 1
            continue
        verdict = D.differs(o, n, trials=200, seed=i)
        counts[f"uncertified_{verdict}"] += 1
        if verdict is True:
            candidates.append({"kind": "REWRITE", "case": m["case"], "rule": m["rule"],
                               "sql": sql.get(m["case"]), "old": o, "new": n})
    totals = Counter()
    for h in shards:
        for k, v in h["summary"].items():
            if isinstance(v, (int, float)):
                totals[k] += v
    errors = Counter()
    for c in candidates:
        if c["kind"] == "ERROR":
            errors["; ".join(f"{k}={c[k][:100]}" for k in SPARK_CONFIGS if c.get(k, "ok") != "ok")] += 1
    result = {"summary": dict(totals), "event_counts": dict(counts),
              "candidates_by_kind": dict(Counter(c["kind"] for c in candidates)),
              "candidates_by_family": {f"{k}:{f}": v for (k, f), v in Counter(
                  (c["kind"], c.get("family")) for c in candidates).items()},
              "error_messages": dict(errors.most_common()),
              "candidates": candidates}
    Path(out).write_text(json.dumps(result, indent=1))
    print("QUERYPROOF_HUNT_TRIAGE", json.dumps({k: result[k] for k in (
        "summary", "event_counts", "candidates_by_kind", "candidates_by_family",
        "error_messages")}, sort_keys=True))


if __name__ == "__main__":
    args = sys.argv[1:]
    ev = kern = None
    if "--events" in args:
        i = args.index("--events")
        ev, kern = args[i + 1], args[i + 2]
        args = args[:i] + args[i + 3:]
    main(args[0], args[1:], ev, kern)
