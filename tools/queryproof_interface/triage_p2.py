"""Phase-two triage: apply known_issues.json to every finding and audit violation, then cluster the
rest by (engine or writer, kind or claim, shape, writer, normalised error) with one representative.

Usage: python triage_p2.py ARTIFACT_DIR KNOWN_ISSUES.json
"""
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict

art, known = sys.argv[1], json.load(open(sys.argv[2], encoding="utf-8"))


def errs(f):
    return " ".join(r.get("error", "") for r in f["results"].values())


def _close(a, b):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_close(x, y) for x, y in zip(a, b))
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        return fa == fb or (fa != fa and fb != fb) or abs(fa - fb) <= 1e-6 + 1e-5 * max(abs(fa), abs(fb))
    return a == b


def float_only(results):
    """Every mode returned rows, and they differ only in floating-point rounding."""
    rows = [r["rows"] for r in results.values()]
    if not rows or any(r is None for r in rows):
        return False
    key = lambda r: json.dumps(r, default=str)  # noqa: E731
    first = sorted(rows[0], key=key)
    return all(_close(first, sorted(x, key=key)) for x in rows[1:])


def match(sig, rec):
    for k, pat in sig.items():
        if k in ("id", "class"):
            continue
        if k == "float_only":
            if float_only(rec.get("results", {})) != pat:
                return False
            continue
        if k == "results_mode":
            if pat not in rec.get("modes", ()):
                return False
            continue
        if not re.search(pat, str(rec.get(k, ""))):
            return False
    return True


classes, unknown, reps, by_gen = Counter(), Counter(), {}, defaultdict(Counter)
checks, cpu, audit_cpu = Counter(), Counter(), Counter()
for path in sorted(glob.glob(os.path.join(art, "**", "iface_*.json"), recursive=True)):
    gen = re.sub(r"_\d+$", "", os.path.basename(path)[6:-5])
    d = json.load(open(path))
    checks[gen] += d["summary"].get("checks", 0)
    for k, v in d["summary"].get("cpu_seconds", {}).items():
        cpu[gen] += v
    for f in d["findings"]:
        for kind, mode in f["kinds"]:
            rec = {"engine": f["engine"], "kind": kind, "shape": f["shape"], "writer": f["writer_set"],
                   "format": f["format"], "qid": f["qid"], "sql": f["sql"], "error": errs(f), "modes": list(f["results"]),
                   "results": {m: {"rows": r.get("rows")} for m, r in f["results"].items()}}
            sig = next((s for s in known if match(s, rec)), None)
            if sig:
                classes[(sig["class"], sig["id"])] += 1
                by_gen[gen][(sig["class"], sig["id"])] += 1
            else:
                key = (f["engine"], kind, f["shape"], f["writer_set"], re.sub(r"\d+", "N", rec["error"])[:90])
                unknown[(gen,) + key] += 1
                reps.setdefault((gen,) + key, f)
for path in sorted(glob.glob(os.path.join(art, "**", "audit_*.json"), recursive=True)):
    gen = re.sub(r"_\d+$", "", os.path.basename(path)[6:-5])
    d = json.load(open(path))
    audit_cpu[gen] += d.get("cpu_seconds", 0)
    for v in d["violations"]:
        rec = {"writer": v["writer_set"], "claim": v["claim"], "shape": v["shape"], "error": v["detail"]}
        sig = next((s for s in known if match(s, rec)), None)
        if sig:
            classes[(sig["class"], sig["id"])] += 1
            by_gen[gen][(sig["class"], sig["id"])] += 1
        else:
            key = ("AUDIT:" + v["writer_set"], v["claim"], v["shape"], v.get("col", ""), re.sub(r"\d+", "N", v["detail"])[:90])
            unknown[(gen,) + key] += 1
            reps.setdefault((gen,) + key, v)
coarse, cshapes, crep = Counter(), defaultdict(set), {}
for key, n in unknown.items():
    gen, eng, kind, shp, wr, err = key[:6]
    ck = (eng.split("-")[0], kind, wr.split("_")[0], err[:70])
    coarse[ck] += n; cshapes[ck].add(shp); crep.setdefault(ck, key)
print("== coarse unclassified groups:", len(coarse))
for ck, n in coarse.most_common():
    r = reps[crep[ck]]
    extra = ascii(r["sql"][-100:]) + " | exp " + ascii(r["expected"])[:60] + " " + ascii({m: ascii(x)[:70] for m, x in list(r["results"].items())[:3]}) if "results" in r else ascii(r["detail"])[:140]
    print(n, ascii(ck)[:200], len(cshapes[ck]), ascii(sorted(cshapes[ck])[:4])[:120], "|", extra)
