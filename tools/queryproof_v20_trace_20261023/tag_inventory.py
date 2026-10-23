"""Static inventory of Catalyst/SQL TreeNodeTag definitions and their read sites.

Usage: python tag_inventory.py SOURCES_DIR [SOURCES_DIR ...] > TAG_INVENTORY.json
Input: unpacked official spark-*_2.13-4.2.0-sources.jar trees (Maven Central).
For each `val X = TreeNodeTag[...]("name")` it records where X is read
(getTagValue/containsTag) and written (setTagValue/unsetTagValue), grouped by
package area. This is evidence for the hand-written classification in
EFFECT_CONTRACT.json, not a proof: reflective or aliased reads are not found.
"""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

DEF = re.compile(r"val\s+(\w+)\s*(?::[^=]+)?=\s*(?:new\s+)?TreeNodeTag\[(?:[^\[\]]|\[[^\[\]]*\])*\]\(\s*\"([^\"]+)\"\s*\)")
AREAS = [
    ("catalyst/analysis", "ANALYZER"),
    ("catalyst/optimizer", "OPTIMIZER"),
    ("catalyst/plans/logical", "LOGICAL_PLAN"),
    ("catalyst/expressions", "EXPRESSIONS"),
    ("catalyst/trees", "TREES"),
    ("execution/adaptive", "AQE"),
    ("execution/columnar", "PHYSICAL"),
    ("execution/joins", "PHYSICAL"),
    ("execution/aggregate", "PHYSICAL"),
    ("execution/exchange", "PHYSICAL"),
    ("sql/execution/SparkStrategies", "PLANNER"),
    ("sql/execution/SparkPlan", "PLANNER"),
    ("sql/execution/QueryExecution", "PLANNER"),
    ("sql/execution", "EXECUTION_OTHER"),
    ("sql/classic", "DATASET_API"),
    ("sql/connect", "CONNECT"),
]


def area(path):
    p = path.replace("\\", "/")
    for needle, name in AREAS:
        if needle in p:
            return name
    return "OTHER"


def main(roots):
    files = [f for r in roots for f in Path(r).rglob("*.scala")]
    texts = {str(f): f.read_text(errors="replace") for f in files}
    defs = {}
    for path, text in texts.items():
        for m in DEF.finditer(text):
            defs.setdefault(m.group(2), []).append({"val": m.group(1), "file": path})
    out = {}
    for name, sites in sorted(defs.items()):
        vals = sorted({s["val"] for s in sites})
        read = defaultdict(set)
        write = defaultdict(set)
        pat_read = re.compile(r"(?:getTagValue|containsTag)\(\s*(?:\w+\.)*(" + "|".join(vals) + r")\b")
        pat_write = re.compile(r"(?:setTagValue|unsetTagValue)\(\s*(?:\w+\.)*(" + "|".join(vals) + r")\b")
        for path, text in texts.items():
            short = path.split("org/apache/spark/")[-1]
            if pat_read.search(text):
                read[area(path)].add(short)
            if pat_write.search(text):
                write[area(path)].add(short)
        out[name] = {"vals": vals,
                     "defined_in": sorted({s["file"].split("org/apache/spark/")[-1] for s in sites}),
                     "read_areas": {a: sorted(v) for a, v in sorted(read.items())},
                     "write_areas": {a: sorted(v) for a, v in sorted(write.items())}}
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main(sys.argv[1:])
