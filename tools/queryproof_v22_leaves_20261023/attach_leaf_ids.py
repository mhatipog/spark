"""Rewrite raw traces so every leaf in every AST event carries its observed data identity.

Usage: python attach_leaf_ids.py IN_DIR OUT_DIR
Each QUERYPROOF_AST line is paired with the QUERYPROOF_V22_LEAVES line emitted
just before it for the same (query, executor, batch, rule, iteration). The leaf
fingerprint is injected into the leaf's JSON node as "queryproof_leaf_fp", so it
becomes part of the printer's descriptor. A leaf without an observed fingerprint
stays without the field, and the V22 printer then refuses it (fail closed).
"""
import base64
import json
import sys
from pathlib import Path


def b64d(x):
    return base64.b64decode(x).decode("utf-8")


def b64e(x):
    return base64.b64encode(x.encode("utf-8")).decode("ascii")


def inject(nodes_json, fps):
    nodes = json.loads(nodes_json)
    for item in [f for f in fps.split(";") if f]:
        idx, fp = item.split("=", 1)
        i = int(idx)
        if 0 <= i < len(nodes) and nodes[i].get("num-children") == 0:
            nodes[i]["queryproof_leaf_fp"] = fp
    return json.dumps(nodes)


def main(src, dst):
    out = Path(dst)
    out.mkdir(parents=True, exist_ok=True)
    stats = {"events": 0, "with_leaf_ids": 0}
    for path in sorted(Path(src).glob("*.stderr")):
        pending = None
        lines = []
        for line in path.read_text(errors="replace").splitlines():
            if line.startswith("QUERYPROOF_V22_LEAVES\t"):
                f = [b64d(x) for x in line.split("\t")[1:]]
                pending = (tuple(f[1:6]), f[0], f[6], f[7])
            elif line.startswith("QUERYPROOF_AST\t"):
                parts = line.split("\t")
                f = [b64d(x) for x in parts[1:]]
                stats["events"] += 1
                if pending and pending[0] == tuple(f[:5]) and pending[1] == "OK":
                    f[5] = inject(f[5], pending[2])
                    f[6] = inject(f[6], pending[3])
                    stats["with_leaf_ids"] += 1
                    line = "\t".join([parts[0]] + [b64e(x) for x in f])
                pending = None
            lines.append(line)
        (out / path.name).write_text("\n".join(lines) + "\n")
        rows = path.with_name(path.name.replace(".stderr", ".rows.json"))
        if rows.exists():
            (out / rows.name).write_text(rows.read_text())
    print("QUERYPROOF_V22_LEAF_IDS", json.dumps(stats))


if __name__ == "__main__":
    main(*sys.argv[1:3])
