"""V15: kernel-decided deep guard certificates on real Catalyst events.

Usage: python run_v15.py TRACE [TRACE ...] --report OUT.json [--keep DIR]

For every effective QUERYPROOF_AST event (any rule) the printer emits OLD/NEW
terms; events outside the grammar are UNKNOWN. The Coq kernel then DECIDES
`check old new` (pass 1, vm_compute) and every verdict is re-asserted as a
kernel-checked Example (pass 2); accepted events also get an instance of
`check_sound`. JSON-level adversarial mutants of each accepted
InferFiltersFromConstraints event must all be rejected.
Python never decides acceptance.
"""
import argparse
import copy
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PROOFS = ROOT / "research/queryproof/proofs"
sys.path.insert(0, str(ROOT / "tools/queryproof_instrumented"))
from rank_ast import decode  # noqa: E402
from print_terms import Printer, Unsupported, CAT, ATTR, tree  # noqa: E402

COQC = os.environ.get("COQC", "coqc")
IFFC = "InferFiltersFromConstraints"
ISNOTNULL = CAT + "expressions.IsNotNull"


def events(paths):
    for path in paths:
        for line in Path(path).read_text(errors="replace").splitlines():
            if line.startswith("QUERYPROOF_AST\t"):
                e = decode(line)
                if e["old"] != e["new"]:
                    yield e


# ---- JSON-level adversarial mutants (applied to accepted IFFC events) ----

def _join(nodes):
    return next(n for n in nodes if n["class"].endswith("plans.logical.Join"))


def _guards(nodes):
    return [n for n in nodes if n["class"].endswith("plans.logical.Filter")
            and n["condition"][0]["class"] == ISNOTNULL]


def m_wrong_attribute(old, new):
    attrs = [a[0] for n in new if n["class"].endswith("LogicalRDD") for a in n["output"]]
    g = _guards(new)
    if not g:
        return None
    target = g[0]["condition"][1]
    other = next((a for a in attrs if a["exprId"] != target["exprId"]
                  and a["dataType"] == target["dataType"]), None)
    if other is None:
        return None
    g[0]["condition"][1] = copy.deepcopy(other)
    return old, new


def m_null_safe(old, new):
    for plan in (old, new):
        c = _join(plan)["condition"]
        if not c or c[0]["class"] not in (CAT + "expressions.LessThan",
                                          CAT + "expressions.EqualTo"):
            return None
        c[0]["class"] = CAT + "expressions.EqualNullSafe"
    return old, new


def m_left_outer(old, new):
    if _join(old)["joinType"]["object"] != CAT + "plans.Inner$":
        return None
    for plan in (old, new):
        _join(plan)["joinType"]["object"] = CAT + "plans.LeftOuter$"
    return old, new


def m_swap_guards(old, new):
    g = _guards(new)
    if len(g) != 2:
        return None
    g[0]["condition"], g[1]["condition"] = g[1]["condition"], g[0]["condition"]
    return old, new


def m_or_condition(old, new):
    for plan in (old, new):
        j = _join(plan)
        if not j["condition"]:
            return None
        c = j["condition"]
        j["condition"] = [{"class": CAT + "expressions.Or", "num-children": 2,
                           "left": 0, "right": 1}] + copy.deepcopy(c) + copy.deepcopy(c)
    return old, new


def m_frame_edit(old, new):
    new[0]["queryproof_mutation"] = 1
    return old, new


MUTANTS = {"wrong_guard_attribute": m_wrong_attribute,
           "null_safe_join": m_null_safe,
           "left_outer_preserved_guard": m_left_outer,
           "swapped_guard_sides": m_swap_guards,
           "or_join_condition": m_or_condition,
           "frame_edit": m_frame_edit}


# ---- Coq ----

def coq(source, workdir, name):
    path = Path(workdir) / f"{name}.v"
    path.write_text(source)
    r = subprocess.run([COQC, "-q", "-Q", str(workdir), "QP", str(path)],
                       capture_output=True, text=True, cwd=workdir)
    if r.returncode != 0:
        raise RuntimeError(f"COQ_FAILED {name}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("traces", nargs="+")
    ap.add_argument("--report", required=True)
    ap.add_argument("--keep")
    args = ap.parse_args()

    items, unknown = [], []
    for e in events(args.traces):
        meta = {"case": e["case"], "rule": e["rule"].split(".")[-1],
                "batch": e["batch"], "iteration": e["iteration"]}
        try:
            o, n = Printer_pair(e["old"], e["new"])
        except Unsupported as exc:
            unknown.append({**meta, "reason": str(exc)})
            continue
        items.append({**meta, "kind": "event", "old": o, "new": n,
                      "raw": (e["old"], e["new"])})

    with tempfile.TemporaryDirectory(prefix="qp-v15-") as tmp:
        work = Path(args.keep) if args.keep else Path(tmp)
        work.mkdir(parents=True, exist_ok=True)
        (work / "QueryProofDeepGuard.v").write_text(
            (PROOFS / "QueryProofDeepGuard.v").read_text())
        coq_out = coq((work / "QueryProofDeepGuard.v").read_text(), work, "QueryProofDeepGuard")
        assert "Closed under the global context" in coq_out, "AXIOMS_IN_CHECK_SOUND"

        def decide(batch, name):
            body = ["From QP Require Import QueryProofDeepGuard.",
                    "From Coq Require Import ZArith List. Import ListNotations.", ""]
            for i, it in enumerate(batch):
                body.append(f"Definition o{i} := {it['old']}.")
                body.append(f"Definition n{i} := {it['new']}.")
                body.append(f"Eval vm_compute in check o{i} n{i}.")
            out = coq("\n".join(body) + "\n", work, name)
            verdicts = re.findall(r"=\s*(true|false)\s*\n\s*:\s*bool", out)
            assert len(verdicts) == len(batch), "VERDICT_COUNT_MISMATCH"
            return [v == "true" for v in verdicts]

        for it, v in zip(items, decide(items, "V15Decide")):
            it["accepted"] = v

        mutants, mutant_unknown = [], []
        for it in items:
            if not (it["accepted"] and it["rule"] == IFFC):
                continue
            for label, fn in MUTANTS.items():
                o, n = copy.deepcopy(it["raw"][0]), copy.deepcopy(it["raw"][1])
                pair = fn(o, n)
                if pair is None:
                    continue
                try:
                    mo, mn = Printer_pair(*pair)
                except Unsupported as exc:
                    mutant_unknown.append({"case": it["case"], "mutant": label,
                                           "reason": str(exc)})
                    continue
                mutants.append({"case": it["case"], "mutant": label, "old": mo, "new": mn})
        for m, v in zip(mutants, decide(mutants, "V15DecideMutants") if mutants else []):
            m["accepted"] = v

        # Pass 2: every verdict becomes a kernel-checked statement.
        body = ["From QP Require Import QueryProofDeepGuard.",
                "From Coq Require Import ZArith List. Import ListNotations.", ""]
        for prefix, batch in (("ev", items), ("mut", mutants)):
            for i, it in enumerate(batch):
                verdict = "true" if it["accepted"] else "false"
                body += [f"Definition {prefix}_o{i} := {it['old']}.",
                         f"Definition {prefix}_n{i} := {it['new']}.",
                         f"Example {prefix}_{i} : check {prefix}_o{i} {prefix}_n{i} = {verdict}.",
                         "Proof. vm_compute. reflexivity. Qed."]
                if it["accepted"]:
                    body += [f"Definition {prefix}_{i}_sound := check_sound _ _ {prefix}_{i}."]
        coq("\n".join(body) + "\n", work, "V15Kernel")

    def counts(xs, key):
        return dict(Counter(x[key] for x in xs))
    accepted = [it for it in items if it["accepted"]]
    report = {
        "schema": "queryproof.v15.deep-guard.v1",
        "nature": "RETROSPECTIVE on historical W32 development traces; kernel-decided",
        "effective_events": len(items) + len(unknown),
        "printed_events": len(items),
        "unsupported_events": len(unknown),
        "unsupported_reasons": counts(unknown, "reason"),
        "accepted_events": [{k: it[k] for k in ("case", "rule", "batch", "iteration")}
                            for it in accepted],
        "accepted_by_rule": counts(accepted, "rule"),
        "rejected_printed_by_rule": counts([it for it in items if not it["accepted"]], "rule"),
        "iffc_events_total": sum(1 for x in items + unknown if x["rule"] == IFFC),
        "iffc_accepted_cases": sorted({it["case"] for it in accepted if it["rule"] == IFFC}),
        "mutants_total": len(mutants),
        "mutants_accepted": [{k: m[k] for k in ("case", "mutant")} for m in mutants if m["accepted"]],
        "mutants_rejected_by_kind": counts([m for m in mutants if not m["accepted"]], "mutant"),
        "mutants_unsupported": mutant_unknown,
        "limits": [
            "Printer (TreeNode JSON -> term) is trusted and unverified",
            "Spark evaluator assumed to match eval_e on integral comparisons, IsNotNull, And, Or",
            "Equal opaque descriptors assumed to denote equal behaviour (source identity)",
            "TreeNode JSON may erase native metadata (see V12/V14); not re-checked here",
            "Tags, caches, SQLConf and optimizer control flow are NOT covered",
            "Per-event local soundness only; no composition over the optimizer suffix",
        ],
    }
    Path(args.report).write_text(json.dumps(report, indent=1, sort_keys=True))
    print("QUERYPROOF_V15_SUMMARY", json.dumps({k: report[k] for k in (
        "effective_events", "printed_events", "unsupported_events", "accepted_by_rule",
        "iffc_events_total", "iffc_accepted_cases", "mutants_total", "mutants_accepted",
        "mutants_rejected_by_kind")}, sort_keys=True))
    return 0 if not report["mutants_accepted"] else 1


def Printer_pair(old, new):
    p = Printer()
    return p.plan(old)[0], p.plan(new)[0]


if __name__ == "__main__":
    raise SystemExit(main())
