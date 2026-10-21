"""V15: kernel-decided deep guard certificates on real Catalyst events.

Usage: python run_v15.py TRACE [TRACE ...] --report OUT.json [--keep DIR]
       [--deps A.v,B.v --checker check2 --sound check2_sound]   (V16)

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
    return next((n for n in nodes if n["class"].endswith("plans.logical.Join")), None)


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
    if _join(old) is None or _join(new) is None:
        return None
    for plan in (old, new):
        c = _join(plan)["condition"]
        if not c or c[0]["class"] not in (CAT + "expressions.LessThan",
                                          CAT + "expressions.EqualTo"):
            return None
        c[0]["class"] = CAT + "expressions.EqualNullSafe"
    return old, new


def _joins(nodes):
    return [n for n in nodes if n["class"].endswith("plans.logical.Join")]


def _guarded_join(old, new):
    """(preorder join index, side) of the join whose input received a new
    IsNotNull-guard Filter in NEW, or None (V18 targeting)."""
    old_conds = {_cond_key(n) for n in old if n["class"].endswith("plans.logical.Filter")}
    found = []

    def walk(t):
        node, kids = t
        if node["class"].endswith("plans.logical.Join"):
            index = len(found)
            found.append(None)
            for side, (child, _) in zip(("left", "right"), kids):
                if (found[index] is None and child["class"].endswith("plans.logical.Filter")
                        and child["condition"][0]["class"] in (ISNOTNULL, CAT + "expressions.And")
                        and _cond_key(child) not in old_conds):
                    found[index] = side
        for kid in kids:
            walk(kid)
    walk(tree(new))
    hit = next(((i, s) for i, s in enumerate(found) if s is not None), None)
    if hit is None or len(_joins(old)) != len(found):
        return None
    return hit


def m_left_outer(old, new):
    """Make the guarded join LEFT OUTER, only when the new guard is on its left."""
    hit = _guarded_join(old, new)
    if hit is None or hit[1] != "left":
        return None
    i = hit[0]
    for plan in (old, new):
        if _joins(plan)[i]["joinType"]["object"] != CAT + "plans.Inner$":
            return None
    for plan in (old, new):
        _joins(plan)[i]["joinType"]["object"] = CAT + "plans.LeftOuter$"
    return old, new


def m_swap_guards(old, new):
    g = _guards(new)
    if len(g) != 2:
        return None
    g[0]["condition"], g[1]["condition"] = g[1]["condition"], g[0]["condition"]
    return old, new


def m_or_condition(old, new):
    """ON := c OR (a = a) on the guarded join, with `a` an attribute of c that is
    not the guarded one: rows whose guarded attribute is NULL can now match,
    so the guard becomes unjustified. (V17's `c OR c` was equivalent to c.)"""
    hit = _guarded_join(old, new)
    if hit is None:
        return None
    guard = next(n for n in _guarded_filters(new) if True)
    guarded = {json.dumps(x.get("exprId"), sort_keys=True)
               for x in guard["condition"] if x.get("class") == ATTR}
    cond = _joins(old)[hit[0]]["condition"]
    other = next((x for x in cond if x.get("class") == ATTR and
                  json.dumps(x.get("exprId"), sort_keys=True) not in guarded), None)
    if not cond or other is None:
        return None
    eq = [{"class": CAT + "expressions.EqualTo", "num-children": 2, "left": 0, "right": 1},
          copy.deepcopy(other), copy.deepcopy(other)]
    for plan in (old, new):
        j = _joins(plan)[hit[0]]
        j["condition"] = ([{"class": CAT + "expressions.Or", "num-children": 2,
                            "left": 0, "right": 1}] + copy.deepcopy(j["condition"]) + eq)
    return old, new


def _guarded_filters(new):
    return [n for n in new if n["class"].endswith("plans.logical.Filter")
            and n["condition"][0]["class"] in (ISNOTNULL, CAT + "expressions.And")]


def m_frame_edit(old, new):
    new[0]["queryproof_mutation"] = 1
    return old, new


def _cond_key(n):
    return json.dumps(n["condition"], sort_keys=True)


def m_drop_pushed_filter(old, new):
    """Delete a NEW filter whose condition also filters OLD (a moved filter)."""
    old_conds = {_cond_key(n) for n in old if n["class"].endswith("plans.logical.Filter")}
    for i, n in enumerate(new):
        if n["class"].endswith("plans.logical.Filter") and _cond_key(n) in old_conds:
            del new[i]
            return old, new
    return None


def m_unimplied_conjunct(old, new):
    """Strengthen a NEW filter with IsNotNull of an attribute it does not reject."""
    attrs = [a[0] for n in new if n["class"].endswith("LogicalRDD") for a in n["output"]]
    for n in new:
        if not n["class"].endswith("plans.logical.Filter"):
            continue
        used = {json.dumps(x.get("exprId"), sort_keys=True) for x in n["condition"]
                if x.get("class") == ATTR}
        other = next((a for a in attrs
                      if json.dumps(a["exprId"], sort_keys=True) not in used), None)
        if other is None:
            return None
        n["condition"] = ([{"class": CAT + "expressions.And", "num-children": 2,
                            "left": 0, "right": 1}] + n["condition"] +
                          [{"class": ISNOTNULL, "num-children": 1, "child": 0},
                           copy.deepcopy(other)])
        return old, new
    return None


MUTANTS = {"wrong_guard_attribute": m_wrong_attribute,
           "null_safe_join": m_null_safe,
           "left_outer_preserved_guard": m_left_outer,
           "swapped_guard_sides": m_swap_guards,
           "or_join_condition": m_or_condition,
           "frame_edit": m_frame_edit}
# Families applied to accepted events of rules other than IFFC (V16).
PUSH_MUTANTS = {"wrong_filter_attribute": m_wrong_attribute,
                "drop_pushed_filter": m_drop_pushed_filter,
                "unimplied_conjunct": m_unimplied_conjunct,
                "frame_edit": m_frame_edit}


# ---- Coq ----

def coq(source, workdir, name):
    path = Path(workdir) / f"{name}.v"
    path.write_text(source)
    r = subprocess.run([COQC, "-q", "-R", str(workdir), "QP", str(path)],
                       capture_output=True, text=True, cwd=workdir)
    if r.returncode != 0:
        raise RuntimeError(f"COQ_FAILED {name}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("traces", nargs="+")
    ap.add_argument("--report", required=True)
    ap.add_argument("--keep")
    ap.add_argument("--deps", default="QueryProofDeepGuard.v")
    ap.add_argument("--checker", default="check")
    ap.add_argument("--sound", default="check_sound")
    args = ap.parse_args()
    deps = args.deps.split(",")
    module = " ".join(d[:-2] for d in deps)

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
        work = (Path(args.keep) if args.keep else Path(tmp)).resolve()
        work.mkdir(parents=True, exist_ok=True)
        for dep in deps:
            coq_out = coq((PROOFS / dep).read_text(), work, dep[:-2])
            assert "Closed under the global context" in coq_out, "AXIOMS_IN_" + dep

        def decide(batch, name):
            body = [f"From QP Require Import {module}.",
                    "From Coq Require Import ZArith List. Import ListNotations.", ""]
            for i, it in enumerate(batch):
                body.append(f"Definition o{i} := {it['old']}.")
                body.append(f"Definition n{i} := {it['new']}.")
                body.append(f"Eval vm_compute in {args.checker} o{i} n{i}.")
            out = coq("\n".join(body) + "\n", work, name)
            verdicts = re.findall(r"=\s*(true|false)\s*\n\s*:\s*bool", out)
            assert len(verdicts) == len(batch), "VERDICT_COUNT_MISMATCH"
            return [v == "true" for v in verdicts]

        for it, v in zip(items, decide(items, "V15Decide")):
            it["accepted"] = v

        mutants, mutant_unknown = [], []
        for it in items:
            if not it["accepted"]:
                continue
            families = MUTANTS if it["rule"] == IFFC else PUSH_MUTANTS
            for label, fn in families.items():
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
                mutants.append({"case": it["case"], "rule": it["rule"], "mutant": label,
                                "old": mo, "new": mn})
        for m, v in zip(mutants, decide(mutants, "V15DecideMutants") if mutants else []):
            m["accepted"] = v

        # Pass 2: every verdict becomes a kernel-checked statement.
        body = [f"From QP Require Import {module}.",
                "From Coq Require Import ZArith List. Import ListNotations.", ""]
        for prefix, batch in (("ev", items), ("mut", mutants)):
            for i, it in enumerate(batch):
                verdict = "true" if it["accepted"] else "false"
                body += [f"Definition {prefix}_o{i} := {it['old']}.",
                         f"Definition {prefix}_n{i} := {it['new']}.",
                         f"Example {prefix}_{i} : {args.checker} {prefix}_o{i} {prefix}_n{i} = {verdict}.",
                         "Proof. vm_compute. reflexivity. Qed."]
                if it["accepted"]:
                    body += [f"Definition {prefix}_{i}_sound := {args.sound} _ _ {prefix}_{i}."]
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
        "events_by_rule": counts(items + unknown, "rule"),
        "unsupported_by_rule": counts(unknown, "rule"),
        "unsupported_by_rule_reason": counts(
            [{"k": x["rule"] + " | " + x["reason"]} for x in unknown], "k"),
        "rejected_events": [{k: it[k] for k in ("case", "rule", "batch", "iteration")}
                            for it in items if not it["accepted"]],
        "mutants_by_rule": counts(mutants, "rule"),
        # Ordered like ev_<i> / mut_<i> in the generated kernel file.
        "event_index": [{k: it[k] for k in ("case", "rule", "accepted")} for it in items],
        "mutant_index": [{k: m[k] for k in ("case", "rule", "mutant", "accepted")}
                         for m in mutants],
        "iffc_events_total": sum(1 for x in items + unknown if x["rule"] == IFFC),
        "iffc_accepted_cases": sorted({it["case"] for it in accepted if it["rule"] == IFFC}),
        "mutants_total": len(mutants),
        "checker": args.checker,
        "mutants_accepted": [{k: m[k] for k in ("case", "rule", "mutant")}
                             for m in mutants if m["accepted"]],
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
