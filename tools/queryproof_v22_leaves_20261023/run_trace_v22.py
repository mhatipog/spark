"""V22 (copy of V21 run_trace_v21.py): V22 printer (leaf identity, opaque reads) and
QueryProofDeepWide3/Trace3. Input traces must have leaf identities attached
(attach_leaf_ids.py).

V21 (copy of the frozen V20 run_trace.py): V21 printer and QueryProofDeepWide2/Trace2.

V20: kernel-decided whole-trace certificates under the effect contract.

Usage: python run_trace.py TRACE.stderr [...] --report OUT.json [--keep DIR]

Per query: the logical optimizer's events after `Finish Analysis`, hidden-tag
names before and after each event (QUERYPROOF_V20_TAGS stream), printed with
ONE printer per query so identical plans give identical terms. Coq decides
trace_ok (events by check4, connectivity, tags) and each part of it, then
re-asserts every verdict as a kernel-checked Example. Each accepted trace gets
a closed trace_eval instance and trace-level mutants that must be rejected.
Python decides nothing.
"""
import argparse
import base64
import copy
import importlib.util
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
sys.path.insert(0, str(ROOT / "tools/queryproof_v15_deep_guard_20261021"))
from rank_ast import decode  # noqa: E402

COQC = os.environ.get("COQC", "coqc")
FINISH = "Finish Analysis"
UNKNOWN_TAG_ID = 999


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


PRINTER = load(HERE / "print_terms_v22.py", "qp_printer_v22")
CONTRACT_DIR = ROOT / "tools/queryproof_v20_trace_20261023"   # contract and inventory are reused unchanged
V15 = load(ROOT / "tools/queryproof_v15_deep_guard_20261021/run_v15.py", "qp_run_v15")
# Mutant families that change only NEW (a corrupted rewrite spliced into a trace).
def m_leaf_data_swap(old, new):
    """V22: same plan JSON, but one leaf in NEW now holds different data."""
    for n in new:
        if n.get("queryproof_leaf_fp"):
            n["queryproof_leaf_fp"] = n["queryproof_leaf_fp"] + ":swapped"
            return old, new
    return None


SPLICE = {"wrong_guard_attribute": V15.m_wrong_attribute,
          "swapped_guard_sides": V15.m_swap_guards,
          "drop_pushed_filter": V15.m_drop_pushed_filter,
          "unimplied_conjunct": V15.m_unimplied_conjunct,
          "frame_edit": V15.m_frame_edit}


def tags_line(line):
    return [base64.b64decode(x).decode() for x in line.rstrip("\n").split("\t")[1:]]


def parse_query(path):
    """Returns (events, ansi, cut_ok, unknown_tag_lines) for the logical optimizer."""
    records = []
    for line in Path(path).read_text(errors="replace").splitlines():
        if line.startswith("QUERYPROOF_AST\t"):
            records.append(("ast", decode(line)))
        elif line.startswith("QUERYPROOF_V20_TAGS\t"):
            records.append(("tags", tags_line(line)))
    opt = next((r[1][2] for r in records if r[0] == "tags" and r[1][0] == "INIT"
                and r[1][3] == FINISH), None)
    if opt is None:
        return None
    ansi = next(r[1][6] for r in records if r[0] == "tags" and r[1][0] == "INIT" and r[1][2] == opt)
    cur, pending, events, finish_after, unknown = None, None, [], None, 0
    for kind, rec in records:
        if kind == "tags":
            if rec[2] != opt:
                continue
            if rec[0] == "INIT":
                cur = rec[7]
            elif rec[0] == "CHANGE":
                pending = ((rec[3], rec[4], rec[5]), cur, rec[7])
                cur = rec[7]
            else:
                unknown += 1
            continue
        e = rec
        if e["executor"] != opt or e["old"] == e["new"]:
            continue
        key = (e["batch"], e["rule"], str(e["iteration"]))
        if pending and pending[0] == key:
            hb, ha = pending[1], pending[2]
            pending = None
        else:
            hb = ha = cur
        if e["batch"] == FINISH:
            finish_after = e["new"]
            events = []
            continue
        events.append({"rule": e["rule"].split(".")[-1], "batch": e["batch"],
                       "old": e["old"], "new": e["new"],
                       "hb": [t for t in (hb or "").split(",") if t],
                       "ha": [t for t in (ha or "").split(",") if t]})
    cut_ok = finish_after is None or not events or finish_after == events[0]["old"]
    return events, ansi, cut_ok, unknown


def coq(source, workdir, name):
    path = Path(workdir) / f"{name}.v"
    path.write_text(source)
    r = subprocess.run([COQC, "-q", "-R", str(workdir), "QP", str(path)],
                       capture_output=True, text=True, cwd=workdir)
    if r.returncode != 0:
        raise RuntimeError(f"COQ_FAILED {name}\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    return r.stdout


def coq_list(xs):
    return "[" + "; ".join(xs) + "]"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("traces", nargs="+")
    ap.add_argument("--report", required=True)
    ap.add_argument("--keep")
    args = ap.parse_args()

    contract = json.loads((CONTRACT_DIR / "EFFECT_CONTRACT.json").read_text())
    inventory = json.loads((CONTRACT_DIR / "TAG_INVENTORY.json").read_text())
    assert set(contract["tags"]) == set(inventory), "CONTRACT_DOES_NOT_COVER_INVENTORY"
    assert not contract["result_affecting_tags_found"]
    tag_ids = {name: i for i, name in enumerate(sorted(contract["tags"]))}

    def tid(name):
        return tag_ids.get(name, UNKNOWN_TAG_ID)

    queries, skipped = [], Counter()
    for path in args.traces:
        qid = Path(path).name.split(".")[0]
        parsed = parse_query(path)
        if parsed is None:
            skipped["NO_OPTIMIZER"] += 1
            continue
        events, ansi, cut_ok, unknown_lines = parsed
        printer = PRINTER.Printer()
        printed, why = [], None
        for e in events:
            try:
                printed.append((printer.plan(e["old"])[0], printer.plan(e["new"])[0]))
            except PRINTER.Unsupported as exc:
                why = f"{e['rule']}: {exc}"
                break
        unknown_tags = sorted({t for e in events for t in e["hb"] + e["ha"] if t not in tag_ids})
        queries.append({"case": qid, "events": events, "printed": printed if why is None else None,
                        "unsupported": why, "ansi": ansi, "cut_connected": cut_ok,
                        "unknown_tag_lines": unknown_lines, "unknown_tags": unknown_tags,
                        "json_breaks": sum(1 for a, b in zip(events, events[1:]) if a["new"] != b["old"]),
                        "printer": printer})

    def ev_term(before, hb, after, ha):
        return (f"(mkEvent {before} {coq_list(str(tid(t)) for t in hb)} "
                f"{after} {coq_list(str(tid(t)) for t in ha)})")

    with tempfile.TemporaryDirectory(prefix="qp-v20-") as tmp:
        work = (Path(args.keep) if args.keep else Path(tmp)).resolve()
        work.mkdir(parents=True, exist_ok=True)
        for dep in ("QueryProofDeepWide3.v", "QueryProofTrace3.v"):
            out = coq((PROOFS / dep).read_text(), work, dep[:-2])
            assert "Closed under the global context" in out, "AXIOMS_IN_" + dep
        header = ["From QP Require Import QueryProofDeepWide3 QueryProofTrace3.",
                  "From Coq Require Import ZArith List. Import ListNotations.",
                  f"Definition ok_ids : list nat := {coq_list(str(i) for i in tag_ids.values())}.",
                  "Definition ok_tag (x : nat) : bool := memb x ok_ids.",
                  "Fixpoint links (es : list event) : list bool :=",
                  "  match es with",
                  "  | e :: ((e2 :: _) as rest) => plan_eqb (ev_after e) (ev_before e2) :: links rest",
                  "  | _ => []",
                  "  end.", ""]

        # (name, list of event terms) for every trace and trace-level mutant
        traces = []
        for qi, q in enumerate(queries):
            if q["printed"] == []:
                # No effective optimizer event: the plan is unchanged (trivially
                # certified; counted separately from non-empty certified traces).
                q.update(trace_ok=True, event_ok=[], links_ok=[])
            if not q["printed"]:
                continue
            terms = [ev_term(o, e["hb"], n, e["ha"]) for (o, n), e in zip(q["printed"], q["events"])]
            q["coq"] = f"q{qi}"
            traces.append((f"q{qi}", terms, q, None))

        body = list(header)
        for name, terms, _, _ in traces:
            body.append(f"Definition {name}_evs : list event := {coq_list(terms)}.")
            body.append(f"Eval vm_compute in (trace_ok ok_tag {name}_evs, "
                        f"map (event_ok ok_tag) {name}_evs, links {name}_evs).")
        out = coq("\n".join(body) + "\n", work, "V20Decide")
        results = re.findall(r"=\s*\((true|false),\s*\[([^\]]*)\],\s*\[([^\]]*)\]\)", out.replace("\n", " "))
        assert len(results) == len(traces), ("VERDICT_COUNT", len(results), len(traces))
        for (name, terms, q, _), (ok, evs, links) in zip(traces, results):
            q["trace_ok"] = ok == "true"
            q["event_ok"] = [b.strip() == "true" for b in evs.split(";") if b.strip()]
            q["links_ok"] = [b.strip() == "true" for b in links.split(";") if b.strip()]

        # Trace-level mutants of accepted, non-empty traces.
        mutants = []
        for name, terms, q, _ in list(traces):
            if not q["trace_ok"] or not terms:
                continue
            if len(terms) >= 3:
                mutants.append(("drop_middle_event", q["case"], terms[:1] + terms[2:]))
            if len(terms) >= 2:
                mutants.append(("swap_adjacent_events", q["case"], [terms[1], terms[0]] + terms[2:]))
            e0 = q["events"][0]
            mutants.append(("unknown_tag", q["case"],
                            [ev_term(q["printed"][0][0], e0["hb"], q["printed"][0][1],
                                     e0["ha"] + ["queryproof_unknown_tag"])] + terms[1:]))
            # V22: the same plan JSON with one leaf holding different data (event 0).
            pair = m_leaf_data_swap(copy.deepcopy(q["events"][0]["old"]),
                                    copy.deepcopy(q["events"][0]["new"]))
            if pair is not None:
                try:
                    bad = q["printer"].plan(pair[1])[0]
                    seq = list(terms)
                    e0 = q["events"][0]
                    seq[0] = ev_term(q["printed"][0][0], e0["hb"], bad, e0["ha"])
                    if len(seq) > 1:
                        e1 = q["events"][1]
                        seq[1] = ev_term(bad, e1["hb"], q["printed"][1][1], e1["ha"])
                    mutants.append(("leaf_data_swap", q["case"], seq))
                except PRINTER.Unsupported:
                    pass
            for i, e in enumerate(q["events"]):
                done = False
                for label, fn in SPLICE.items():
                    pair = fn(copy.deepcopy(e["old"]), copy.deepcopy(e["new"]))
                    if pair is None or pair[0] != e["old"] or pair[1] == e["new"]:
                        continue
                    try:
                        bad = q["printer"].plan(pair[1])[0]
                    except PRINTER.Unsupported:
                        continue
                    seq = list(terms)
                    seq[i] = ev_term(q["printed"][i][0], e["hb"], bad, e["ha"])
                    if i + 1 < len(seq):     # keep connectivity: next event starts from the mutant
                        seq[i + 1] = ev_term(bad, q["events"][i + 1]["hb"],
                                             q["printed"][i + 1][1], q["events"][i + 1]["ha"])
                    mutants.append(("splice_" + label, q["case"], seq))
                    done = True
                    break
                if done:
                    break
        body = list(header)
        for mi, (_, _, seq) in enumerate(mutants):
            body.append(f"Eval vm_compute in trace_ok ok_tag {coq_list(seq)}.")
        mut_verdicts = []
        if mutants:
            out = coq("\n".join(body) + "\n", work, "V20DecideMutants")
            mut_verdicts = [v == "true" for v in re.findall(r"=\s*(true|false)\s*\n\s*:\s*bool", out)]
            assert len(mut_verdicts) == len(mutants), "MUTANT_VERDICT_COUNT"

        # Pass 2: kernel-checked statements for every verdict.
        body = list(header)
        for name, terms, q, _ in traces:
            body.append(f"Definition {name}_evs : list event := {coq_list(terms)}.")
            verdict = "true" if q["trace_ok"] else "false"
            body.append(f"Example {name}_trace : trace_ok ok_tag {name}_evs = {verdict}.")
            body.append("Proof. vm_compute. reflexivity. Qed.")
            if q["trace_ok"] and terms:
                body.append(f"Definition {name}_meaning := trace_eval ok_tag _ _ {name}_trace.")
        for mi, ((_, _, seq), v) in enumerate(zip(mutants, mut_verdicts)):
            body.append(f"Example mut{mi} : trace_ok ok_tag {coq_list(seq)} = "
                        f"{'true' if v else 'false'}.")
            body.append("Proof. vm_compute. reflexivity. Qed.")
        coq("\n".join(body) + "\n", work, "V20Kernel")

    printed = [q for q in queries if q["printed"] is not None]
    full = [q for q in printed if q.get("trace_ok")]
    report = {
        "schema": "queryproof.v22.trace.v1",
        "queries": len(queries) + sum(skipped.values()),
        "skipped": dict(skipped),
        "queries_with_suffix_events": sum(1 for q in queries if q["events"]),
        "printed_traces": len(printed),
        "unprintable_traces": [{"case": q["case"], "why": q["unsupported"]} for q in queries
                               if q["printed"] is None],
        "certified_full_traces": len(full),
        "certified_nonempty_full_traces": sum(1 for q in full if q["events"]),
        "certified_cases": sorted(q["case"] for q in full),
        "json_connectivity_breaks": sum(q["json_breaks"] for q in queries),
        "kernel_link_failures": sum(q["links_ok"].count(False) for q in printed),
        "cut_disconnected": sorted(q["case"] for q in queries if not q["cut_connected"]),
        "ansi_off": sorted(q["case"] for q in queries if q["ansi"] != "true"),
        "unknown_tags": sorted({t for q in queries for t in q["unknown_tags"]}),
        "unknown_tag_lines": sum(q["unknown_tag_lines"] for q in queries),
        "observed_tags": dict(Counter(t for q in queries for e in q["events"]
                                      for t in set(e["hb"] + e["ha"]))),
        "events_total": sum(len(q["events"]) for q in queries),
        "events_by_rule": dict(Counter(e["rule"] for q in queries for e in q["events"])),
        "uncertified_events_by_rule": dict(Counter(
            e["rule"] for q in printed for e, ok in zip(q["events"], q["event_ok"]) if not ok)),
        "first_failure_by_rule": dict(Counter(
            next((e["rule"] for e, ok in zip(q["events"], q["event_ok"]) if not ok), "LINK_OR_TAG")
            for q in printed if not q["trace_ok"])),
        "unprintable_by_reason": dict(Counter((q["unsupported"] or "").split(": ")[-1]
                                              for q in queries if q["printed"] is None)),
        "trace_mutants": len(mutants),
        "trace_mutants_accepted": [{"kind": k, "case": c} for (k, c, _), v in zip(mutants, mut_verdicts) if v],
        "trace_mutants_rejected_by_kind": dict(Counter(k for (k, _, _), v in zip(mutants, mut_verdicts) if not v)),
        "certified_endpoints": [{"case": q["case"], "first": q["printed"][0][0], "last": q["printed"][-1][1]}
                                for q in full if q["events"]],
    }
    Path(args.report).write_text(json.dumps(report, indent=1))
    print("QUERYPROOF_V22_TRACE", json.dumps({k: report[k] for k in (
        "queries", "queries_with_suffix_events", "printed_traces", "certified_full_traces",
        "certified_nonempty_full_traces", "json_connectivity_breaks", "kernel_link_failures",
        "ansi_off", "unknown_tags", "trace_mutants", "trace_mutants_accepted",
        "first_failure_by_rule")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
