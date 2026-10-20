"""Round Y development: PHASE-LOCAL observed-state guard for optimizer suffixes.

Global nonempty tag/metadata/opaque occurrence is not a sound reason to
reject a post-FinishAnalysis *suffix*: it conflates earlier Analyzer state
with post-cut optimizer transitions. Require instead:
  - complete state+frame witness streams;
  - zero opaque collections at cut, every post-cut event/batch and exit;
  - post-cut stable tag/metadata fingerprints (frame guard, hashed);
  - no scope mismatches.
This depends on the frame observer; it is NOT a fifth independent proof.
"""
from collections import Counter, defaultdict

from frame_guard import parse_records, score_frame
from state_guard import score_state_witness


def score_phase_local(path, scopes):
    records, errors = parse_records(
        path.read_text(errors="replace").splitlines())
    state = score_state_witness(path, scopes)
    frame = score_frame(path, scopes)
    candidates = {s["case"] for s in
                  scopes.get("optimizer_suffix_case_counts", [])
                  if s["changed"] and s["proved"]}
    stack = []
    cut_seen = set()
    dirty = defaultdict(Counter)
    samples = defaultdict(Counter)

    def observe(case, stats, stage):
        if len(stats) != 4:
            errors.append("PHASE_BAD_STATE_ARITY")
            return
        samples[case][stage] += 1
        if stats[2] > 0:
            dirty[case]["opaque_" + stage] += 1
        if stats[0] > 0:
            samples[case]["tag_present_" + stage] += 1
        if stats[1] > 0:
            samples[case]["metadata_present_" + stage] += 1

    for record in records:
        core = record["core"]
        case = core[0]
        if record["kind"] == "EVENT":
            if not stack or stack[-1]["kind"] != "BATCH":
                errors.append("PHASE_EVENT_OUTSIDE_BATCH")
                break
            if any(x["kind"] == "EXECUTOR" and x["finished"] for x in stack):
                for stats in record["states"]:
                    observe(case, stats, "post_event")
            continue
        _case, kind, executor, batch, uid, phase = core
        if phase == "BEGIN":
            post = any(x["kind"] == "EXECUTOR" and x["finished"] for x in stack)
            entry = {"kind": kind, "case": case, "executor": executor,
                     "batch": batch, "uid": uid, "post": post, "finished": False}
            stack.append(entry)
            if post:
                observe(case, record["states"][0], "post_batch_begin")
            continue
        if not stack:
            errors.append("PHASE_ORPHAN_END")
            break
        entry = stack.pop()
        if any(entry[k] != v for k, v in (
                ("kind", kind), ("case", case), ("executor", executor),
                ("batch", batch), ("uid", uid))):
            errors.append("PHASE_SCOPE_MISMATCH")
            break
        if entry["post"]:
            observe(case, record["states"][0], "post_batch_end")
        if kind == "BATCH" and batch == "Finish Analysis":
            if not stack or stack[-1]["kind"] != "EXECUTOR":
                errors.append("PHASE_FINISH_WITHOUT_EXECUTOR")
                break
            stack[-1]["finished"] = True
            cut_seen.add(case)
            observe(case, record["states"][0], "finish_cut")
        if kind == "EXECUTOR" and entry["finished"]:
            observe(case, record["states"][0], "optimizer_exit")
    if stack:
        errors.append("PHASE_UNCLOSED_SCOPES")
    if state.get("status") != "COMPLETE_JVM_WITNESSES":
        errors.append("PHASE_INCOMPLETE_STATE_STREAM")
    if frame.get("status") != "COMPLETE_FRAME_COMPARISON":
        errors.append("PHASE_INCOMPLETE_FRAME_STREAM")
    frame_eligible = set(frame.get("newly_frame_preserved_suffixes", []))
    survivors = (candidates & cut_seen & frame_eligible) - set(dirty)
    if errors:
        survivors = set()
    return {
        "status": "COMPLETE_PHASE_LOCAL_OBSERVED_STATE" if not errors
                  else "INVALID_PHASE_LOCAL_STATE",
        "errors": sorted(set(errors)),
        "candidates": sorted(candidates),
        "optimizer_cuts": sorted(cut_seen),
        "frame_eligible": sorted(frame_eligible),
        "postcut_opaque_cases": {c: dict(v) for c,v in sorted(dirty.items())},
        "phase_local_eligible_cases": sorted(survivors),
        "phase_local_eligible_count": len(survivors),
        "sample_counter_totals": dict(sorted(
            sum((counter for counter in samples.values()), Counter()).items())),
        "legacy_global_state_guard_count": state.get(
            "state_guarded_changed_suffix_count"),
        "limitations": [
            "Development, posthoc W replay, not first-run or independent of frame audit",
            "Only inspected state categories; tag/metadata hashes are not injective",
            "In-suffix mutation restored between snapshots can be missed",
            "The theorem concerns the suffix after Finish Analysis, not Analyzer correctness",
        ],
    }
