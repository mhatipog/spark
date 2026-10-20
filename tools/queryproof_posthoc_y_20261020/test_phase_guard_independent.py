"""Independent synthetic falsification checks for the UNFROZEN Y phase guard.

The W32 and X snapshots are never mutated. These are manually constructed
observer records, not evidence that Spark emitted them. Outcomes are diagnostic.
"""
import base64
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1]
for subdir in ("queryproof_posthoc_y_20261020",
               "queryproof_state_frame_20261020",
               "queryproof_state_witness_20261020"):
    sys.path.insert(0, str(TOOLS / subdir))

from phase_guard import score_phase_local


CASE = "Y_FIXTURE_01"
SCOPE_SUMMARY = {
    "scope_markers": 8,
    "recorded_rule_events": 1,
    "post_finish_analysis_optimizer_suffixes": 1,
    "optimizer_suffix_case_counts": [
        {"case": CASE, "proved": True, "changed": True}
    ],
}


def marker(name, fields):
    return name + "\t" + "\t".join(
        base64.b64encode(str(value).encode("utf8")).decode("ascii")
        for value in fields)


def state_tuple(opaque=0, tagged=0, metadata=0):
    return (tagged, metadata, opaque, 1)


def synthetic_trace(*, pre_opaque=0, cut_opaque=0,
                    event_opaque=0, exit_opaque=0,
                    stable_tags=False, drop_last_frame=False):
    # A complete, well-nested optimizer executor, two batches, and one
    # changed rule. All source positions are explicitly enumerated.
    lines = []
    frame_tags = "fixed-opaque-tag-digest" if stable_tags else ""
    def emit_scope(kind, batch, uid, phase, stats):
        context = [CASE, kind, "Optimizer", batch, uid, phase]
        lines.append(marker("QUERYPROOF_SCOPE", [*context, "[]"]))
        lines.append(marker("QUERYPROOF_STATE", [
            CASE, "SCOPE", *context[1:], *stats]))
        lines.append(marker("QUERYPROOF_FRAME", [
            CASE, "SCOPE", *context[1:], frame_tags, "", "OUT", "false"]))
    def emit_event(stats):
        context = [CASE, "Optimizer", "Infer Filters", "InferFiltersFromConstraints", "1"]
        lines.append(marker("QUERYPROOF_AST", [*context, "[]", "[{}]"]))
        lines.append(marker("QUERYPROOF_STATE", [
            CASE, "EVENT", *context[1:], *stats, *stats]))
        lines.append(marker("QUERYPROOF_FRAME", [
            CASE, "EVENT", *context[1:],
            frame_tags, "", "OUT", "false",
            frame_tags, "", "OUT", "false"]))
    tag = 1 if stable_tags else 0
    normal = state_tuple(tagged=tag)
    emit_scope("EXECUTOR", "Optimizer", "1", "BEGIN", normal)
    emit_scope("BATCH", "Resolution", "2", "BEGIN", normal)
    emit_scope("BATCH", "Resolution", "2", "END",
               state_tuple(opaque=pre_opaque, tagged=tag))
    emit_scope("BATCH", "Finish Analysis", "3", "BEGIN", normal)
    emit_scope("BATCH", "Finish Analysis", "3", "END",
               state_tuple(opaque=cut_opaque, tagged=tag))
    emit_scope("BATCH", "Infer Filters", "4", "BEGIN", normal)
    emit_event(state_tuple(opaque=event_opaque, tagged=tag))
    emit_scope("BATCH", "Infer Filters", "4", "END", normal)
    emit_scope("EXECUTOR", "Optimizer", "1", "END",
               state_tuple(opaque=exit_opaque, tagged=tag))
    if drop_last_frame:
        lines.pop()
    return "\n".join(lines) + "\n"


class IndependentPhaseGuardTests(unittest.TestCase):
    def score(self, **kwargs):
        with tempfile.TemporaryDirectory(prefix="queryproof-y-synthetic-") as temp:
            path = Path(temp) / "fixture.log"
            path.write_text(synthetic_trace(**kwargs), encoding="utf8")
            return score_phase_local(path, SCOPE_SUMMARY)

    def test_clean_suffix_passes_narrow_observational_gate(self):
        report = self.score()
        self.assertEqual(report["status"], "COMPLETE_PHASE_LOCAL_OBSERVED_STATE")
        self.assertEqual(report["phase_local_eligible_cases"], [CASE])

    def test_precut_opaque_must_not_poison_postcut_proof(self):
        report = self.score(pre_opaque=1)
        self.assertEqual(report["phase_local_eligible_cases"], [CASE])
        self.assertEqual(report["legacy_global_state_guard_count"], 0)

    def test_opaque_at_cut_fails_closed(self):
        report = self.score(cut_opaque=1)
        self.assertEqual(report["phase_local_eligible_count"], 0)

    def test_opaque_in_postcut_changed_rule_fails_closed(self):
        report = self.score(event_opaque=1)
        self.assertEqual(report["phase_local_eligible_count"], 0)

    def test_opaque_at_optimizer_exit_fails_closed(self):
        report = self.score(exit_opaque=1)
        self.assertEqual(report["phase_local_eligible_count"], 0)

    def test_missing_frame_fails_closed(self):
        report = self.score(drop_last_frame=True)
        self.assertEqual(report["status"], "INVALID_PHASE_LOCAL_STATE")
        self.assertEqual(report["phase_local_eligible_count"], 0)

    def test_tag_presence_can_pass_y_but_does_not_prove_identity(self):
        report = self.score(stable_tags=True)
        self.assertEqual(report["phase_local_eligible_cases"], [CASE])
        # Two DIFFERENT tag maps have the SAME exact prehash encoding in
        # instrument_frame.py. This is a representation collision, not an
        # attack on SHA-256. So this acceptance cannot prove real state equality.
        first = {"qpA": "one|qpB:two"}
        second = {"qpA": "one", "qpB": "two"}
        def spark_encoding(values):
            return "|".join(sorted(k + ":" + v for k,v in values.items()))
        self.assertNotEqual(first, second)
        self.assertEqual(spark_encoding(first), spark_encoding(second))
        self.assertEqual(
            hashlib.sha256(spark_encoding(first).encode()).digest(),
            hashlib.sha256(spark_encoding(second).encode()).digest())


if __name__ == "__main__":
    unittest.main()
