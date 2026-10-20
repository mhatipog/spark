"""Synthetic positive and adversarial controls for hidden Catalyst state framing."""
import base64
import unittest

from frame_guard import parse_records, audit_frame_records


def enc(prefix, fields):
    return prefix + "\t" + "\t".join(
        base64.b64encode(str(v).encode()).decode() for v in fields)


def scope(batch, uid, phase, plan="[]", tags="T:a", attrs="id:md", output="out"):
    fields = ["H01", "EXECUTOR" if not batch else "BATCH", "Optimizer",
              batch, str(uid), phase, plan]
    context = ["H01", "SCOPE", *fields[1:6]]
    return [
        enc("QUERYPROOF_SCOPE", fields),
        enc("QUERYPROOF_STATE", context + [1, 1, 0, 1]),
        enc("QUERYPROOF_FRAME", context + [tags, attrs, output, "false"]),
    ]


def event(before="T:a", after="T:a", output_before="out", output_after="out",
          attrs_before="id:md", attrs_after="id:md", opaque=False):
    fields = ["H01", "Optimizer", "Aggregate", "Rule", "1", "[]", "[]"]
    context = ["H01", "EVENT", *fields[1:5]]
    return [
        enc("QUERYPROOF_AST", fields),
        enc("QUERYPROOF_STATE", context + [
            1, 1, 1 if opaque else 0, 1, 1, 1, 0, 1]),
        enc("QUERYPROOF_FRAME", context + [
            before, attrs_before, output_before, "false",
            after, attrs_after, output_after, "false"]),
    ]


def sample(event_lines=None, other_end_tags="T:a"):
    if event_lines is None:
        event_lines = event()
    return [
        *scope("", 1, "BEGIN", tags="T:before"),
        *scope("Finish Analysis", 2, "BEGIN", tags="T:before"),
        *scope("Finish Analysis", 2, "END"),
        *scope("Aggregate", 3, "BEGIN"),
        *event_lines,
        *scope("Aggregate", 3, "END", tags=other_end_tags),
        *scope("", 1, "END"),
    ]


def audit(lines):
    records, errs = parse_records(lines)
    return audit_frame_records(records, errs, {
        "scope_markers": 6,
        "recorded_rule_events": 1,
        "post_finish_analysis_optimizer_suffixes": 1,
        "optimizer_suffix_case_counts": [{
            "case": "H01", "proved": True, "changed": True}],
    })


class FrameGuardTests(unittest.TestCase):
    def test_preserved_hidden_frames_accept(self):
        report = audit(sample())
        self.assertEqual(report["status"], "COMPLETE_FRAME_COMPARISON")
        self.assertEqual(report["newly_frame_preserved_count"], 1)

    def test_rule_tag_mutation_rejected(self):
        report = audit(sample(event(before="T:a", after="T:b")))
        self.assertEqual(report["newly_frame_preserved_count"], 0)
        self.assertIn("TAG_FRAME_CHANGED", report["suffix_failure_reasons"])

    def test_batch_tag_mutation_rejected(self):
        report = audit(sample(other_end_tags="T:b"))
        self.assertEqual(report["newly_frame_preserved_count"], 0)

    def test_surviving_attribute_metadata_change_rejected(self):
        report = audit(sample(event(attrs_after="id:other")))
        self.assertEqual(report["newly_frame_preserved_count"], 0)
        self.assertIn("SURVIVING_ATTRIBUTE_METADATA_CHANGED",
                      report["suffix_failure_reasons"])

    def test_output_metadata_change_rejected(self):
        report = audit(sample(event(output_after="other")))
        self.assertEqual(report["newly_frame_preserved_count"], 0)

    def test_opaque_collection_rejected(self):
        report = audit(sample(event(opaque=True)))
        self.assertEqual(report["newly_frame_preserved_count"], 0)

    def test_missing_frame_fails_closed(self):
        report = audit(sample()[:-1])
        self.assertEqual(report["status"], "INVALID_FRAME_COMPARISON")
        self.assertEqual(report["newly_frame_preserved_count"], 0)


if __name__ == "__main__":
    unittest.main()
