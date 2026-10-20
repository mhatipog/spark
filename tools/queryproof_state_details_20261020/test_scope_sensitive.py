"""Boundary controls for the suffix-local hidden-state audit."""
import base64
import unittest

from scope_sensitive_guard import audit_details


def encode(prefix, fields):
    return prefix + "\t" + "\t".join(
        base64.b64encode(str(v).encode()).decode() for v in fields)


def point(kind, executor, batch, uid, phase, tags=0, metadata=0, opaque=0):
    raw = ["H01", kind, executor, batch, str(uid), phase, "[]"]
    ctx = ["H01", "SCOPE", kind, executor, batch, str(uid), phase]
    return [
        encode("QUERYPROOF_SCOPE", raw),
        encode("QUERYPROOF_STATE", ctx + [tags, metadata, opaque, 1]),
        encode("QUERYPROOF_STATE_CLASS", ctx + [
            "M:Before=1" if metadata else "T:Before=1" if tags else ""]),
    ]


def changed(tags=0, metadata=0, opaque=0):
    raw = ["H01", "Optimizer", "Aggregate", "Rule", "1", "[]", "[]"]
    ctx = ["H01", "EVENT", "Optimizer", "Aggregate", "Rule", "1"]
    return [
        encode("QUERYPROOF_AST", raw),
        encode("QUERYPROOF_STATE", ctx + [
            tags, metadata, opaque, 1, 0, 0, 0, 1]),
        encode("QUERYPROOF_STATE_CLASS", ctx + [
            "T:Rule=1" if tags else "M:Rule=1" if metadata else "", ""]),
    ]


def sample(pre_tags=0, suffix_meta=0, after_event_tags=0):
    return [
        *point("EXECUTOR", "Optimizer", "", 1, "BEGIN", tags=pre_tags),
        *point("BATCH", "Optimizer", "Finish Analysis", 2, "BEGIN"),
        *point("BATCH", "Optimizer", "Finish Analysis", 2, "END"),
        *point("BATCH", "Optimizer", "Aggregate", 3, "BEGIN", metadata=suffix_meta),
        *changed(tags=after_event_tags),
        *point("BATCH", "Optimizer", "Aggregate", 3, "END"),
        *point("EXECUTOR", "Optimizer", "", 1, "END"),
    ]


def audit(lines):
    return audit_details(lines, {
        "scope_markers": 6,
        "recorded_rule_events": 1,
        "optimizer_suffix_case_counts": [{
            "case": "H01", "changed": True, "proved": True}],
    })


class ScopeSensitiveTests(unittest.TestCase):
    def test_hidden_pre_normalization_tag_not_a_suffix_blocker(self):
        report = audit(sample(pre_tags=1))
        self.assertEqual(report["status"], "COMPLETE_CLASSIFIED_JVM_TRACE")
        self.assertEqual(report["clean_suffix_count"], 1)

    def test_metadata_after_normalization_blocks_suffix(self):
        report = audit(sample(suffix_meta=1))
        self.assertEqual(report["clean_suffix_count"], 0)
        self.assertEqual(report["suffix_guard_blockers_by_case"], {"H01": ["metadata"]})

    def test_changed_rule_tags_block_suffix(self):
        report = audit(sample(after_event_tags=1))
        self.assertEqual(report["clean_suffix_count"], 0)
        self.assertIn(["T:Rule", 1], [list(x) for x in report["top_suffix_hidden_classes"]])

    def test_missing_detail_fails_closed(self):
        broken = sample()[:-1]
        report = audit(broken)
        self.assertEqual(report["status"], "INVALID_DETAIL_TRACE")
        self.assertEqual(report["clean_suffix_count"], 0)


if __name__ == "__main__":
    unittest.main()
