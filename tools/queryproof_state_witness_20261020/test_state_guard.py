"""Adversarial controls for the retrospective state guard."""
import base64
import unittest

from state_guard import audit_witness_stream


def marker(kind, fields):
    return kind + "\t" + "\t".join(
        base64.b64encode(str(value).encode()).decode() for value in fields)


SCOPE = ["H01", "BATCH", "Optimizer", "Aggregate", "7", "BEGIN", "[]"]
EVENT = ["H01", "Optimizer", "Aggregate", "CollapseProject", "1", "[]", "[]"]


def scope_line():
    return marker("QUERYPROOF_SCOPE", SCOPE)


def event_line():
    return marker("QUERYPROOF_AST", EVENT)


def scope_witness(tags=0, metadata=0, opaque=0, nodes=1):
    return marker("QUERYPROOF_STATE", [
        "H01", "SCOPE", *SCOPE[1:6], tags, metadata, opaque, nodes])


def event_witness(tags=0, metadata=0, opaque=0, nodes=1):
    return marker("QUERYPROOF_STATE", [
        "H01", "EVENT", *EVENT[1:5],
        tags, metadata, opaque, nodes, 0, 0, 0, 1])


def audit(lines):
    return audit_witness_stream(lines, {
        "scope_markers": 1,
        "recorded_rule_events": 1,
        "optimizer_suffix_case_counts": [{
            "case": "H01", "proved": True, "changed": True}] ,
    })


class StateGuardTests(unittest.TestCase):
    def test_zero_state_keeps_conditional_suffix(self):
        report = audit([scope_line(), scope_witness(),
                        event_line(), event_witness()])
        self.assertEqual(report["status"], "COMPLETE_JVM_WITNESSES")
        self.assertEqual(report["state_guarded_changed_suffix_count"], 1)

    def test_nonempty_metadata_blocks_case(self):
        report = audit([scope_line(), scope_witness(metadata=1),
                        event_line(), event_witness()])
        self.assertEqual(report["cases_blocked"], {"H01": ["metadata"]})
        self.assertEqual(report["state_guarded_changed_suffix_count"], 0)

    def test_mutable_tree_tag_blocks_case(self):
        report = audit([scope_line(), scope_witness(tags=2),
                        event_line(), event_witness()])
        self.assertEqual(report["cases_blocked"], {"H01": ["tags"]})

    def test_opaque_collection_blocks_case(self):
        report = audit([scope_line(), scope_witness(),
                        event_line(), event_witness(opaque=1)])
        self.assertEqual(report["cases_blocked"], {"H01": ["opaque"]})

    def test_missing_event_witness_is_invalid(self):
        report = audit([scope_line(), scope_witness(), event_line()])
        self.assertEqual(report["status"], "INVALID_WITNESS")
        self.assertEqual(report["state_guarded_changed_suffix_count"], 0)

    def test_orphan_witness_is_invalid(self):
        report = audit([scope_witness(), scope_line(),
                        scope_witness(), event_line(), event_witness()])
        self.assertIn("ORPHAN_WITNESS", report["errors"])

    def test_wrong_case_is_invalid(self):
        wrong = scope_witness().replace(
            base64.b64encode(b"H01").decode(), base64.b64encode(b"H02").decode(), 1)
        report = audit([scope_line(), wrong, event_line(), event_witness()])
        self.assertIn("WITNESS_IDENTITY_MISMATCH", report["errors"])

    def test_negative_counts_invalid(self):
        report = audit([scope_line(), scope_witness(tags=-1),
                        event_line(), event_witness()])
        self.assertIn("INVALID_JVM_COUNTER", report["errors"])

    def test_zero_nodes_invalid(self):
        report = audit([scope_line(), scope_witness(nodes=0),
                        event_line(), event_witness()])
        self.assertIn("INVALID_JVM_COUNTER", report["errors"])

    def test_missing_scope_markers_invalid(self):
        report = audit([event_line(), event_witness()])
        self.assertIn("SCOPE_WITNESS_DENOMINATOR_MISMATCH", report["errors"])

    def test_marker_error_invalid(self):
        report = audit([scope_line(), scope_witness(), event_line(),
                        event_witness(), "QUERYPROOF_SCOPE_ERROR\tBroken"])
        self.assertIn("JVM_MARKER_ERROR", report["errors"])

    def test_swapped_witness_invalid(self):
        report = audit([scope_line(), event_witness(),
                        event_line(), scope_witness()])
        self.assertEqual(report["status"], "INVALID_WITNESS")


if __name__ == "__main__":
    unittest.main()
