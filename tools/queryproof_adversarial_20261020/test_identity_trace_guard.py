"""Adversarial protocol tests: no witness, reordering or forgery-by-context.

Token content is trusted ONLY when minted by the in-process observer; these
tests exercise fail-closed transcript handling, not cryptographic provenance.
"""
import base64
import json
import unittest

from identity_trace_guard import (
    SUPPORTED_RDD, audit_trace,
)


def marker(name, fields):
    return name + "\t" + "\t".join(
        base64.b64encode(str(f).encode()).decode() for f in fields)


def event(old_token, new_token, cls=SUPPORTED_RDD, old_leaf=None,
          corrupt_input_context=False):
    leaf = [{"class": cls, "num-children": 0}]
    core = ["control", "optimizer", "batch", "rule", "1"]
    prefix = ["control", "EVENT", *core[1:]]
    base = marker("QUERYPROOF_AST", [*core, json.dumps(leaf), json.dumps(leaf)])
    state = marker("QUERYPROOF_STATE", [*prefix, *([0, 0, 0, 1] * 2)])
    frame = marker("QUERYPROOF_FRAME", [*prefix, *(["", "", "output", "false"] * 2)])
    left = old_leaf if old_leaf is not None else cls + "|" + old_token
    if corrupt_input_context:
        prefix[0] = "some-other-case"
    input_line = marker("QUERYPROOF_INPUT", [*prefix, left, cls + "|" + new_token])
    return [base, state, frame, input_line]


class AdversarialTraceProtocol(unittest.TestCase):
    def audit(self, lines):
        return audit_trace(lines)

    def test_genuine_swap_fails_closed(self):
        result = self.audit(event("rdd#1", "rdd#2"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["records"][0]["reason"], "INPUT_REFERENCE_CHANGED")

    def test_stable_input_is_admissible_but_not_a_semantics_proof(self):
        result = self.audit(event("rdd#1", "rdd#1"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["records"][0]["status"], "PRESERVED")

    def test_missing_last_input_marker_fails(self):
        self.assertEqual(self.audit(event("rdd#1", "rdd#1")[:-1])["status"], "INVALID")

    def test_reordered_state_frame_input_fails(self):
        lines = event("rdd#1", "rdd#1")
        lines[1], lines[2] = lines[2], lines[1]
        self.assertEqual(self.audit(lines)["status"], "INVALID")

    def test_wrong_case_binding_fails(self):
        self.assertEqual(self.audit(event("rdd#1", "rdd#1",
                                          corrupt_input_context=True))["status"], "INVALID")

    def test_unsupported_leaf_fails_closed(self):
        cls = "org.apache.spark.sql.catalyst.plans.logical.LocalRelation"
        result = self.audit(event("UNKNOWN", "UNKNOWN", cls=cls))
        self.assertEqual(result["records"][0]["reason"], "UNKNOWN_RELATION_SEMANTICS")

    def test_missing_leaf_token_fails(self):
        result = self.audit(event("rdd#1", "rdd#1", old_leaf=""))
        self.assertEqual(result["records"][0]["reason"], "INCOMPLETE_LEAF_BINDING")

    def test_malformed_token_fails(self):
        result = self.audit(event("rdd#oops", "rdd#oops"))
        self.assertEqual(result["records"][0]["reason"], "INVALID_RDD_REFERENCE")

    def test_duplicate_input_marker_is_invalid(self):
        lines = event("rdd#1", "rdd#1")
        self.assertEqual(self.audit(lines + [lines[-1]])["status"], "INVALID")

    def test_corrupted_base64_is_invalid(self):
        lines = event("rdd#1", "rdd#1")
        lines[-1] += "!INVALID"
        self.assertEqual(self.audit(lines)["status"], "INVALID")


if __name__ == "__main__":
    unittest.main()
