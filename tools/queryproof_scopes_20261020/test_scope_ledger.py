"""Adversarial scope-ledger controls with explicit Catalyst boundary snapshots."""
import base64
import json
import unittest

from scope_ledger import decode_scope, summarize_scope_stream


def marker(uid, phase, value, kind="BATCH", batch="Aggregate",
           case="H01", executor="Optimizer"):
    return {
        "type": "scope", "uid": uid, "phase": phase, "kind": kind,
        "batch": batch if kind == "BATCH" else "", "case": case,
        "executor": executor, "plan": [{"step": value}],
    }


def changed(before, after, yes=True, case="H01", executor="Optimizer",
            batch="Aggregate"):
    return {
        "type": "event", "case": case, "executor": executor, "batch": batch,
        "old": [{"step": before}], "new": [{"step": after}],
        "rule": "MockRule", "certified": yes,
    }


def complete(start, end, steps, executor="Optimizer"):
    return [
        marker(1, "BEGIN", start, "EXECUTOR", executor=executor),
        marker(2, "BEGIN", start, executor=executor),
        *steps,
        marker(2, "END", end, executor=executor),
        marker(1, "END", end, "EXECUTOR", executor=executor),
    ]


class ScopeLedgerTests(unittest.TestCase):
    def test_changed_batch_and_executor_certified(self):
        r = summarize_scope_stream(complete(
            0, 2, [changed(0, 1), changed(1, 2)]))
        self.assertEqual(r["status"], "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER")
        self.assertEqual(r["certified_changed_batches"], 1)
        self.assertEqual(r["certified_changed_executors"], 1)
        self.assertEqual(r["recorded_rule_events"], 2)

    def test_identity_batch_and_executor(self):
        r = summarize_scope_stream(complete(0, 0, []))
        self.assertEqual(r["exact_identity_batches"], 1)
        self.assertEqual(r["exact_identity_executors"], 1)

    def test_unknown_changed_rule_blocks_both_scopes(self):
        r = summarize_scope_stream(complete(0, 1, [changed(0, 1, False)]))
        self.assertEqual(r["certified_changed_batches"], 0)
        self.assertEqual(r["certified_changed_executors"], 0)
        self.assertEqual(r["by_reason"]["BATCH:UNKNOWN_RULE"], 1)

    def test_unknown_internal_reversal_is_exact_identity(self):
        r = summarize_scope_stream(complete(
            0, 0, [changed(0, 1, False), changed(1, 0, False)]))
        self.assertEqual(r["exact_identity_batches"], 1)
        self.assertEqual(r["exact_identity_executors"], 1)

    def test_initial_gap_never_proves_change(self):
        r = summarize_scope_stream(complete(0, 2, [changed(1, 2)]))
        self.assertEqual(r["certified_changed_batches"], 0)
        self.assertEqual(r["by_reason"]["BATCH:ENDPOINT_GAP"], 1)

    def test_final_gap_never_proves_change(self):
        r = summarize_scope_stream(complete(0, 3, [changed(0, 2)]))
        self.assertEqual(r["certified_changed_batches"], 0)

    def test_middle_gap_never_proves_change(self):
        r = summarize_scope_stream(complete(
            0, 3, [changed(0, 1), changed(2, 3)]))
        self.assertEqual(r["by_reason"]["BATCH:INTERMEDIATE_GAP"], 1)

    def test_changed_batch_without_recorded_rule_is_unknown(self):
        r = summarize_scope_stream(complete(0, 1, []))
        self.assertEqual(r["by_reason"]["BATCH:UNRECORDED_DELTA"], 1)

    def test_event_outside_batch_is_invalid(self):
        r = summarize_scope_stream([changed(0, 1)])
        self.assertEqual(r["status"], "INVALID_TRACE")
        self.assertIn("EVENT_OUTSIDE_BATCH", r["errors"])

    def test_scope_end_mismatch_is_invalid(self):
        seq = complete(0, 1, [changed(0, 1)])
        seq[-2]["uid"] = 55
        r = summarize_scope_stream(seq)
        self.assertEqual(r["status"], "INVALID_TRACE")
        self.assertIn("SCOPE_END_MISMATCH", r["errors"])

    def test_missing_scope_end_is_invalid(self):
        r = summarize_scope_stream(complete(0, 1, [changed(0, 1)])[:-1])
        self.assertEqual(r["status"], "INVALID_TRACE")
        self.assertIn("UNCLOSED_SCOPE", r["errors"])

    def test_case_switch_inside_scope_is_invalid(self):
        seq = complete(0, 1, [changed(0, 1)])
        seq[2]["case"] = "H02"
        r = summarize_scope_stream(seq)
        self.assertIn("EVENT_SCOPE_MISMATCH", r["errors"])

    def test_nested_executor_is_not_counted_as_parent_batch_rewrite(self):
        seq = [
            marker(1, "BEGIN", 0, "EXECUTOR"),
            marker(2, "BEGIN", 0),
            marker(3, "BEGIN", 10, "EXECUTOR", executor="SubAnalyzer"),
            marker(4, "BEGIN", 10, executor="SubAnalyzer"),
            changed(10, 11, executor="SubAnalyzer"),
            marker(4, "END", 11, executor="SubAnalyzer"),
            marker(3, "END", 11, "EXECUTOR", executor="SubAnalyzer"),
            changed(0, 1),
            marker(2, "END", 1),
            marker(1, "END", 1, "EXECUTOR"),
        ]
        r = summarize_scope_stream(seq)
        self.assertEqual(r["certified_changed_batches"], 2)
        self.assertEqual(r["certified_changed_executors"], 2)

    def test_two_batches_compose_only_on_identical_boundary(self):
        seq = [
            marker(1, "BEGIN", 0, "EXECUTOR"),
            marker(2, "BEGIN", 0),
            changed(0, 1),
            marker(2, "END", 1),
            marker(3, "BEGIN", 1, batch="Cleanup"),
            changed(1, 2, batch="Cleanup"),
            marker(3, "END", 2, batch="Cleanup"),
            marker(1, "END", 2, "EXECUTOR"),
        ]
        r = summarize_scope_stream(seq)
        self.assertEqual(r["certified_changed_batches"], 2)
        self.assertEqual(r["certified_changed_executors"], 1)

    def test_executor_boundary_gap_fails_even_if_batches_each_pass(self):
        seq = [
            marker(1, "BEGIN", 0, "EXECUTOR"),
            marker(2, "BEGIN", 0),
            changed(0, 1),
            marker(2, "END", 1),
            marker(3, "BEGIN", 8, batch="Cleanup"),
            changed(8, 9, batch="Cleanup"),
            marker(3, "END", 9, batch="Cleanup"),
            marker(1, "END", 9, "EXECUTOR"),
        ]
        r = summarize_scope_stream(seq)
        self.assertEqual(r["certified_changed_batches"], 2)
        self.assertEqual(r["certified_changed_executors"], 0)
        self.assertEqual(r["by_reason"]["EXECUTOR:INTERMEDIATE_GAP"], 1)

    def test_scope_base64_decoder(self):
        raw = ["H01", "BATCH", "Optimizer", "Aggregate", "12", "BEGIN",
               json.dumps([{"node": "Aggregate"}])]
        line = "QUERYPROOF_SCOPE\t" + "\t".join(
            base64.b64encode(value.encode()).decode() for value in raw)
        r = decode_scope(line)
        self.assertEqual(r["uid"], 12)
        self.assertEqual(r["plan"], [{"node": "Aggregate"}])

    def test_invalid_marker_fails_closed(self):
        bad = "QUERYPROOF_SCOPE\t" + "\t".join(["%%%%"] * 7)
        with self.assertRaises(ValueError):
            decode_scope(bad)


if __name__ == "__main__":
    unittest.main()
