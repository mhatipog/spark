"""Adversarial controls for exact plan-state chain composition."""
import unittest

from chain_connectivity import summarize_event_chain


def transition(case, before, after, rule="ColumnPruning", yes=True):
    return {"case": case, "old": [{"plan": before}],
            "new": [{"plan": after}], "rule": rule, "certified": yes}


class ChainConnectivityTests(unittest.TestCase):
    def test_two_exact_consecutive_accepted_edges_compose(self):
        result = summarize_event_chain([
            transition("q1", 0, 1),
            transition("q1", 1, 2, "RemoveRepetitionFromGroupExpressions"),
        ])
        self.assertEqual(result["multi_edge_segments"], 1)
        self.assertEqual(result["adjacent_certified_pairs"], 1)
        self.assertEqual(result["max_certified_segment_length"], 2)
        self.assertEqual(result["fully_connected_certified_recorded_case_traces"], 1)

    def test_same_case_not_enough_when_ast_snapshot_mismatches(self):
        result = summarize_event_chain([
            transition("q1", 0, 1), transition("q1", 3, 4)])
        self.assertEqual(result["adjacent_certified_pairs"], 0)
        self.assertEqual(result["multi_edge_segments"], 0)
        self.assertEqual(result["max_certified_segment_length"], 1)

    def test_unknown_middle_breaks_chain_despite_ast_equality(self):
        r = summarize_event_chain([
            transition("q1", 0, 1), transition("q1", 1, 2, yes=False),
            transition("q1", 2, 3)])
        self.assertEqual(r["adjacent_exact_ast_pairs_all_rules"], 2)
        self.assertEqual(r["adjacent_certified_pairs"], 0)
        self.assertEqual(r["fully_connected_certified_recorded_case_traces"], 0)

    def test_interleaved_cases_have_independent_adjacency(self):
        r = summarize_event_chain([
            transition("q1", 0, 1), transition("q2", 1, 2),
            transition("q1", 1, 3), transition("q2", 2, 4)])
        self.assertEqual(r["adjacent_certified_pairs"], 2)
        self.assertEqual(r["cases_with_two_edge_segment"], 2)

    def test_equal_snapshot_different_executor_is_not_a_chain(self):
        a = transition("q1", 0, 1)
        b = transition("q1", 1, 2)
        a["executor"] = "Optimizer"
        b["executor"] = "Analyzer"
        r = summarize_event_chain([a, b])
        self.assertEqual(r["multi_edge_segments"], 0)

    def test_equal_snapshot_different_batch_is_not_a_chain(self):
        a = transition("q1", 0, 1)
        b = transition("q1", 1, 2)
        a.update({"executor": "Optimizer", "batch": "A"})
        b.update({"executor": "Optimizer", "batch": "B"})
        r = summarize_event_chain([a, b])
        self.assertEqual(r["multi_edge_segments"], 0)

    def test_same_executor_and_batch_allows_exact_chain(self):
        a = transition("q1", 0, 1)
        b = transition("q1", 1, 2)
        a.update({"executor": "Optimizer", "batch": "OperatorOpt"})
        b.update({"executor": "Optimizer", "batch": "OperatorOpt"})
        r = summarize_event_chain([a, b])
        self.assertEqual(r["multi_edge_segments"], 1)

    def test_unknown_single_event_never_proves_whole_trace(self):
        r = summarize_event_chain([transition("q1", 0, 1, yes=False)])
        self.assertEqual(r["fully_connected_certified_recorded_case_traces"], 0)
        self.assertEqual(r["locally_certified_events"], 0)

    def test_single_accepted_event_is_not_multistep(self):
        r = summarize_event_chain([transition("q1", 0, 1)])
        self.assertEqual(r["fully_connected_certified_recorded_case_traces"], 1)
        self.assertEqual(r["multi_edge_segments"], 0)

    def test_distinct_rules_same_before_not_a_chain(self):
        r = summarize_event_chain([
            transition("q1", 0, 1, "ColumnPruning"),
            transition("q1", 0, 2, "CollapseProject")])
        self.assertEqual(r["multi_edge_segments"], 0)

    def test_exact_plan_equality_checks_every_metadata_field(self):
        e = transition("q1", 0, 1)
        e["new"][0]["metadata"] = {"nullable": False}
        r = summarize_event_chain([e, transition("q1", 1, 2)])
        self.assertEqual(r["adjacent_certified_pairs"], 0)

    def test_maximal_segments_not_double_counted(self):
        r = summarize_event_chain([
            transition("q1", 0, 1), transition("q1", 1, 2),
            transition("q1", 2, 3), transition("q1", 3, 4, yes=False),
            transition("q1", 4, 5), transition("q1", 5, 6)])
        self.assertEqual(r["max_certified_segment_length"], 3)
        self.assertEqual(r["multi_edge_segments"], 2)
        self.assertEqual(r["adjacent_certified_pairs"], 3)

    def test_cross_scope_snapshot_match_is_classified_but_not_claimed_as_chain(self):
        a = transition("q1", 0, 1, "RemoveRepetitionFromGroupExpressions")
        b = transition("q1", 1, 2, "ColumnPruning")
        a.update({"executor": "Analyzer", "batch": "Cleanup"})
        b.update({"executor": "Optimizer", "batch": "OperatorOpt"})
        r = summarize_event_chain([a, b])
        self.assertEqual(r["exact_snapshot_certified_pairs_any_scope"], 1)
        self.assertEqual(r["adjacent_certified_pairs"], 0)
        self.assertEqual(
            r["exact_snapshot_certified_boundary_types"][0]["scope_class"],
            "different_executor")

    def test_same_executor_new_batch_distinguished(self):
        a = transition("q1", 0, 1)
        b = transition("q1", 1, 2)
        a.update({"executor": "Optimizer", "batch": "A"})
        b.update({"executor": "Optimizer", "batch": "B"})
        r = summarize_event_chain([a, b])
        self.assertEqual(
            r["exact_snapshot_certified_boundary_types"][0]["scope_class"],
            "same_executor_different_batch")

    def test_no_events_is_not_a_proof(self):
        r = summarize_event_chain([])
        self.assertEqual(r["recorded_events"], 0)
        self.assertEqual(r["fully_connected_certified_recorded_case_traces"], 0)


if __name__ == "__main__":
    unittest.main()
