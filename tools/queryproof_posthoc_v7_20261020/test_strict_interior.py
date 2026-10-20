"""Posthoc V7 identity-bypass falsification, with an independent V6 control.

No fabricated case is counted as a real Spark observation. All structural
claims here concern a synthetic in-memory RuleExecutor trace only.
"""
import sys
import unittest
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1]
for folder in ("queryproof_posthoc_v6_20261020",
               "queryproof_posthoc_v7_20261020"):
    sys.path.insert(0, str(TOOLS / folder))

from scope_ledger_v6 import summarize_scope_stream as prior
from scope_ledger_v7_all_steps import summarize_scope_stream as strict
from scope_ledger_v6 import _chain_proof as prior_chain
from scope_ledger_v7_all_steps import _chain_proof as strict_chain

CASE = "SYNTHETIC_V7"
EXEC = "BaseSessionStateBuilder$$anon$2"
A = [{"class": "A", "num-children": 0}]
B = [{"class": "B", "num-children": 0}]
C = [{"class": "C", "num-children": 0}]


def scope(kind, phase, batch, uid, plan):
    return dict(type="scope", kind=kind, phase=phase, batch=batch,
                uid=uid, plan=plan, case=CASE, executor=EXEC)


def event(batch, old, new, *, certified, rule):
    return dict(type="event", case=CASE, executor=EXEC, batch=batch,
                old=old, new=new, certified=certified, rule=rule)


def trace(*, hidden_unknown_cycle=True, final_certified=True,
          cycle_before_finish=False):
    steps = [scope("EXECUTOR", "BEGIN", "", 1, A)]
    if cycle_before_finish:
        steps += [
            scope("BATCH", "BEGIN", "Analyzer Cycle", 2, A),
            event("Analyzer Cycle", A, B, certified=False, rule="Broken1"),
            event("Analyzer Cycle", B, A, certified=False, rule="Broken2"),
            scope("BATCH", "END", "Analyzer Cycle", 2, A)]
    steps += [
        scope("BATCH", "BEGIN", "Finish Analysis", 3, A),
        scope("BATCH", "END", "Finish Analysis", 3, A)]
    if hidden_unknown_cycle:
        steps += [
            scope("BATCH", "BEGIN", "Cycle", 4, A),
            event("Cycle", A, B, certified=False, rule="Unsupported1"),
            event("Cycle", B, A, certified=False, rule="Unsupported2"),
            scope("BATCH", "END", "Cycle", 4, A)]
    steps += [
        scope("BATCH", "BEGIN", "Final", 5, A),
        event("Final", A, C, certified=final_certified, rule="ProofRule"),
        scope("BATCH", "END", "Final", 5, C),
        scope("EXECUTOR", "END", "", 1, C)]
    return steps


class StrictInteriorProvenance(unittest.TestCase):
    def test_snapshot_identity_bypass_is_real_in_v6(self):
        old = prior_chain(A, A, [
            {"old": A, "new": B, "certified": False},
            {"old": B, "new": A, "certified": False}])
        new = strict_chain(A, A, [
            {"old": A, "new": B, "certified": False},
            {"old": B, "new": A, "certified": False}])
        self.assertEqual(old, (True, "EXACT_IDENTITY"))
        self.assertEqual(new, (False, "UNSUPPORTED_INTERIOR_RULE"))

    def test_changed_postcut_suffix_legacy_passes_but_strict_rejects(self):
        old = prior(trace())
        new = strict(trace())
        self.assertEqual(old["status"], "RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER")
        self.assertTrue(old["optimizer_suffix_case_counts"][0]["proved"])
        self.assertFalse(new["optimizer_suffix_case_counts"][0]["proved"])
        self.assertEqual(len(new["postcut_identity_bypass_events"]), 1)
        self.assertEqual(new["postcut_identity_bypass_events"][0][
            "uncertified_rule_steps"], 2)

    def test_source_cutpoint_does_not_punish_precut_unknown_cycle(self):
        new = strict(trace(hidden_unknown_cycle=False,
                           cycle_before_finish=True))
        self.assertTrue(new["optimizer_suffix_case_counts"][0]["proved"])
        # The entire executor is NOT proved; only post-Finish-Analysis suffix.
        self.assertEqual(new["certified_changed_executors"], 0)
        self.assertEqual(new["postcut_identity_bypass_events"], [])

    def test_clean_effective_rule_is_accepted(self):
        out = strict(trace(hidden_unknown_cycle=False))
        self.assertTrue(out["optimizer_suffix_case_counts"][0]["proved"])
        self.assertEqual(out["postcut_identity_bypass_events"], [])

    def test_wrong_or_missing_rule_proof_fails_closed(self):
        out = strict(trace(hidden_unknown_cycle=False, final_certified=False))
        self.assertFalse(out["optimizer_suffix_case_counts"][0]["proved"])


if __name__ == "__main__":
    unittest.main()
