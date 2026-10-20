"""Fail-closed V7 adversarial regression controls."""
import unittest
from scope_ledger_v7 import _chain_proof, summarize_scope_stream

P = [{"class": "P"}]
Q = [{"class": "Q"}]
def step(old, new, certified):
    return {"old": old, "new": new, "certified": certified,
            "rule": "synthetic", "executor": "opt", "batch": "b", "case": "T"}
def scope(kind, phase, plan, uid):
    return {"type": "scope", "case": "T", "kind": kind, "phase": phase,
            "plan": plan, "uid": uid, "executor": "opt", "batch": "b"}
class StrictChainTests(unittest.TestCase):
    def test_uncertified_cycle_rejected(self):
        self.assertEqual(_chain_proof(P,P,[step(P,Q,False),step(Q,P,False)])[1],"UNKNOWN_RULE")
    def test_certified_cycle_accepted(self):
        self.assertTrue(_chain_proof(P,P,[step(P,Q,True),step(Q,P,True)])[0])
    def test_missing_delta_rejected(self):
        self.assertFalse(_chain_proof(P,Q,[])[0])
    def test_disconnected_chain_rejected(self):
        self.assertEqual(_chain_proof(P,Q,[step(Q,P,True)])[1],"ENDPOINT_GAP")
    def test_identity_executor_with_unknown_child_rejected(self):
        stream=[scope("EXECUTOR","BEGIN",P,1),scope("BATCH","BEGIN",P,2),
                dict(step(P,Q,False),type="event"),dict(step(Q,P,False),type="event"),
                scope("BATCH","END",P,2),scope("EXECUTOR","END",P,1)]
        result=summarize_scope_stream(stream)
        self.assertEqual(result["status"],"RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER")
        self.assertEqual(result["by_reason"].get("BATCH:UNKNOWN_RULE"),1)
        self.assertEqual(result["by_reason"].get("EXECUTOR:UNKNOWN_ENCLOSED_BATCH"),1)
        inventory=result["unknown_rule_case_inventory"]
        self.assertEqual(sum(r["count"] for r in inventory if r["case"]=="T"),2)
        self.assertEqual({r["rule"] for r in inventory},{"synthetic"})
if __name__=="__main__": unittest.main()
