"""V7 strict ledger adversarial unit tests. Run: python -m unittest discover -s tools/queryproof_v7_strict_20261020 -p 'test_*.py'"""
import unittest
from scope_ledger_v7 import _chain_proof, summarize_scope_stream

P=[{"class":"P","num-children":0}]
Q=[{"class":"Q","num-children":0}]
def scope(kind,phase,plan,uid=1,batch=""):
    return dict(type="scope",case="T01",kind=kind,phase=phase,plan=plan,uid=uid,executor="optimizer",batch=batch)
def event(old,new,certified):
    return dict(type="event",case="T01",executor="optimizer",batch="b",old=old,new=new,rule="synthetic",certified=certified)
class StrictChainTests(unittest.TestCase):
    def test_uncertified_cycle_rejected(self):
        self.assertEqual(_chain_proof(P,P,[event(P,Q,False),event(Q,P,False)])[1],"UNKNOWN_RULE")
    def test_certified_cycle_accepted(self):
        self.assertTrue(_chain_proof(P,P,[event(P,Q,True),event(Q,P,True)])[0])
    def test_missing_delta_rejected(self):
        self.assertFalse(_chain_proof(P,Q,[])[0])
    def test_disconnected_chain_rejected(self):
        self.assertEqual(_chain_proof(P,Q,[event(Q,P,True)])[1],"ENDPOINT_GAP")
    def test_identity_executor_with_unknown_child_rejected(self):
        stream=[scope("EXECUTOR","BEGIN",P,1),scope("BATCH","BEGIN",P,2,"b"),
                event(P,Q,False),event(Q,P,False),scope("BATCH","END",P,2,"b"),
                scope("EXECUTOR","END",P,1)]
        result=summarize_scope_stream(stream)
        self.assertEqual(result["status"],"RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER")
        self.assertEqual(result["certified_changed_optimizer_suffixes"],0)
        self.assertIn("EXECUTOR:UNKNOWN_ENCLOSED_BATCH",result["by_reason"])
if __name__=="__main__": unittest.main()
