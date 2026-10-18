"""Exact abstract global-aggregate cardinality guard: unit regression."""
import unittest
from cardinality_smt import derive,TRUE,FALSE,UNKNOWN

class CardinalityProof(unittest.TestCase):
    def test_unbounded_counterexample_and_exact_guard(self):
        r=derive()
        self.assertTrue(r['passed'])
        self.assertEqual(r['checks']['unconditional']['smt_result'],'SAT')
        self.assertEqual(r['checks']['synthesized_exact']['smt_result'],'UNSAT')
        self.assertEqual(r['checks']['nonempty_source']['smt_result'],'UNSAT')
        self.assertEqual(r['checks']['scalar_always_null']['smt_result'],'UNSAT')
    def test_only_empty_nonnull_values_violate(self):
        r=derive()
        violations=[(x['n'],x['v']) for x in r['finite_replay'] if not x['safe']]
        self.assertEqual(sorted(violations),[(0,FALSE),(0,TRUE)])
    def test_nonnegative_not_sufficient(self):
        r=derive()
        self.assertEqual(r['checks']['nonnegative_source']['smt_result'],'SAT')
        w=r['checks']['nonnegative_source']['witness']
        self.assertEqual(w['n'],0)
        self.assertIn(w['v'],(TRUE,FALSE))

if __name__=='__main__':unittest.main()
