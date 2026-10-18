"""Unbounded nullable integer Z3 proofs checked against independent finite data."""
import unittest

from test_context_proof import event
from context_proof import certificate_for_transition
from smt_validator import prove_pair,certify
from sqlite_check import independently_verify

class SmtRealRewriteTests(unittest.TestCase):
    def test_unsafe_nested_not_yields_unbounded_sat(self):
        local=certificate_for_transition(event(True))
        self.assertEqual(local['status'],'COUNTEREXAMPLE')
        z=certify(local)
        self.assertEqual(z['status'],'COUNTEREXAMPLE')
        self.assertEqual(z['root']['solver_status'],'SAT')
        self.assertEqual(z['guard'],'BLOCK')
        self.assertEqual(z['safe_observation_partitions'],['ANY','TRUE_ONLY'])
        self.assertEqual(sum(v is None for v in z['root']['witness'].values()),1)
        self.assertTrue(independently_verify(local)['verified'])

    def test_safe_positive_join_has_unbounded_unsat(self):
        local=certificate_for_transition(event(False))
        z=certify(local)
        self.assertEqual(z['status'],'PROVED_WITHIN_FRAGMENT')
        self.assertEqual(z['root']['solver_status'],'UNSAT')
        self.assertEqual(z['guard'],'ALLOW')
        self.assertEqual(z['safe_observation_partitions'],['ANY','TRUE_ONLY'])

    def test_full_3vl_is_distinguishable_even_when_true_set_matches(self):
        local=certificate_for_transition(event(False))
        z=certify(local)
        self.assertEqual(z['root']['solver_status'],'UNSAT')
        self.assertEqual(z['partition_results']['FULL_3VL']['solver_status'],'SAT')
        self.assertEqual(z['partition_results']['FALSE_ONLY']['solver_status'],'SAT')
        self.assertEqual(z['partition_results']['NULL_ONLY']['solver_status'],'SAT')


if __name__=='__main__':unittest.main()
