"""Exhaustive independent 3VL truth-table tests of observation pullbacks."""
import itertools
import unittest
from lattice import (D,PARTITIONS,LABELS,OPERATORS,apply,block,requirement,
                     follow_path,siblings,insert_at,signature_witnesses,
                     enumerate_substitution_power)


def run_path(path,leaf,side_values):
    value=leaf
    for (op,pos),s in reversed(list(zip(path,side_values))):
        value=apply(op,insert_at(op,pos,value,s))
    return value


class TestObserverLattice(unittest.TestCase):
    def test_all_five_partitions(self):
        self.assertEqual(len(PARTITIONS),5)
        self.assertEqual(set(PARTITIONS),set(LABELS.values()))

    def test_exact_signature_witness_and_coarseness_all_operators(self):
        for name in OPERATORS:
            for pos in range(OPERATORS[name][0]):
                for observer in PARTITIONS:
                    required=requirement(observer,name,pos)
                    self.assertIn(required,PARTITIONS)
                    witnesses=signature_witnesses(observer,name,pos)
                    for item in witnesses:
                        if item['same_required_block']:
                            self.assertFalse(item['witnesses'],(name,pos,observer,item))
                        else:
                            # Every separated pair has a concrete sibling counterexample.
                            self.assertTrue(item['witnesses'],(name,pos,observer,item))

    def test_known_minimal_demands(self):
        self.assertEqual(follow_path([('NOT',0)]),LABELS['FALSE_ONLY'])
        self.assertEqual(follow_path([('IS_NULL',0)]),LABELS['NULL_ONLY'])
        self.assertEqual(follow_path([('IS_NOT_NULL',0)]),LABELS['NULL_ONLY'])
        self.assertEqual(follow_path([('IS_TRUE',0)]),LABELS['TRUE_ONLY'])
        self.assertEqual(follow_path([('IS_FALSE',0)]),LABELS['FALSE_ONLY'])
        self.assertEqual(follow_path([('AND',0)]),LABELS['TRUE_ONLY'])
        self.assertEqual(follow_path([('OR',1)]),LABELS['TRUE_ONLY'])
        self.assertEqual(follow_path([('AND',0)],LABELS['NULL_ONLY']),LABELS['FULL_3VL'])
        self.assertEqual(follow_path([('EQUAL_NULL_SAFE',0)]),LABELS['FULL_3VL'])

    def test_nested_paths_sound_and_minimal_exhaustively(self):
        paths=[
            [('NOT',0),('OR',0)],
            [('AND',1),('NOT',0)],
            [('IS_NULL',0),('OR',0)],
            [('OR',1),('NOT',0),('AND',0)],
            [('NOT',0),('NOT',0),('IS_NULL',0)],
            [('OR',0),('AND',1),('NOT',0)],
        ]
        for path in paths:
            choices=list(itertools.product(*(siblings(op,i) for op,i in path)))
            for observer in PARTITIONS:
                requirement_at_leaf=follow_path(path,observer)
                for a,b in itertools.product(D,repeat=2):
                    can_distinguish=any(
                        block(observer,run_path(path,a,chosen)) !=
                        block(observer,run_path(path,b,chosen)) for chosen in choices)
                    equal_req=(block(requirement_at_leaf,a)==
                               block(requirement_at_leaf,b))
                    self.assertEqual(equal_req,not can_distinguish,
                         (path,observer,a,b,requirement_at_leaf))

    def test_exact_enumeration_of_unary_maps(self):
        counts=enumerate_substitution_power()
        self.assertEqual(counts['ANY']['safe_pairs_of_729'],729)
        self.assertEqual(counts['FULL_3VL']['safe_pairs_of_729'],27)
        for label in ('TRUE_ONLY','FALSE_ONLY','NULL_ONLY'):
            self.assertEqual(counts[label]['safe_pairs_of_729'],125)
            self.assertEqual(counts[label]['additional_vs_full'],98)


if __name__=='__main__':
    unittest.main()
