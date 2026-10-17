import random
import unittest
from topk_certificate import local_topq, merge_topq, witness


class TopKAlgebraTests(unittest.TestCase):
    def test_partition_monoid_on_frozen_random_unique_key_sequences(self):
        rng = random.Random(20261017)
        for trial in range(75):
            count = rng.randint(3, 45)
            rows = [(i, 'n'+str(i), 100*i+17) for i in rng.sample(range(200),count)]
            rng.shuffle(rows)
            pieces = [[], [], [], []]
            for row in rows:
                pieces[rng.randrange(4)].append(row)
            for q in (0, 1, 2, 5, count, count+3):
                expected = local_topq(rows,q)
                observed = merge_topq([local_topq(p,q) for p in pieces],q)
                self.assertEqual(observed,expected,(trial,q))

    def test_membership_different_but_already_sorted_catches_pr_class(self):
        self.assertEqual(witness(['c','d','f'], ['b','d','f'])['verdict'], 'MEMBERSHIP_VIOLATION')
        self.assertEqual(witness(['a','b','e'], ['a','b','e'])['verdict'], 'NO_VIOLATION_OBSERVED')

    def test_missing_unique_identity_is_unknown(self):
        self.assertEqual(witness(['same','same'], ['same','other'])['verdict'],'UNKNOWN')

    def test_offset_slice_of_global_certificate(self):
        rows = [(1,'a',30),(2,'b',20),(3,'c',40),(4,'d',60),(5,'e',10),(6,'f',50)]
        partitions = [rows[:2],rows[2:4],rows[4:]]
        full = merge_topq([local_topq(p,6) for p in partitions],6)
        self.assertEqual([x[3] for x in full[:3]],['d','f','c'])
        self.assertEqual([x[3] for x in full[3:6]],['a','b','e'])


if __name__=='__main__':
    unittest.main()
