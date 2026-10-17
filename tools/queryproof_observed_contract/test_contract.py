"""Pure-stdlib tests independent of Spark and the known SQL reproducer."""
import unittest
from contract import audit


def k(index=0, direction='ASC', nulls='FIRST'):
    return [{'index': index, 'direction': direction, 'nulls': nulls}]


class ContractTests(unittest.TestCase):
    def test_ascending_violation(self):
        self.assertEqual(audit([(1,), (3,), (2,)], k())['verdict'], 'CONTRACT_VIOLATED')

    def test_descending(self):
        self.assertEqual(audit([(3,), (2,), (1,)], k(direction='DESC'))['verdict'], 'NO_VIOLATION_OBSERVED')
        self.assertEqual(audit([(3,), (1,), (2,)], k(direction='DESC'))['verdict'], 'CONTRACT_VIOLATED')

    def test_ties_do_not_impose_stable_order(self):
        self.assertEqual(audit([(1, 100), (1, -2), (2, 4)], k())['verdict'], 'NO_VIOLATION_OBSERVED')

    def test_second_key(self):
        kk = k() + k(1, 'DESC')
        self.assertEqual(audit([(1, 9), (1, 8), (2, 3)], kk)['verdict'], 'NO_VIOLATION_OBSERVED')
        self.assertEqual(audit([(1, 8), (1, 9), (2, 3)], kk)['verdict'], 'CONTRACT_VIOLATED')

    def test_null_policy_independent_of_direction(self):
        for direction in ('ASC', 'DESC'):
            self.assertEqual(audit([(None,), (2,)], k(direction=direction, nulls='FIRST'))['verdict'], 'NO_VIOLATION_OBSERVED')
            self.assertEqual(audit([(None,), (2,)], k(direction=direction, nulls='LAST'))['verdict'], 'CONTRACT_VIOLATED')
            self.assertEqual(audit([(2,), (None,)], k(direction=direction, nulls='LAST'))['verdict'], 'NO_VIOLATION_OBSERVED')

    def test_unknown_instead_of_unsound_assertion(self):
        self.assertEqual(audit([(1,), (2,)], [])['verdict'], 'UNKNOWN')
        self.assertEqual(audit([(1.2,), (3.4,)], k())['verdict'], 'UNKNOWN')
        self.assertEqual(audit([(1,), (2,)], k(1))['verdict'], 'UNKNOWN')

    def test_zero_one_row_do_not_prove_correctness(self):
        for rows in ([], [(3,)]):
            self.assertEqual(audit(rows, k())['verdict'], 'NO_VIOLATION_OBSERVED')

    def test_detects_all_adjacent_swap_mutants_of_strictly_sorted_keys(self):
        base = [(i,) for i in range(8)]
        for i in range(len(base)-1):
            rows = base[:]
            rows[i], rows[i+1] = rows[i+1], rows[i]
            self.assertEqual(audit(iter(rows), k())['verdict'], 'CONTRACT_VIOLATED')


if __name__ == '__main__':
    unittest.main()
