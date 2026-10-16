"""Unit tests of SQL grammar, independent row oracle and greedy reductions."""
import unittest
from dataclasses import replace
from generate import Case,sql,reference,diagnose,complexity,reductions,MANIFEST
BASE=Case(shape="cte",join="LEFT",left_start=0,right_start=0,left_n=3,right_n=2,
          left_partitions=2,right_partitions=2,modulus=1,offset=1,limit=1)
class TestContracts(unittest.TestCase):
    def test_calibration_reference(self):
        self.assertEqual(reference(BASE),[[1,0,0],[1,0,1]])
    def test_reverse_detected_independent_of_expected_rows(self):
        self.assertTrue(diagnose(BASE,list(reversed(reference(BASE))))["order_violation"])
    def test_correct_not_flagged(self):
        self.assertFalse(diagnose(BASE,reference(BASE))["order_violation"])
    def test_bag_mismatch_not_called_sort_bug(self):
        d=diagnose(BASE,[[1,0,1],[1,0,42]])
        self.assertTrue(d["bag_violation"])
        self.assertFalse(d["order_violation"])
    def test_left_join_unmatched_nullable(self):
        x=replace(BASE,left_start=8,right_start=0,modulus=3,offset=0,limit=1)
        self.assertEqual(reference(x),[[8,2,None]])
    def test_inner_join_unmatched(self):
        x=replace(BASE,left_start=9,right_start=0,modulus=5,offset=0,limit=1,join="INNER")
        self.assertEqual(reference(x),[])
    def test_no_negative_or_invalid_params(self):
        self.assertFalse(replace(BASE,offset=100).valid())
        self.assertTrue(replace(BASE,limit=20).valid())  # SQL LIMIT can exceed remaining rows.
    def test_reducers_strictly_shrink(self):
        x=replace(BASE,left_n=5,right_n=4)
        self.assertTrue(all(complexity(y)<complexity(x) for y in reductions(x)))
    def test_shape_does_not_change_reference(self):
        y=replace(BASE,shape="nested")
        self.assertEqual(reference(BASE),reference(y))
        self.assertIn("WITH top",sql(BASE))
        self.assertNotIn("WITH top",sql(y))
    def test_query_explicit_final_order(self):
        self.assertTrue(sql(BASE).endswith("ORDER BY top.id ASC,rhs.bid ASC"))
    def test_prespecified_cases_are_legal(self):
        for case in MANIFEST["cases"]:
            x=Case(**{k:v for k,v in case.items() if k not in ("case_id","tier")})
            self.assertTrue(x.valid(),case["case_id"])
    def test_unique_row_keys(self):
        for item in MANIFEST["cases"]:
            c=Case(**{k:v for k,v in item.items() if k not in ("case_id","tier")})
            rows=reference(c)
            self.assertEqual(len(set((r[0],r[2]) for r in rows)),len(rows))
if __name__=="__main__":unittest.main()
