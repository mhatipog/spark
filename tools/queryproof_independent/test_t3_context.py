"""Regression tests for SQL three-valued semantic context obligations.

The predicates are the ALREADY documented DuckDB #23685 / Spark SPARK-58384
bug family. These tests are in-sample semantic controls, not new discoveries.
"""
import itertools
import unittest

from t3_context import (VALUES,TRUTH,TRUE_SET,FULL_3VL,
    tri_and,tri_or,tri_not,original,rewrite,keep,obligation_for_child,
    classify,equivalence,report)


class ThreeValuedContextTests(unittest.TestCase):
    def test_filter_true_set_equivalent(self):
        result=equivalence(original,rewrite,TRUE_SET)
        self.assertTrue(result['equivalent'])
        self.assertEqual(result['counterexamples'],[])

    def test_boolean_values_not_equivalent(self):
        result=equivalence(original,rewrite,FULL_3VL)
        self.assertFalse(result['equivalent'])
        self.assertIn({'a':0,'b':None,'original':None,'rewrite':False},result['counterexamples'])
        self.assertIn({'a':None,'b':1,'original':None,'rewrite':False},result['counterexamples'])

    def test_not_observer_makes_rewrite_unsafe(self):
        result=equivalence(lambda a,b:tri_not(original(a,b)),
                           lambda a,b:tri_not(rewrite(a,b)),TRUE_SET)
        self.assertFalse(result['equivalent'])
        self.assertTrue(any(x['rewrite'] is True and x['original'] is None
                            for x in result['counterexamples']))

    def test_is_null_observer_makes_rewrite_unsafe(self):
        result=equivalence(lambda a,b:original(a,b) is None,
                           lambda a,b:rewrite(a,b) is None,TRUE_SET)
        self.assertFalse(result['equivalent'])

    def test_true_set_stable_under_positive_and_or(self):
        # Positive AND/OR cannot distinguish FALSE from UNKNOWN when
        # the only observer is whether the complete predicate is TRUE.
        for a,b in itertools.product(VALUES,repeat=2):
            orig,alter=original(a,b),rewrite(a,b)
            for sibling in TRUTH:
                with self.subTest(a=a,b=b,sibling=sibling,op='and'):
                    self.assertEqual(keep(tri_and(orig,sibling)),
                                     keep(tri_and(alter,sibling)))
                with self.subTest(a=a,b=b,sibling=sibling,op='or'):
                    self.assertEqual(keep(tri_or(orig,sibling)),
                                     keep(tri_or(alter,sibling)))

    def test_classify_safe_and_unsafe_ancestors(self):
        self.assertEqual(classify(('JOIN_ON_ROOT','AND','OR')),TRUE_SET)
        self.assertEqual(classify(('JOIN_ON_ROOT','NOT','OR')),FULL_3VL)
        self.assertEqual(classify(('FILTER','AND','IS_NULL','OR')),FULL_3VL)
        self.assertEqual(classify(('CASE_WHEN','OR')),TRUE_SET)
        self.assertEqual(obligation_for_child('unknown_sql_function',TRUE_SET),FULL_3VL)
        self.assertEqual(obligation_for_child('AND',FULL_3VL),FULL_3VL)

    def test_emitted_report_has_explicit_counterexamples(self):
        r=report()
        self.assertTrue(r['root_filter']['equivalent'])
        for k in ('value_projection','under_not','under_is_null'):
            self.assertFalse(r[k]['equivalent'],k)
            self.assertGreater(len(r[k]['counterexamples']),0,k)


if __name__=='__main__':
    unittest.main()
