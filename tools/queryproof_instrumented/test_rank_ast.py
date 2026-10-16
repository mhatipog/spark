"""Focused negative and positive tests for AST risk features."""
import unittest
from rank_ast import changes, _sorted_topk_subqueries

def n(name, children=0, **kwargs):
    return {"class":"org.apache.spark.sql.catalyst.plans.logical."+name,
            "num-children":children, **kwargs}
def e(old, new):
    return {"old":old,"new":new}

class TestAstRiskFeatures(unittest.TestCase):
    def test_correlated_subquery_loses_sort_but_limit_survives(self):
        before = n('GlobalLimit',1)
        after = n('GlobalLimit',1)
        def embed(sort):
            plan=[before,n('LocalLimit',1)]
            if sort:plan += [n('Sort',1)]
            plan += [n('LogicalRDD')]
            # when Sort is removed, LocalLimit still has its LogicalRDD child
            return [{"class":"org.apache.spark.sql.catalyst.expressions.ScalarSubquery",
                     "num-children":0,"plan":plan}]
        old=[n('Filter',0,condition=embed(True))]
        new=[n('Filter',0,condition=embed(False))]
        self.assertEqual(_sorted_topk_subqueries(old),1)
        self.assertEqual(_sorted_topk_subqueries(new),0)
        self.assertEqual(changes(e(old,new))["subquery_topk_sort_guarantee_lost"],12)
    def test_safe_project_deletion_is_not_movement(self):
        old=[n('Project',1),n('LocalLimit',1),n('LogicalRDD')]
        new=[n('LocalLimit',1),n('LogicalRDD')]
        self.assertNotIn('project_moved_below_selection_boundary',changes(e(old,new)))
    def test_real_project_reordering_is_risk(self):
        old=[n('Project',1),n('LocalLimit',1),n('LogicalRDD')]
        new=[n('LocalLimit',1),n('Project',1),n('LogicalRDD')]
        self.assertEqual(changes(e(old,new))['project_moved_below_selection_boundary'],6)
    def test_sort_removed_without_topk_is_weak_only(self):
        old=[n('Sort',1),n('LogicalRDD')]
        new=[n('LogicalRDD')]
        self.assertEqual(changes(e(old,new)),{'sort_count_decreased_weak':2})
    def test_no_change_no_risk(self):
        plan=[n('GlobalLimit',1),n('LocalLimit',1),n('LogicalRDD')]
        self.assertEqual(changes(e(plan,plan)),{})
    def test_malformed_nested_plan_is_not_silently_accepted(self):
        sub={"class":"org.apache.spark.sql.catalyst.expressions.ScalarSubquery",
             "plan":[n('GlobalLimit',2),n('LogicalRDD')]}
        with self.assertRaises(ValueError):
            _sorted_topk_subqueries([n('Filter',0,condition=[sub])])

if __name__=='__main__':
    unittest.main()
