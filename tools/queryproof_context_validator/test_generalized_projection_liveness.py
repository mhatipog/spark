"""Adversarial tests of compositional observational Project liveness."""
import copy
import unittest

from generalized_projection_liveness_certificate import (
    certify_generalized_projection_liveness_event as certify,
)
from test_join_prefilter import att, EX, PL
from test_contextual_join_guard import logical_rdd
from test_collapse_project import alias, alias_ref, literal, proj
from test_column_pruning_liveness import agg, agg_fn


def rem(src, divisor=2):
    return [{"class": EX + "Remainder", "num-children": 2,
             "left": 0, "right": 1}, src, literal(divisor, "integer")]


def alias_expr(expr, expr_id, name):
    return [alias(expr[0], expr_id, name)[0], *expr]


def example():
    a, b = att(0), att(1)
    k = alias_expr(rem(a), 10, "k")
    key = alias_ref(10, "k")
    top = agg([[copy.deepcopy(key)]], [
        agg_fn("Sum", [copy.deepcopy(a)], 20),
        agg_fn("Count", [literal(1, "integer")], 21)])
    base = logical_rdd(a, b)
    old = [top, proj([a], [b], k), base]
    new = [copy.deepcopy(top), proj([a], [key]),
           proj([a], [b], copy.deepcopy(k)), copy.deepcopy(base)]
    return {"old": old, "new": new}


def dead_expression_example():
    a, b = att(0), att(1)
    k = alias_expr(rem(a), 10, "k")
    top = agg([[a]], [agg_fn("Count", [literal(1, "integer")], 20)])
    base = logical_rdd(a, b)
    return {"old": [top, proj([a], [b], k), base],
            "new": [copy.deepcopy(top), proj([a], [b]),
                    proj([a], [b], copy.deepcopy(k)),
                    copy.deepcopy(base)]}


class GeneralizedLivenessTests(unittest.TestCase):
    def yes(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT", result)
        return result

    def no(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "UNKNOWN", result)
        return result

    def test_live_computed_alias_survives_nested_project(self):
        result = self.yes(example())
        self.assertEqual(result["live_attributes"], 2)
        self.assertEqual(result["old_project_depth"], 1)
        self.assertEqual(result["new_project_depth"], 2)

    def test_dead_remainder_may_be_rearranged(self):
        result = self.yes(dead_expression_example())
        self.assertEqual(result["live_attributes"], 1)

    def test_unchanged_sort_is_only_bag_congruence(self):
        e = example()
        sort = {"class": PL + "Sort", "num-children": 1, "child": 0,
                "global": True, "order": [["same"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        self.assertEqual(self.yes(e)["sort_frames"], 1)

    def test_empty_projection_preserves_count_multiplicity(self):
        a = att(0)
        top = agg([], [agg_fn("Count", [literal(1, "integer")], 20)])
        base = logical_rdd(a)
        e = {"old": [top, base],
             "new": [copy.deepcopy(top), proj(), copy.deepcopy(base)]}
        r = self.yes(e)
        self.assertEqual(r["live_attributes"], 0)
        self.assertEqual(r["new_output_width"], 0)

    def test_duplicate_rows_are_preserved_by_projection(self):
        bag = [(1, 2), (1, 2), (None, 3)]
        projected = [(row[0],) for row in bag]
        self.assertEqual(projected.count((1,)), 2)
        self.assertEqual(len(projected), len(bag))
        self.yes(dead_expression_example())

    def test_original_leaf_filter_is_unchanged(self):
        e = example()
        f = {"class": PL + "Filter", "num-children": 1,
             "child": 0, "condition": [
                 {"class": EX + "IsNotNull", "num-children": 1,
                  "child": 0}, att(0)]}
        e["old"].insert(-1, copy.deepcopy(f))
        e["new"].insert(-1, copy.deepcopy(f))
        self.yes(e)

    def test_change_live_remainder_denominator_rejected(self):
        e = example()
        e["new"][2]["projectList"][2][-1]["value"] = 3
        self.no(e)

    def test_change_live_aggregate_rejected(self):
        e = example()
        e["new"][0]["groupingExpressions"] = [[att(0)]]
        self.no(e)

    def test_drop_live_key_rejected(self):
        e = example()
        e["new"][1]["projectList"] = [[att(0)]]
        self.no(e)

    def test_alias_name_disagreement_rejected(self):
        e = example()
        e["new"][2]["projectList"][2][0]["name"] = "different"
        self.no(e)

    def test_alias_shadows_existing_expr_id_rejected(self):
        e = example()
        e["new"][2]["projectList"][2][0]["exprId"]["id"] = 0
        self.no(e)

    def test_duplicate_output_expr_id_rejected(self):
        e = example()
        e["new"][1]["projectList"].append([att(0)])
        self.no(e)

    def test_changed_base_relation_rejected(self):
        e = example()
        e["new"][-1]["output"][0][0]["exprId"]["id"] = 90
        self.no(e)

    def test_modified_sort_metadata_rejected(self):
        e = example()
        sort = {"class": PL + "Sort", "num-children": 1,
                "child": 0, "global": True, "order": [["same"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        e["new"][0]["order"] = [["changed"]]
        self.no(e)

    def test_zero_divisor_even_for_dead_expression_rejected(self):
        e = dead_expression_example()
        e["old"][1]["projectList"][2][-1]["value"] = 0
        self.no(e)

    def test_nonliteral_divisor_rejected(self):
        e = example()
        e["new"][2]["projectList"][2][-1] = att(1)
        self.no(e)

    def test_rand_cannot_be_discarded(self):
        e = dead_expression_example()
        e["old"][1]["projectList"][2] = alias_expr(
            [{"class": EX + "Rand", "num-children": 0}], 10, "k")
        self.no(e)

    def test_reminder_with_unknown_metadata_rejected(self):
        e = example()
        e["old"][1]["projectList"][2][1]["failOnError"] = True
        self.no(e)

    def test_outer_join_base_not_in_schema_whitelist(self):
        e = example()
        join = {"class": PL + "Join", "num-children": 2,
                "left": 0, "right": 1,
                "joinType": {"object":
                    "org.apache.spark.sql.catalyst.plans.LeftOuter$"}}
        base = logical_rdd(att(0), att(1))
        e["old"][-1:] = [join, base, copy.deepcopy(base)]
        e["new"][-1:] = [copy.deepcopy(join), copy.deepcopy(base),
                         copy.deepcopy(base)]
        self.no(e)

    def test_unknown_cast_in_computed_project_rejected(self):
        e = example()
        e["old"][1]["projectList"][2] = alias_expr([
            {"class": EX + "Cast", "num-children": 1, "child": 0},
            att(0)], 10, "k")
        self.no(e)

    def test_changed_project_output_metadata_rejected(self):
        e = example()
        e["new"][1]["metadata"] = "untrusted"
        self.no(e)

    def test_project_reference_out_of_scope_rejected(self):
        e = example()
        e["new"][2]["projectList"][2] = alias_expr(rem(att(99)), 10, "k")
        self.no(e)

    def test_project_stack_depth_limit(self):
        e = example()
        for _ in range(8):
            e["new"].insert(2, proj([att(0)]))
        self.no(e)

    def test_same_tree_is_not_a_transition(self):
        e = example()
        e["new"] = copy.deepcopy(e["old"])
        self.no(e)

    def test_different_live_identity_cannot_be_forged(self):
        e = example()
        e["new"][1]["projectList"][1][0]["exprId"]["id"] = 11
        self.no(e)


if __name__ == "__main__":
    unittest.main()
