"""Sound and deliberately adversarial identity-removal / Aggregate controls."""
import copy
import unittest

from aggregate_framed_identity_certificate import (
    certify_aggregate_framed_identity_event as certify,
)
from test_join_prefilter import att, EX, PL
from test_collapse_project import alias, alias_ref, literal, proj
from test_contextual_join_guard import logical_rdd


def aggregate(groups, expressions):
    return {"class": PL + "Aggregate", "num-children": 1,
            "child": 0, "groupingExpressions": groups,
            "aggregateExpressions": expressions}


def example():
    a = att(0)
    computed = alias(a, 10, "x")
    computed.insert(1, {"class": EX + "Add", "num-children": 2,
                        "left": 0, "right": 1})
    computed.append(literal(1, "integer"))
    intermediate = alias_ref(10, "x")
    forwarded = proj([intermediate])
    created = proj(computed)
    group = aggregate([[intermediate]], [alias(intermediate, 20, "group_x")])
    agg_attr = alias_ref(20, "group_x")
    sum_expr = alias(agg_attr, 30, "total")
    sum_expr.insert(1, {"class": EX + "aggregate.AggregateExpression",
                        "num-children": 1, "aggregateFunction": 0})
    sum_expr.insert(2, {"class": EX + "aggregate.Sum", "num-children": 1, "child": 0})
    summary = aggregate([], [sum_expr])
    leaf = logical_rdd(a)
    return {"old": [summary, group, forwarded, created, leaf],
            "new": [copy.deepcopy(summary), copy.deepcopy(group),
                    copy.deepcopy(created), copy.deepcopy(leaf)]}



def example_sort_remainder():
    a, b = att(0), att(1)
    reduced = alias(b, 10, "mod")
    reduced.insert(1, {"class": EX + "Remainder", "num-children": 2,
                       "left": 0, "right": 1})
    reduced.append(literal(2, "integer"))
    lower = proj([a], reduced)
    outer = proj([a], [alias_ref(10, "mod")])
    mod = alias_ref(10, "mod")
    grouped = aggregate(
        [[mod]],
        [alias(mod, 20, "group_mod"),
         [*alias(a, 30, "total")[:1],
          {"class": EX + "aggregate.AggregateExpression",
           "num-children": 1, "aggregateFunction": 0},
          {"class": EX + "aggregate.Sum",
           "num-children": 1, "child": 0},
          a]])
    sorted_rows = {"class": PL + "Sort", "num-children": 1,
                   "child": 0, "global": True,
                   "order": [["synthetic unchanged sort key"]]}
    leaf = logical_rdd(a, b)
    return {"old": [sorted_rows, grouped, outer, lower, leaf],
            "new": [copy.deepcopy(sorted_rows), copy.deepcopy(grouped),
                    copy.deepcopy(lower), copy.deepcopy(leaf)]}


class AggregateFramedIdentityTests(unittest.TestCase):
    def yes(self, event):
        r = certify(event)
        self.assertEqual(r["status"], "EQUIVALENT_IN_FRAGMENT", r)
        return r

    def no(self, event):
        r = certify(event)
        self.assertEqual(r["status"], "UNKNOWN", r)
        return r

    def test_sort_group_remainder_identity(self):
        result = self.yes(example_sort_remainder())
        self.assertEqual(result["aggregate_frames"], 1)
        self.assertEqual(result["sort_frames"], 1)
        self.assertEqual(result["forwarded_columns"], 2)

    def test_sort_frame_changed_rejected(self):
        e = example_sort_remainder()
        e["new"][0]["order"] = [["changed sort key"]]
        self.no(e)

    def test_sort_frame_metadata_equal_but_unsafe_granularity_rejected(self):
        e = example_sort_remainder()
        e["old"][0]["num-children"] = 2
        e["new"][0]["num-children"] = 2
        self.no(e)

    def test_remainder_expression_unchanged(self):
        e = example_sort_remainder()
        self.assertEqual(e["old"][3], e["new"][2])
        self.yes(e)

    def test_changed_divisor_rejected(self):
        e = example_sort_remainder()
        e["new"][2]["projectList"][1][-1]["value"] = 3
        self.no(e)

    def test_non_integral_remainder_rejected(self):
        e = example_sort_remainder()
        for leaf in (e["old"][-1], e["new"][-1]):
            leaf["output"][1][0]["dataType"] = "double"
        for lower in (e["old"][3], e["new"][2]):
            lower["projectList"][1][2]["dataType"] = "double"
        self.no(e)

    def test_unsupported_divide_rejected(self):
        e = example_sort_remainder()
        for lower in (e["old"][3], e["new"][2]):
            lower["projectList"][1][1]["class"] = EX + "Divide"
        self.no(e)

    def test_forwarded_expr_id_under_sort_rejected(self):
        e = example_sort_remainder()
        e["old"][2]["projectList"][1][0]["exprId"]["id"] = 78
        self.no(e)

    def test_non_identity_order_under_sort_rejected(self):
        e = example_sort_remainder()
        e["old"][2]["projectList"].reverse()
        self.no(e)

    def test_extra_limit_inside_aggregate_rejected(self):
        e = example_sort_remainder()
        lim = {"class": PL + "LocalLimit", "num-children": 1,
               "child": 0, "limitExpr": [literal(1, "integer")]}
        e["old"].insert(2, lim)
        e["new"].insert(2, copy.deepcopy(lim))
        self.no(e)

    def test_two_group_and_integer_sum_frames(self):
        result = self.yes(example())
        self.assertEqual(result["aggregate_frames"], 2)
        self.assertEqual(result["forwarded_columns"], 1)

    def test_integer_arithmetic_not_inlined(self):
        e = example()
        self.assertEqual(e["old"][3], e["new"][2])
        self.yes(e)

    def test_changed_underlying_arithmetic_rejected(self):
        e = example()
        e["new"][2]["projectList"][0][-1]["value"] = 2
        self.no(e)

    def test_wrong_forwarded_expr_id_rejected(self):
        e = example()
        e["old"][2]["projectList"][0][0]["exprId"]["id"] = 99
        self.no(e)

    def test_wrong_forwarded_name_rejected(self):
        e = example()
        e["old"][2]["projectList"][0][0]["name"] = "mismatch"
        self.no(e)

    def test_non_identity_projection_rejected(self):
        e = example()
        e["old"][2]["projectList"][0] = alias(
            alias_ref(10, "x"), 40, "not_identity")
        self.no(e)

    def test_changed_aggregate_rejected(self):
        e = example()
        e["new"][0]["groupingExpressions"] = [[alias_ref(20, "group_x")]]
        self.no(e)

    def test_nondeterministic_aggregate_rejected(self):
        e = example()
        e["old"][0]["aggregateExpressions"][0][2]["class"] = EX + "Rand"
        e["new"][0]["aggregateExpressions"][0][2]["class"] = EX + "Rand"
        self.no(e)

    def test_unrecognized_aggregate_metadata_rejected(self):
        e = example()
        e["old"][0]["optimizerHint"] = 7
        e["new"][0]["optimizerHint"] = 7
        self.no(e)

    def test_float_addition_not_certified(self):
        e = example()
        e["old"][-1]["output"][0][0]["dataType"] = "double"
        e["new"][-1]["output"][0][0]["dataType"] = "double"
        e["old"][3]["projectList"][0][2]["dataType"] = "double"
        e["new"][2]["projectList"][0][2]["dataType"] = "double"
        self.no(e)

    def test_non_integer_sum_rejected(self):
        e = example()
        e["old"][0]["aggregateExpressions"][0][2]["class"] = EX + "Average"
        e["new"][0]["aggregateExpressions"][0][2]["class"] = EX + "Average"
        self.no(e)

    def test_substituted_output_order_rejected(self):
        e = example()
        e["old"][2]["projectList"].append([att(0)])
        self.no(e)

    def test_limit_frame_not_assumed_congruent(self):
        e = example()
        lim = {"class": PL + "GlobalLimit", "num-children": 1,
               "child": 0, "limitExpr": [{"class": EX + "Literal",
                                         "num-children": 0,
                                         "dataType": "integer", "value": 1}]}
        e["old"].insert(0, lim)
        e["new"].insert(0, copy.deepcopy(lim))
        self.no(e)


if __name__ == "__main__":
    unittest.main()
