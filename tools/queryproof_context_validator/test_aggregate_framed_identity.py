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


class AggregateFramedIdentityTests(unittest.TestCase):
    def yes(self, event):
        r = certify(event)
        self.assertEqual(r["status"], "EQUIVALENT_IN_FRAGMENT", r)
        return r

    def no(self, event):
        r = certify(event)
        self.assertEqual(r["status"], "UNKNOWN", r)
        return r

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
