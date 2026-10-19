"""Reject malformed and non-equivalent INNER JOIN Filter transportation."""
import copy
import unittest

from join_filter_transport_certificate import certify_inner_join_filter_transport
from test_join_prefilter import att, expr, event
from test_project_pushdown import alias, att as pat

EX = "org.apache.spark.sql.catalyst.expressions."
PL = "org.apache.spark.sql.catalyst.plans.logical."


def filt(cond):
    return {"class": PL + "Filter", "num-children": 1,
            "condition": cond, "child": 0}


def transported(where, condition=None, left=None, right=None,
                join_condition_after=None):
    base = event(condition=condition)
    joined = copy.deepcopy(base["old"])
    new = copy.deepcopy(joined)
    if join_condition_after is not None:
        new[0]["condition"] = join_condition_after
    if left is not None:
        new.insert(1, filt(left))
    if right is not None:
        new.insert(len(new) - 2, filt(right))
    return {"old": [filt(where), *joined], "new": new}


class JoinFilterTransportTests(unittest.TestCase):
    def test_left_filter_transport(self):
        a = att(0)
        ev = transported(expr("IsNotNull", [a]),
                         left=expr("IsNotNull", [a]))
        result = certify_inner_join_filter_transport(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertTrue(result["sqlite_verified"])
        self.assertEqual(result["side_guards"], 1)

    def test_two_side_guard_transport(self):
        a, b = att(0), att(1)
        where = expr("And", expr("IsNotNull", [a]),
                    expr("IsNotNull", [b]))
        ev = transported(where, left=expr("IsNotNull", [a]),
                         right=expr("IsNotNull", [b]))
        result = certify_inner_join_filter_transport(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["side_guards"], 2)

    def test_where_can_move_into_join_condition(self):
        a, b = att(0), att(1)
        c = expr("EqualTo", [a], [b])
        where = expr("IsNotNull", [a])
        ev = transported(where, condition=c,
                         join_condition_after=expr("And", c, where))
        result = certify_inner_join_filter_transport(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["side_guards"], 0)

    def test_null_safe_join_with_null_filter(self):
        a, b = att(0), att(1)
        c = expr("EqualNullSafe", [a], [b])
        where = expr("IsNull", [a])
        ev = transported(where, condition=c, left=where)
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "EQUIVALENT_IN_FRAGMENT")

    def test_dropped_filter_is_not_equivalent(self):
        a = att(0)
        ev = transported(expr("IsNull", [a]))
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")

    def test_extra_restrictive_guard_is_not_equivalent(self):
        a = att(0)
        ev = transported(expr("IsNull", [a]),
                         left=expr("IsNotNull", [a]))
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")

    def test_wrong_side_guard_rejected(self):
        a, b = att(0), att(1)
        ev = transported(expr("IsNotNull", [a]),
                         right=expr("IsNotNull", [a]))
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")

    def test_join_metadata_change_and_outer_rejected(self):
        a = att(0)
        ev = transported(expr("IsNotNull", [a]),
                         left=expr("IsNotNull", [a]))
        ev["new"][0]["hint"] = "changed"
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")
        ev = transported(expr("IsNotNull", [a]),
                         left=expr("IsNotNull", [a]))
        ev["new"][0]["joinType"] = {"object": PL + "LeftOuter$"}
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")

    def test_malformed_child_and_untrusted_project_rejected(self):
        a = att(0)
        ev = transported(expr("IsNotNull", [a]),
                         left=expr("IsNotNull", [a]))
        ev["new"][2]["projectList"] = [[{"class": EX + "Rand",
                                        "num-children": 0}]]
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")
        ev = transported(expr("IsNotNull", [a]),
                         left=expr("IsNotNull", [a]))
        ev["new"][1]["child"] = 1
        self.assertEqual(certify_inner_join_filter_transport(ev)["status"],
                         "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
