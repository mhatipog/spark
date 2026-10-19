"""Adversarial controls for exact inner-join guard implication."""
import unittest
from join_prefilter_certificate import certify_inner_join_prefilter_event

EX = "org.apache.spark.sql.catalyst.expressions."
PL = "org.apache.spark.sql.catalyst.plans.logical."
INNER = {"object": "org.apache.spark.sql.catalyst.plans.Inner$"}


def att(i):
    return {"class": EX + "AttributeReference", "num-children": 0,
            "name": "v" + str(i), "dataType": "integer",
            "exprId": {"id": i + 1, "jvmId": "synthetic"}}


def expr(op, *children):
    return [{"class": EX + op, "num-children": len(children)}] + [
        node for child in children for node in child
    ]


def project(attribute):
    node = {"class": PL + "Project", "num-children": 1,
            "projectList": [[attribute]]}
    return [node, {"class": PL + "LocalRelation", "num-children": 0}]


def event(condition=None, left_guard=None, right_guard=None):
    a, b = att(0), att(1)
    join = {"class": PL + "Join", "num-children": 2,
            "joinType": INNER,
            "condition": condition if condition is not None
                         else expr("EqualTo", [a], [b])}
    left, right = project(a), project(b)
    def with_guard(nodes, guard):
        if guard is None:
            return nodes
        return [{"class": PL + "Filter", "num-children": 1,
                 "condition": guard}, *nodes]
    return {"old": [join, *left, *right],
            "new": [dict(join), *with_guard(left, left_guard),
                    *with_guard(right, right_guard)]}


class JoinGuardTests(unittest.TestCase):
    def test_equality_forces_both_sides_nonnull(self):
        a, b = att(0), att(1)
        ev = event(left_guard=expr("IsNotNull", [a]),
                   right_guard=expr("IsNotNull", [b]))
        result = certify_inner_join_prefilter_event(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["guarded_sides"], [0, 1])
        self.assertEqual(result["orbits_checked"], 5)
        self.assertTrue(result["sqlite_verified"])

    def test_only_one_side_guard(self):
        a = att(0)
        res = certify_inner_join_prefilter_event(
            event(left_guard=expr("IsNotNull", [a])))
        self.assertEqual(res["status"], "EQUIVALENT_IN_FRAGMENT")

    def test_unjustified_guard_must_abstain(self):
        a, b = att(0), att(1)
        res = certify_inner_join_prefilter_event(
            event(left_guard=expr("IsNull", [a])))
        self.assertEqual(res["status"], "UNKNOWN")
        self.assertEqual(res["reason"], "JOIN_CONDITION_DOES_NOT_IMPLY_GUARDS")
        # Null-safe equality permits NULL=NULL; rejecting this guard is essential.
        res = certify_inner_join_prefilter_event(
            event(condition=expr("EqualNullSafe", [a], [b]),
                  left_guard=expr("IsNotNull", [a])))
        self.assertEqual(res["status"], "UNKNOWN")

    def test_side_scoping_and_unsupported_metadata_fail_closed(self):
        a, b = att(0), att(1)
        wrong_side = event(left_guard=expr("IsNotNull", [b]))
        self.assertEqual(certify_inner_join_prefilter_event(wrong_side)["status"],
                         "UNKNOWN")
        changed_child = event(left_guard=expr("IsNotNull", [a]))
        changed_child["new"][2]["projectList"] = [[b]]
        self.assertEqual(certify_inner_join_prefilter_event(changed_child)["status"],
                         "UNKNOWN")
        outer = event(left_guard=expr("IsNotNull", [a]))
        outer["old"][0]["joinType"] = {"object": PL + "LeftOuter$"}
        outer["new"][0]["joinType"] = dict(outer["old"][0]["joinType"])
        self.assertEqual(certify_inner_join_prefilter_event(outer)["status"],
                         "UNKNOWN")
        changed_metadata = event(left_guard=expr("IsNotNull", [a]))
        changed_metadata["new"][0]["extra"] = "untrusted"
        self.assertEqual(certify_inner_join_prefilter_event(
            changed_metadata)["status"], "UNKNOWN")
        malformed = event(left_guard=expr("IsNotNull", [a]))
        malformed["new"][1]["num-children"] = 2
        self.assertEqual(certify_inner_join_prefilter_event(malformed)["status"],
                         "UNKNOWN")

    def test_filters_must_be_added_only(self):
        a = att(0)
        ev = event(left_guard=expr("IsNotNull", [a]))
        ev["new"][1] = {"class": PL + "Project", "num-children": 1,
                        "projectList": [[a]]}
        self.assertEqual(certify_inner_join_prefilter_event(ev)["status"],
                         "UNKNOWN")
        no_filter = event()
        self.assertEqual(certify_inner_join_prefilter_event(no_filter)["status"],
                         "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
