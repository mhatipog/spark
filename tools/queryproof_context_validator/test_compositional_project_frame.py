"""Negative controls for compositional pure-Project context frames."""
import copy
import unittest

from compositional_project_frame_certificate import certify_pure_project_framed_event
from test_join_prefilter import att, expr, event
from test_project_pushdown import direct_event, project, alias, att as project_att
from test_join_filter_transport import transported


def wrap(core, items):
    outer = project(items)
    return {"old": [copy.deepcopy(outer), *core["old"]],
            "new": [copy.deepcopy(outer), *core["new"]]}


class ProjectFrameTests(unittest.TestCase):
    def test_lift_both_join_side_guards(self):
        a, b = att(0), att(1)
        core = event(left_guard=expr("IsNotNull", [a]),
                     right_guard=expr("IsNotNull", [b]))
        result = certify_pure_project_framed_event(wrap(core, [[a], [b]]))
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["core"], "inner_join_implied_filter_insertion")
        self.assertEqual(result["frames"], 1)
        self.assertTrue(result["sqlite_verified"])

    def test_lift_filter_project_swap_with_rename(self):
        a, named = project_att(0), project_att(20)
        core = direct_event(expr("IsNotNull", [named]),
                            expr("IsNotNull", [a]), [alias(a)])
        result = certify_pure_project_framed_event(wrap(core, [[named]]))
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["core"], "filter_project_commutation")
        self.assertEqual(result["orbits_checked_per_core"], [2])

    def test_pure_alias_frame_preserves_bag(self):
        a = project_att(0)
        core = direct_event(expr("IsNotNull", [a]),
                            expr("IsNotNull", [a]))
        result = certify_pure_project_framed_event(wrap(core, [alias(a, out=42)]))
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["alias_outputs"], 1)

    def test_nested_two_frames(self):
        a, b = att(0), att(1)
        core = event(left_guard=expr("IsNotNull", [a]),
                     right_guard=expr("IsNotNull", [b]))
        once = wrap(core, [[a], [b]])
        twice = wrap(once, [[a]])
        result = certify_pure_project_framed_event(twice)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["frames"], 2)

    def test_changing_frame_metadata_is_rejected(self):
        a = att(0)
        core = direct_event(expr("IsNotNull", [a]),
                            expr("IsNotNull", [a]))
        ev = wrap(core, [[a]])
        ev["new"][0]["projectList"] = [[att(3)]]
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")
        ev = wrap(core, [[a]])
        ev["new"][0]["extra"] = 17
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")

    def test_nondeterministic_root_project_is_rejected(self):
        a = att(0)
        core = direct_event(expr("IsNotNull", [a]),
                            expr("IsNotNull", [a]))
        ev = wrap(core, [[{"class": "org.apache.spark.sql.catalyst.expressions.Rand",
                          "num-children": 0}]])
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")

    def test_frame_unknown_child_output_is_rejected(self):
        a = att(0)
        core = direct_event(expr("IsNotNull", [a]),
                            expr("IsNotNull", [a]))
        ev = wrap(core, [[att(3)]])
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")

    def test_lift_where_transport_across_inner_join(self):
        a, b = att(0), att(1)
        core = transported(expr("IsNotNull", [a]),
                           left=expr("IsNotNull", [a]))
        ev = wrap(core, [[a], [b]])
        result = certify_pure_project_framed_event(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["core"], "inner_join_filter_transport")
        self.assertTrue(result["sqlite_verified"])

    def test_wrong_join_predicate_and_invalid_guards_rejected(self):
        a, b = att(0), att(1)
        core = event(left_guard=expr("IsNull", [a]))
        ev = wrap(core, [[a], [b]])
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")
        core = event(left_guard=expr("IsNotNull", [a]))
        ev = wrap(core, [[a], [b]])
        ev["new"][1]["condition"] = expr("IsNull", [a])
        self.assertEqual(certify_pure_project_framed_event(ev)["status"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
