"""Adversarial controls for literal-aware Project/Sort contextual congruence."""
import copy
import unittest

from contextual_join_guard_certificate import certify_contextual_join_guard_event
from test_join_prefilter import att, expr, EX, PL, INNER


def logical_rdd(*attributes):
    return {"class": "org.apache.spark.sql.execution.LogicalRDD",
            "num-children": 0, "output": [[a] for a in attributes],
            "rdd": "fixed", "session": "fixed"}


def literal_alias(out=50):
    return [
        {"class": EX + "Alias", "num-children": 1, "child": 0,
         "exprId": {"id": out, "jvmId": "synthetic"}},
        {"class": EX + "Literal", "num-children": 0, "dataType": "string",
         "value": ""},
    ]


def fixture(left_guard=True, right_guard=True):
    a, b, name = att(0), att(1), att(2)
    name["dataType"] = "string"
    join = {"class": PL + "Join", "num-children": 2,
            "left": 0, "right": 1, "joinType": INNER,
            "condition": expr("EqualTo", [a], [b])}
    left, right = logical_rdd(a, name), logical_rdd(b)
    old = [join, left, right]
    new = [copy.deepcopy(join)]
    for leaf, x, enabled in ((left, a, left_guard), (right, b, right_guard)):
        if enabled:
            new.append({"class": PL + "Filter", "num-children": 1,
                        "child": 0, "condition": expr("IsNotNull", [x])})
        new.append(copy.deepcopy(leaf))
    return {"old": old, "new": new}


def wrap_project(e, entries=None):
    a, b, name = att(0), att(1), att(2)
    name["dataType"] = "string"
    if entries is None:
        entries = [literal_alias(), [a], [b], [name]]
    frame = {"class": PL + "Project", "num-children": 1,
             "child": 0, "projectList": entries}
    return {"old": [copy.deepcopy(frame), *e["old"]],
            "new": [copy.deepcopy(frame), *e["new"]]}


def wrap_sort(e):
    frame = {"class": PL + "Sort", "num-children": 1, "child": 0,
             "global": True, "order": [["synthetic key"]]}
    return {"old": [copy.deepcopy(frame), *e["old"]],
            "new": [copy.deepcopy(frame), *e["new"]]}


class ContextualJoinTests(unittest.TestCase):
    def assert_certified(self, e):
        r = certify_contextual_join_guard_event(e)
        self.assertEqual(r["status"], "EQUIVALENT_IN_FRAGMENT", r)
        self.assertTrue(r["sqlite_verified"])
        return r

    def assert_unknown(self, e):
        r = certify_contextual_join_guard_event(e)
        self.assertEqual(r["status"], "UNKNOWN", r)

    def test_direct_schema_checked_logical_rdd(self):
        r = self.assert_certified(fixture())
        self.assertEqual(r["orbits_checked"], 5)
        self.assertEqual(r["guarded_sides"], [0, 1])

    def test_literal_and_string_column_project_frame(self):
        r = self.assert_certified(wrap_project(fixture()))
        self.assertEqual(r["constants"], 1)
        self.assertEqual(r["frames"], ["project"])

    def test_sort_over_literal_projection_is_bag_sound(self):
        r = self.assert_certified(wrap_sort(wrap_project(fixture())))
        self.assertEqual(r["frames"], ["sort", "project"])

    def test_nested_project_alias_of_attribute(self):
        e = wrap_project(fixture())
        r = self.assert_certified(wrap_project(e, [literal_alias(60)]))
        self.assertEqual(r["constants"], 2)

    def test_missing_guard_not_accepted(self):
        self.assert_unknown(fixture(False, False))

    def test_guard_wrong_side_cannot_be_accepted(self):
        e = fixture()
        e["new"][1]["condition"] = expr("IsNotNull", [att(1)])
        self.assert_unknown(e)

    def test_not_implied_guard_rejected(self):
        e = fixture()
        e["new"][1]["condition"] = expr("IsNull", [att(0)])
        self.assert_unknown(e)

    def test_null_safe_equality_rejects_nonnull_guard(self):
        e = fixture()
        e["old"][0]["condition"] = expr("EqualNullSafe", [att(0)], [att(1)])
        e["new"][0]["condition"] = copy.deepcopy(e["old"][0]["condition"])
        self.assert_unknown(e)

    def test_outer_join_ineligible_even_with_null_guard(self):
        e = wrap_project(fixture())
        for side in ("old", "new"):
            e[side][1]["joinType"] = {"object": PL + "LeftOuter$"}
        self.assert_unknown(e)

    def test_nondeterministic_project_refused(self):
        e = wrap_project(fixture(), [[
            {"class": EX + "Alias", "num-children": 1, "child": 0,
             "exprId": {"id": 50, "jvmId": "synthetic"}},
            {"class": EX + "Rand", "num-children": 0},
        ]])
        self.assert_unknown(e)

    def test_coalesce_project_not_claimed(self):
        e = wrap_project(fixture(), [[
            {"class": EX + "Alias", "num-children": 1, "child": 0,
             "exprId": {"id": 50, "jvmId": "synthetic"}},
            {"class": EX + "Coalesce", "num-children": 2},
            att(0), att(1),
        ]])
        self.assert_unknown(e)

    def test_changed_frame_and_sort_metadata_rejected(self):
        e = wrap_project(fixture())
        e["new"][0]["projectList"][0][1]["value"] = "changed"
        self.assert_unknown(e)
        e = wrap_sort(wrap_project(fixture()))
        e["new"][0]["order"] = [["different"]]
        self.assert_unknown(e)

    def test_unavailable_input_attribute_rejected(self):
        e = wrap_project(fixture(), [[att(99)]])
        self.assert_unknown(e)

    def test_unresolved_leaf_output_rejected(self):
        e = fixture()
        e["old"][1]["output"] = None
        self.assert_unknown(e)

    def test_leaf_schema_reuse_across_join_sides_rejected(self):
        e = fixture()
        e["old"][2]["output"] = copy.deepcopy(e["old"][1]["output"])
        e["new"][4]["output"] = copy.deepcopy(e["old"][1]["output"])
        self.assert_unknown(e)

    def test_extra_join_metadata_rejected(self):
        e = fixture()
        e["new"][0]["joinHintUnexpected"] = 3
        self.assert_unknown(e)

    def test_new_filter_changes_child_itself_rejected(self):
        e = fixture()
        e["new"][2]["rdd"] = "changed"
        self.assert_unknown(e)


if __name__ == "__main__":
    unittest.main()
