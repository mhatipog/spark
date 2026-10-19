"""Adversarial tests for exact Filter / identity-Project commutation."""
import copy
import unittest

from project_pushdown_certificate import certify_project_filter_event

EX = "org.apache.spark.sql.catalyst.expressions."
PL = "org.apache.spark.sql.catalyst.plans.logical."
INNER = {"object": "org.apache.spark.sql.catalyst.plans.Inner$"}


def att(i, typ="integer"):
    return {"class": EX + "AttributeReference", "num-children": 0,
            "name": "v" + str(i), "dataType": typ,
            "exprId": {"id": i + 1, "jvmId": "test"}}


def expr(op, *children):
    return [{"class": EX + op, "num-children": len(children)}] + [
        node for child in children for node in child]


def alias(source, out=20):
    return [{"class": EX + "Alias", "num-children": 1,
             "name": "alias", "child": 0,
             "exprId": {"id": out + 1, "jvmId": "test"}}, source]


def filt(condition):
    return {"class": PL + "Filter", "num-children": 1,
            "condition": condition, "child": 0}


def project(entries):
    return {"class": PL + "Project", "num-children": 1,
            "projectList": entries, "child": 0}


def direct_event(old_condition, new_condition, items=None):
    src = att(0)
    item = items if items is not None else [[src]]
    pj = project(item)
    child = {"class": PL + "LocalRelation", "num-children": 0}
    return {"old": [filt(old_condition), pj, child],
            "new": [copy.deepcopy(pj), filt(new_condition), child]}


def join_event(left, right=None, jt=INNER):
    rhs = right if right is not None else direct_event(
        expr("IsNotNull", [att(1)]), expr("IsNotNull", [att(1)]),
        [[att(1)]])
    base = {"class": PL + "Join", "num-children": 2,
            "left": 0, "right": 1, "joinType": jt,
            "condition": expr("EqualTo", [att(20)], [att(1)])}
    return {"old": [base, *left["old"], *rhs["old"]],
            "new": [copy.deepcopy(base), *left["new"], *rhs["new"]]}


class ProjectionPushdownTests(unittest.TestCase):
    def test_passthrough_filter_swap(self):
        a = att(0)
        ev = direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [a]))
        result = certify_project_filter_event(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["aliases_per_branch"], [0])
        self.assertTrue(result["sqlite_verified"])

    def test_alias_rename_under_nullable_logic(self):
        a = att(0)
        named = att(20)
        ev = direct_event(expr("Not", expr("IsNull", [named])),
                          expr("IsNotNull", [a]), [alias(a)])
        result = certify_project_filter_event(ev)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["aliases_per_branch"], [1])
        self.assertEqual(result["orbits_checked_per_branch"], [2])

    def test_bag_duplicates_and_null_values(self):
        # A rename leaves a duplicated value duplicated; 3VL TRUE alone
        # determines whether an individual row survives.
        values = [None, None, 1, 1, 1, 2]
        def predicate(v):
            return v is not None
        upstream = [v for v in values if predicate(v)]
        downstream = [v for v in values if predicate(v)]
        self.assertEqual(upstream, downstream)
        self.assertEqual(upstream.count(1), 3)

    def test_misbound_expr_id_is_rejected(self):
        a = att(0)
        wrong = att(1)
        result = certify_project_filter_event(
            direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [wrong])))
        self.assertEqual(result["status"], "UNKNOWN")

    def test_invalid_null_predicate_rejected(self):
        a = att(0)
        result = certify_project_filter_event(
            direct_event(expr("IsNotNull", [a]), expr("IsNull", [a])))
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(result["reason"], "PUSHED_FILTER_NOT_EQUIVALENT")

    def test_nondeterministic_or_computed_projection_rejected(self):
        a = att(0)
        f = expr("IsNotNull", [a])
        ev = direct_event(f, f, [[{"class": EX + "Rand",
                                 "num-children": 0}]])
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")
        ev = direct_event(f, f,
                          [[{"class": EX + "Alias", "num-children": 1,
                             "exprId": {"id": 21, "jvmId": "test"}},
                            {"class": EX + "Add", "num-children": 2},
                            att(0), att(1)]])
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")

    def test_project_change_and_input_change_rejected(self):
        a = att(0)
        ev = direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [a]))
        ev["new"][0]["projectList"] = [[att(1)]]
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")
        ev = direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [a]))
        ev["new"][2] = {"class": PL + "OneRowRelation", "num-children": 0}
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")

    def test_join_frame_on_both_sides(self):
        a, named = att(0), att(20)
        left = direct_event(expr("IsNotNull", [named]),
                            expr("IsNotNull", [a]), [alias(a)])
        result = certify_project_filter_event(join_event(left))
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(result["frame"], "unchanged_inner_join")
        self.assertEqual(result["rewritten_branches"], 2)

    def test_outer_join_and_modified_join_rejected(self):
        a, named = att(0), att(20)
        left = direct_event(expr("IsNotNull", [named]),
                            expr("IsNotNull", [a]), [alias(a)])
        ev = join_event(left)
        ev["new"][0]["condition"] = expr("IsNotNull", [named])
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")
        ev = join_event(left, jt={"object": PL + "LeftOuter$"})
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")

    def test_malformed_preorder_and_filter_metadata_rejected(self):
        a = att(0)
        ev = direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [a]))
        ev["new"][1]["extra"] = "unknown"
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")
        ev = direct_event(expr("IsNotNull", [a]), expr("IsNotNull", [a]))
        ev["new"][1]["num-children"] = 2
        self.assertEqual(certify_project_filter_event(ev)["status"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
