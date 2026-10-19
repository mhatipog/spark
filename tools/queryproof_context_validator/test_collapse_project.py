"""Positive and adversarial tests for total attribute/literal composition."""
import copy
import unittest

from collapse_project_certificate import certify_collapse_project_event as certify
from test_join_prefilter import att, EX, PL, INNER
from test_contextual_join_guard import logical_rdd

def alias(target, n, name):
    return [{"class": EX + "Alias", "num-children": 1, "child": 0,
             "exprId": {"id": n, "jvmId": "synthetic"}, "name": name},
            target]

def alias_ref(n, name="renamed", dtype="integer"):
    a = att(0)
    a["exprId"] = {"id": n, "jvmId": "synthetic"}
    a["name"] = name
    a["dataType"] = dtype
    return a

def literal(value="hello", dtype="string"):
    return {"class": EX + "Literal", "num-children": 0,
            "dataType": dtype, "value": value}

def proj(*entries):
    return {"class": PL + "Project", "num-children": 1, "child": 0,
            "projectList": list(entries)}

def case(lower=None, upper=None, merged=None, base=None):
    a, b = att(0), att(1)
    lower = copy.deepcopy(lower if lower is not None else [[a], [b]])
    upper = copy.deepcopy(upper if upper is not None else [
        alias(literal(), 20, "tag"), [a], [b]])
    merged = copy.deepcopy(upper if merged is None else merged)
    base = copy.deepcopy(base if base is not None else
                         [logical_rdd(a, b)])
    return {"old": [proj(*upper), proj(*lower), *copy.deepcopy(base)],
            "new": [proj(*merged), *copy.deepcopy(base)]}

def sort_frame():
    return {"class": PL + "Sort", "num-children": 1,
            "child": 0, "global": True, "order": [["same"]]}

class CollapseTests(unittest.TestCase):
    def yes(self, e):
        v = certify(e)
        self.assertEqual(v["status"], "EQUIVALENT_IN_FRAGMENT", v)
        return v

    def no(self, e):
        v = certify(e)
        self.assertEqual(v["status"], "UNKNOWN", v)
        return v

    def test_direct_identity_and_literal_alias(self):
        p = self.yes(case())
        self.assertEqual(p["columns"], 3)
        self.assertEqual(p["literal_outputs"], 1)

    def test_sort_frame_bag_only(self):
        e = case()
        e["old"].insert(0, sort_frame())
        e["new"].insert(0, sort_frame())
        self.assertEqual(self.yes(e)["sort_frames"], 1)

    def test_alias_into_direct_output(self):
        a, b = att(0), att(1)
        e = case(lower=[alias(a, 10, "alias10"), [b]],
                 upper=[[alias_ref(10, "alias10")], [b]],
                 merged=[alias(a, 10, "alias10"), [b]])
        self.yes(e)

    def test_two_alias_substitutions(self):
        a = att(0)
        e = case(lower=[alias(a, 10, "alias10")],
                 upper=[alias(alias_ref(10, "alias10"), 20, "twice")],
                 merged=[alias(a, 20, "twice")])
        self.yes(e)

    def test_literal_substituted_through_alias(self):
        e = case(lower=[alias(literal(None), 10, "maybe")],
                 upper=[alias(alias_ref(10, "maybe", "string"), 20, "copy")],
                 merged=[alias(literal(None), 20, "copy")])
        self.yes(e)

    def test_null_and_duplicate_values_are_not_eliminated(self):
        a = att(0)
        e = case(lower=[[a], alias(literal(None), 10, "n")],
                 upper=[[a], [alias_ref(10, "n", "string")]],
                 merged=[[a], alias(literal(None), 10, "n")])
        self.yes(e)

    def test_left_outer_join_input_preserved(self):
        a, b = att(0), att(1)
        join = {"class": PL + "Join", "num-children": 2, "left": 0,
                "right": 1,
                "joinType": {"object":
                    "org.apache.spark.sql.catalyst.plans.LeftOuter$"}}
        e = case(base=[join, logical_rdd(a), logical_rdd(b)])
        self.yes(e)

    def test_changed_literal_rejected(self):
        e = case()
        e["new"][0]["projectList"][0][1]["value"] = "different"
        self.no(e)

    def test_changed_output_id_rejected(self):
        e = case()
        e["new"][0]["projectList"][0][0]["exprId"]["id"] = 25
        self.no(e)

    def test_changed_output_name_rejected(self):
        e = case()
        e["new"][0]["projectList"][0][0]["name"] = "wrong"
        self.no(e)

    def test_changed_child_plan_rejected(self):
        e = case()
        e["new"][-1]["rdd"] = "different"
        self.no(e)

    def test_unbound_lower_attribute_rejected(self):
        e = case(lower=[[att(90)]])
        self.no(e)

    def test_upper_attribute_removed_from_lower_rejected(self):
        e = case(lower=[[att(0)]])
        self.no(e)

    def test_type_mismatch_rejected(self):
        a = att(0)
        a["dataType"] = "string"
        e = case(lower=[[a]])
        self.no(e)

    def test_duplicate_output_ids_rejected(self):
        e = case(lower=[[att(0)], [att(0)]])
        self.no(e)

    def test_nondeterministic_lower_projection_rejected(self):
        e = case()
        e["old"][1]["projectList"][0] = [
            {"class": EX+"Alias", "num-children": 1, "child": 0,
             "exprId": {"id": 40, "jvmId": "synthetic"},
             "name": "r"},
            {"class": EX+"Rand", "num-children": 0}]
        self.no(e)

    def test_nested_coalesce_rejected(self):
        e = case()
        e["old"][1]["projectList"][0] = [
            {"class": EX+"Alias", "num-children": 1, "child": 0,
             "exprId": {"id": 40, "jvmId": "synthetic"},
             "name": "co"},
            {"class": EX+"Coalesce", "num-children": 2},
            att(0), att(1)]
        self.no(e)

    def test_bad_project_metadata_rejected(self):
        e = case()
        e["old"][1]["newKey"] = True
        self.no(e)

    def test_changed_sort_metadata_rejected(self):
        e = case()
        e["old"].insert(0, sort_frame())
        e["new"].insert(0, sort_frame())
        e["new"][0]["global"] = False
        self.no(e)

    def test_semijoin_output_not_union_rejected(self):
        a, b = att(0), att(1)
        join = {"class": PL + "Join", "num-children": 2,
                "left": 0, "right": 1,
                "joinType": {"object":
                    "org.apache.spark.sql.catalyst.plans.LeftSemi$"}}
        self.no(case(base=[join, logical_rdd(a), logical_rdd(b)]))

    def test_not_a_collapse_rejected(self):
        e = case()
        e["new"].insert(1, proj([att(0)]))
        self.no(e)


if __name__ == "__main__":
    unittest.main()
