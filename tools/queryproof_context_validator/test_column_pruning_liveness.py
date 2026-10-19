"""Adversarial and positive checks for inserted projection / Aggregate liveness."""
import copy
import unittest

from column_pruning_liveness_certificate import (
    certify_column_pruning_liveness_event as certify,
)
from test_join_prefilter import att, EX, PL
from test_contextual_join_guard import logical_rdd
from test_collapse_project import alias, literal, proj


def agg(groups, outputs):
    return {"class": PL + "Aggregate", "num-children": 1,
            "child": 0, "groupingExpressions": groups,
            "aggregateExpressions": outputs}


def agg_fn(kind, child, expr_id):
    head = alias(child, expr_id, "r" + str(expr_id))[0]
    ae = {"class": EX + "aggregate.AggregateExpression",
          "num-children": 1, "aggregateFunction": 0}
    fn = {"class": EX + "aggregate." + kind,
          "num-children": 1, "child": 0}
    return [head, ae, fn, *child]


def example(used=True, with_filter=False):
    a, b = att(0), att(1)
    outputs = [
        agg_fn("Sum", [a], 20),
        agg_fn("Count", [literal(1, "integer")], 21),
        agg_fn("Average", [a], 22),
    ] if used else [
        agg_fn("Count", [literal(1, "integer")], 21)
    ]
    top = agg([], outputs)
    leaf = logical_rdd(a, b)
    extra = []
    if with_filter:
        extra = [{"class": PL + "Filter", "num-children": 1,
                  "child": 0,
                  "condition": [{"class": EX + "IsNotNull",
                                 "num-children": 1, "child": 0}, a]}]
    old = [copy.deepcopy(top), *copy.deepcopy(extra), leaf]
    new = [copy.deepcopy(top), proj(*([[a]] if used else [])),
           *copy.deepcopy(extra), copy.deepcopy(leaf)]
    return {"old": old, "new": new}


def nested_integer_sum():
    a, b = att(0), att(1)
    plus = [{"class": EX + "Add", "num-children": 2,
             "left": 0, "right": 1}, a, literal(2, "integer")]
    left = agg_fn("Sum", plus, 20)
    head = alias(a, 30, "complex")[0]
    right = [
        head,
        {"class": EX + "Add", "num-children": 2, "left": 0, "right": 1},
        {"class": EX + "aggregate.AggregateExpression",
         "num-children": 1, "aggregateFunction": 0},
        {"class": EX + "aggregate.Sum", "num-children": 1, "child": 0},
        a,
        {"class": EX + "Multiply", "num-children": 2, "left": 0, "right": 1},
        {"class": EX + "Cast", "num-children": 1, "child": 0},
        literal(2, "integer"),
        {"class": EX + "aggregate.AggregateExpression",
         "num-children": 1, "aggregateFunction": 0},
        {"class": EX + "aggregate.Count", "num-children": 1, "child": 0},
        literal(1, "integer"),
    ]
    frame = agg([], [left, right])
    leaf = logical_rdd(a, b)
    return {"old": [frame, leaf],
            "new": [copy.deepcopy(frame), proj([a]), copy.deepcopy(leaf)]}


def constant_null_group():
    a = att(0)
    null = {"class": EX + "Literal", "num-children": 0,
            "dataType": "void", "value": None}
    expr = [alias(a, 20, "n")[0],
            {"class": EX + "Length", "num-children": 1, "child": 0},
            {"class": EX + "Cast", "num-children": 1, "child": 0},
            null]
    frame = agg([[copy.deepcopy(null)]], [expr])
    leaf = logical_rdd(a)
    return {"old": [frame, leaf],
            "new": [copy.deepcopy(frame), proj(), copy.deepcopy(leaf)]}


class ColumnPruningLivenessTests(unittest.TestCase):
    def yes(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT", result)
        return result

    def no(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "UNKNOWN", result)
        return result

    def test_sum_average_count_with_one_unused_attribute(self):
        r = self.yes(example())
        self.assertEqual(r["kept_columns"], 1)
        self.assertEqual(r["dropped_columns"], 1)
        self.assertEqual(r["aggregate_referenced_columns"], 1)

    def test_count_star_empty_projection_preserves_multiplicity(self):
        r = self.yes(example(used=False))
        self.assertEqual(r["kept_columns"], 0)
        self.assertTrue(r["empty_projection_supported"])
        rows = [(1, 1), (1, 1), (2, 3), (None, 4)]
        projected = [tuple() for _ in rows]
        self.assertEqual(len(projected), len(rows))
        self.assertEqual(projected.count(tuple()), 4)

    def test_filter_below_pruning_remains_identical(self):
        self.yes(example(used=True, with_filter=True))

    def test_complex_nested_integer_aggregate_uses_only_live_attribute(self):
        self.yes(nested_integer_sum())

    def test_group_by_constant_with_null_expression(self):
        r = self.yes(constant_null_group())
        self.assertEqual(r["dropped_columns"], 1)

    def test_unchanged_sort_above_aggregate_is_bag_only(self):
        e = example()
        sort = {"class": PL + "Sort", "num-children": 1,
                "child": 0, "global": True, "order": [["unchanged"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        self.assertEqual(self.yes(e)["sort_frames"], 1)

    def test_dropping_used_attribute_abstains(self):
        e = example()
        e["new"][1]["projectList"] = [[att(1)]]
        self.no(e)

    def test_computed_pruning_project_abstains(self):
        e = example()
        e["new"][1]["projectList"] = [alias(att(0), 99, "not_forwarded")]
        self.no(e)

    def test_no_actual_pruning_abstains(self):
        e = example()
        e["new"][1]["projectList"] = [[att(0)], [att(1)]]
        self.no(e)

    def test_changed_aggregate_abstains(self):
        e = example()
        e["new"][0]["groupingExpressions"] = [[att(0)]]
        self.no(e)

    def test_changed_underlying_relation_abstains(self):
        e = example()
        e["new"][-1]["output"][1][0]["exprId"]["id"] = 88
        self.no(e)

    def test_volatile_function_abstains(self):
        e = example()
        e["old"][0]["aggregateExpressions"][0][2]["class"] = EX + "Rand"
        e["new"][0]["aggregateExpressions"][0][2]["class"] = EX + "Rand"
        self.no(e)

    def test_filtered_aggregate_abstains(self):
        e = example()
        e["old"][0]["aggregateExpressions"][0][1]["filter"] = "hidden"
        e["new"][0]["aggregateExpressions"][0][1]["filter"] = "hidden"
        self.no(e)

    def test_corrupted_expression_tree_abstains(self):
        e = nested_integer_sum()
        e["old"][0]["aggregateExpressions"][1].append(literal(1, "integer"))
        e["new"][0]["aggregateExpressions"][1].append(literal(1, "integer"))
        self.no(e)

    def test_duplicate_pruned_column_abstains(self):
        e = example()
        e["new"][1]["projectList"] = [[att(0)], [att(0)]]
        self.no(e)

    def test_changed_sort_abstains(self):
        e = example()
        sort = {"class": PL + "Sort", "num-children": 1,
                "child": 0, "global": True, "order": [["same"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        e["new"][0]["order"] = [["other"]]
        self.no(e)

    def test_limit_instead_of_projection_abstains(self):
        e = example()
        e["new"][1] = {"class": PL + "LocalLimit", "num-children": 1,
                       "child": 0, "limitExpr": [literal(1, "integer")]}
        self.no(e)

    def test_pruning_with_inaccurate_name_abstains(self):
        e = example()
        e["new"][1]["projectList"][0][0]["name"] = "other"
        self.no(e)

    def test_nested_project_rewrite_abstains(self):
        e = example()
        e["new"].insert(2, proj([att(0)]))
        self.no(e)


if __name__ == "__main__":
    unittest.main()
