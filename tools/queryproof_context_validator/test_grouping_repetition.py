"""Adversarial tests for row-bag grouping-key idempotence."""
import copy
import itertools
import unittest

from grouping_repetition_certificate import certify_grouping_repetition_event as certify
from test_join_prefilter import att, EX, PL
from test_contextual_join_guard import logical_rdd
from test_collapse_project import alias, alias_ref, literal, proj
from test_column_pruning_liveness import agg, agg_fn


def ex(groupings=None):
    a, b = att(0), att(1)
    old_groups = groupings if groupings is not None else [
        [copy.deepcopy(a)], [copy.deepcopy(a)]]
    before = agg(old_groups, [
        [copy.deepcopy(a)],
        agg_fn("Count", [literal(1, "integer")], 20),
        agg_fn("Sum", [copy.deepcopy(b)], 21),
    ])
    after = copy.deepcopy(before)
    after["groupingExpressions"] = [copy.deepcopy(old_groups[0])]
    base = logical_rdd(a, b)
    return {"old": [before, base], "new": [after, copy.deepcopy(base)]}


def computed():
    a, b = att(0), att(1)
    k = alias_ref(10, "k")
    project = proj([a], [b], [
        alias(a, 10, "k")[0],
        {"class": EX + "Remainder", "num-children": 2,
         "left": 0, "right": 1},
        a, literal(7, "integer")])
    original = agg([[copy.deepcopy(k)], [copy.deepcopy(k)]], [
        [copy.deepcopy(k)], agg_fn("Count", [literal(1, "integer")], 20)])
    target = copy.deepcopy(original)
    target["groupingExpressions"] = [[copy.deepcopy(k)]]
    base = logical_rdd(a, b)
    return {"old": [original, project, base],
            "new": [target, copy.deepcopy(project), copy.deepcopy(base)]}


class GroupRepetitionTests(unittest.TestCase):
    def yes(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT", result)
        return result

    def no(self, event):
        result = certify(event)
        self.assertEqual(result["status"], "UNKNOWN", result)
        return result

    def test_double_attribute(self):
        result = self.yes(ex())
        self.assertEqual(result["removed_duplicates"], 1)
        self.assertEqual(result["grouping_keys_after"], 1)

    def test_threefold_attribute(self):
        e = ex()
        e["old"][0]["groupingExpressions"].append([copy.deepcopy(att(0))])
        self.assertEqual(self.yes(e)["removed_duplicates"], 2)

    def test_permutation_with_multiple_distinct_keys(self):
        a, b = att(0), att(1)
        e = ex([[a], [b], [copy.deepcopy(a)]])
        e["new"][0]["groupingExpressions"] = [[copy.deepcopy(b)], [copy.deepcopy(a)]]
        self.yes(e)

    def test_two_literals_even_with_empty_input(self):
        e = ex([[literal(0, "integer")], [literal(0, "integer")]])
        self.yes(e)

    def test_two_null_literals(self):
        null = {"class": EX + "Literal", "num-children": 0,
                "dataType": "void", "value": None}
        self.yes(ex([[null], [copy.deepcopy(null)]]))

    def test_computed_project_alias_grouping(self):
        self.yes(computed())

    def test_identical_sort_bag_context(self):
        e = ex()
        sort = {"class": PL + "Sort", "num-children": 1, "child": 0,
                "global": True, "order": [["same"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        self.assertEqual(self.yes(e)["sort_frames"], 1)

    def test_identical_filter_below_grouping(self):
        e = ex()
        f = {"class": PL + "Filter", "num-children": 1, "child": 0,
             "condition": [{"class": EX + "IsNotNull", "num-children": 1,
                            "child": 0}, att(0)]}
        e["old"].insert(1, copy.deepcopy(f))
        e["new"].insert(1, copy.deepcopy(f))
        self.yes(e)

    def test_bag_multiplicities_and_null_orbits_exhaustively(self):
        domain = (None, -1, 0, 1)
        def key_bag(rows, repeated):
            keys = [tuple([x] * repeated) for x in rows]
            return sorted([sum(1 for k in keys if k == q)
                           for q in set(keys)])
        for size in range(5):
            for bag in itertools.product(domain, repeat=size):
                self.assertEqual(key_bag(bag, 2), key_bag(bag, 1))
        self.yes(ex())

    def test_no_transition(self):
        e = ex()
        e["new"] = copy.deepcopy(e["old"])
        self.no(e)

    def test_last_key_dropped_empty_input_counterexample(self):
        e = ex()
        e["new"][0]["groupingExpressions"] = []
        self.no(e)
        # GROUP BY constant is a group aggregate: empty input has no groups.
        # Removing *all* grouping keys would change it to a scalar aggregate,
        # which returns one COUNT row even on the empty input.
        def grouped_count(rows):
            return [(len(rows),)] if rows else []
        def scalar_count(rows):
            return [(len(rows),)]
        self.assertNotEqual(grouped_count([]), scalar_count([]))
        self.assertEqual(grouped_count([1]), scalar_count([1]))

    def test_different_distinct_group_deleted(self):
        a, b = att(0), att(1)
        self.no(ex([[a], [b]]))

    def test_changing_grouping_key_expression(self):
        e = ex()
        e["new"][0]["groupingExpressions"] = [[att(1)]]
        self.no(e)

    def test_new_duplicate_remains(self):
        e = ex()
        e["old"][0]["groupingExpressions"].append([copy.deepcopy(att(0))])
        e["new"][0]["groupingExpressions"].append([copy.deepcopy(att(0))])
        self.no(e)

    def test_changed_aggregate_output(self):
        e = ex()
        e["new"][0]["aggregateExpressions"].pop()
        self.no(e)

    def test_changed_aggregate_metadata(self):
        e = ex()
        e["new"][0]["extra"] = 1
        self.no(e)

    def test_changed_input_value(self):
        e = ex()
        e["new"][-1]["output"][0][0]["exprId"]["id"] = 55
        self.no(e)

    def test_changed_filter(self):
        e = ex()
        e["old"].insert(1, {"class": PL + "Filter", "num-children": 1,
                            "child": 0, "condition": []})
        self.no(e)

    def test_changed_sort(self):
        e = ex()
        sort = {"class": PL + "Sort", "num-children": 1, "child": 0,
                "global": True, "order": [["a"]]}
        e["old"].insert(0, copy.deepcopy(sort))
        e["new"].insert(0, copy.deepcopy(sort))
        e["new"][0]["global"] = False
        self.no(e)

    def test_volatile_rand_cannot_be_deduplicated(self):
        rand = [{"class": EX + "Rand", "num-children": 0}]
        self.no(ex([rand, copy.deepcopy(rand)]))

    def test_zero_divisor_not_a_total_grouping_key(self):
        rem = [{"class": EX + "Remainder", "num-children": 2,
                "left": 0, "right": 1}, att(0),
               literal(0, "integer")]
        self.no(ex([rem, copy.deepcopy(rem)]))

    def test_unbound_expr_id(self):
        e = ex()
        e["old"][0]["groupingExpressions"][0][0]["exprId"]["id"] = 66
        self.no(e)

    def test_nondeterministic_aggregate_output(self):
        e = ex()
        e["old"][0]["aggregateExpressions"][0] = [
            {"class": EX + "Rand", "num-children": 0}]
        e["new"][0]["aggregateExpressions"][0] = copy.deepcopy(
            e["old"][0]["aggregateExpressions"][0])
        self.no(e)

    def test_unknown_scope(self):
        e = ex()
        e["old"].append({"class": PL + "AnotherChild", "num-children": 0})
        self.no(e)

    def test_malformed_serialized_group_expression(self):
        e = ex()
        e["old"][0]["groupingExpressions"][0].append(att(1))
        self.no(e)

    def test_real_spark_attribute_metadata_shape(self):
        e = ex()
        for plan, count in ((e["old"], 2), (e["new"], 1)):
            for group in plan[0]["groupingExpressions"][:count]:
                group[0].update({"metadata": {}, "nullable": True,
                                 "qualifier": []})
        self.yes(e)

    def test_real_spark_qualifier_string_shape(self):
        e = ex()
        for plan in (e["old"], e["new"]):
            for group in plan[0]["groupingExpressions"]:
                group[0].update({"qualifier": "List(integers)",
                                 "metadata": {}, "nullable": True})
        self.yes(e)

    def test_excessive_qualifier_string_rejected(self):
        e = ex()
        for plan in (e["old"], e["new"]):
            for group in plan[0]["groupingExpressions"]:
                group[0]["qualifier"] = "x" * 513
        self.no(e)

    def test_malformed_nullable_metadata_rejected(self):
        e = ex()
        for plan in (e["old"], e["new"]):
            for group in plan[0]["groupingExpressions"]:
                group[0]["nullable"] = "maybe"
        self.no(e)

    def test_untrusted_group_attribute_metadata(self):
        e = ex()
        e["old"][0]["groupingExpressions"][0][0]["nondeterministic"] = True
        e["new"][0]["groupingExpressions"][0][0]["nondeterministic"] = True
        self.no(e)


if __name__ == "__main__":
    unittest.main()
