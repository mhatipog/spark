"""No-Spark tests for the frozen holdout's selection, decoding and scoring."""
import base64
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "queryproof_context_validator"))
from test_equality_orbits import attr, expr, known_pair, join_event
from run_holdout import canonical_bag, _rule_basename, analyze_trace

def wire(event):
    fields = [event.get("case", "Z01"), "Optimizer", "Optimization",
              event["rule"], "1", json.dumps(event["old"]), json.dumps(event["new"])]
    return "QUERYPROOF_AST\t" + "\t".join(
        base64.b64encode(x.encode("utf8")).decode("ascii") for x in fields)

def filter_event(rule):
    before, after = known_pair()
    op = {"class": "org.apache.spark.sql.catalyst.plans.logical.Filter",
          "num-children": 1}
    child = {"class": "dummy.Table", "num-children": 0}
    return {"old": [{**op, "condition": before}, child],
            "new": [{**op, "condition": after}, child],
            "case": "Z02", "rule": rule}

class FrozenHoldoutTests(unittest.TestCase):
    def test_frozen_cases_parse_in_sqlite(self):
        manifest = json.loads((HERE / "CASES.json").read_text())
        protocol = json.loads((HERE / "FROZEN_PROTOCOL.json").read_text())
        self.assertEqual(manifest["benchmark_id"], protocol["benchmark_id"])
        self.assertEqual(len(manifest["cases"]), 18)
        self.assertEqual(len(set(x["id"] for x in manifest["cases"])), 18)
        db = sqlite3.connect(":memory:")
        for table, rows in manifest["source_rows"].items():
            db.execute("CREATE TABLE " + table + " (a INTEGER, b INTEGER, c INTEGER)")
            db.executemany("INSERT INTO " + table + " VALUES (?,?,?)", rows)
        for c in manifest["cases"]:
            with self.subTest(case=c["id"]):
                bag = canonical_bag(db.execute(c["sql"]).fetchall())
                self.assertIsInstance(bag["sha256"], str)
        db.close()

    def test_multiset_hash_is_order_invariant_and_multiplicity_sensitive(self):
        x = [(1, None), (2, 3), (1, None)]
        self.assertEqual(canonical_bag(x), canonical_bag(list(reversed(x))))
        self.assertNotEqual(canonical_bag(x), canonical_bag(x[:2]))
        self.assertEqual(_rule_basename("a.b.OptimizeJoinCondition$"),
                         "OptimizeJoinCondition")

    def test_independently_frozen_rule_filtering_after_scoring(self):
        before, after = known_pair()
        known = join_event(before, after)
        known.update({"case": "Z01", "rule": "a.b.OptimizeJoinCondition"})
        unseen = filter_event("a.b.NewBooleanRule")
        unknown = filter_event("a.b.NewBooleanRule")
        unknown["new"][1] = {"class": "dummy.ChangedTable", "num-children": 0}
        raw = "\n".join(wire(x) for x in (known, unseen, unknown)) + "\n"
        with patch.object(Path, "read_text", return_value=raw):
            result = analyze_trace(Path("/virtual-only/trace.txt"),
                                   {"OptimizeJoinCondition", "RewritePredicateSubquery"})
        self.assertEqual(result["trace_events"], 3)
        self.assertEqual(result["all_events"]["statuses"]["EQUIVALENT_IN_FRAGMENT"], 2)
        self.assertEqual(result["unseen_rule_events"]["events"], 2)
        self.assertEqual(result["unseen_rule_events"]["statuses"]["EQUIVALENT_IN_FRAGMENT"], 1)
        self.assertEqual(result["unseen_rule_events"]["statuses"]["UNKNOWN"], 1)
        self.assertEqual(result["per_operator"]["Filter"]["EQUIVALENT_IN_FRAGMENT"], 1)
        self.assertEqual(result["per_operator"]["UNKNOWN"]["UNKNOWN"], 1)

    def test_instrumentation_error_is_reported_not_silenced(self):
        with patch.object(Path, "read_text",
                          return_value="QUERYPROOF_AST_ERROR\tjava.lang.Exception\n"):
            result = analyze_trace(Path("/virtual-only/trace.txt"), set())
        self.assertEqual(result["trace_events"], 0)
        self.assertEqual(len(result["instrumentation_failures"]), 1)

    def test_corrupt_ast_is_reported_not_accepted(self):
        with patch.object(Path, "read_text",
                          return_value="QUERYPROOF_AST\tBROKEN\n"):
            result = analyze_trace(Path("/virtual-only/trace.txt"), set())
        self.assertEqual(result["trace_events"], 0)
        self.assertEqual(len(result["decode_failures"]), 1)


if __name__ == "__main__":
    unittest.main()
