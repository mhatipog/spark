"""Synthetic proof controls: no historical oracle rows and no rule-name hints."""
import unittest
from equality_orbits import (canonical_representatives, bell_number,
    transfer_observation, certify_join_event, classify_trees, parse_expression)

def attr(i):
    return {"class": "org.apache.spark.sql.catalyst.expressions.AttributeReference",
            "num-children": 0, "name": "v" + str(i), "dataType": "integer",
            "exprId": {"id": i + 1, "jvmId": "synthetic"}}

def expr(name, *children):
    return [{"class": "org.apache.spark.sql.catalyst.expressions." + name,
             "num-children": len(children)}] + [
                 node for child in children for node in child]

def known_pair():
    a,b = [attr(i) for i in range(2)]
    left,right = [a],[b]
    before = expr("Or", expr("EqualTo", left, right),
                  expr("And", expr("IsNull",left), expr("IsNull",right)))
    after = expr("EqualNullSafe", left, right)
    return before,after

def join_event(before,after):
    join = {"class": "org.apache.spark.sql.catalyst.plans.logical.Join",
            "num-children": 2,
            "joinType": {"object": "org.apache.spark.sql.catalyst.plans.Inner$"}}
    tail = [{"class": "dummy.Table", "num-children": 0}]*2
    return {"old": [{**join, "condition": before}, *tail],
            "new": [{**join, "condition": after}, *tail],
            "rule": "no_known_rule_name"}

def extend(n, negative=False):
    before, after = known_pair()
    if negative:
        before,after = expr("Not",before),expr("Not",after)
    for i in range(2,n):
        sibling = expr("IsNotNull",[attr(i)])
        before,after = expr("And",before,sibling),expr("And",after,sibling)
    return join_event(before,after)

class EqualityOrbitTests(unittest.TestCase):
    def test_bell_counts_and_unique_representatives(self):
        for n, count in ((1,2),(2,5),(3,15),(4,52),(5,203),(6,877),(7,4140)):
            valuations=list(canonical_representatives(n))
            self.assertEqual(len(valuations),count)
            self.assertEqual(len(set(valuations)),count)
            self.assertEqual(bell_number(n+1),count)

    def test_observation_pullback(self):
        self.assertEqual(transfer_observation("TRUE_ONLY","Not",0),"FALSE_ONLY")
        self.assertEqual(transfer_observation("TRUE_ONLY","IsNull",0),"NULL_ONLY")
        self.assertEqual(transfer_observation("TRUE_ONLY","And",0),"TRUE_ONLY")
        self.assertEqual(transfer_observation("NULL_ONLY","And",0),"FULL_3VL")

    def test_safe_and_unsafe_real_ast_shapes(self):
        for n in (2,3,4,6):
            for negative in (False,True):
                with self.subTest(n=n,negative=negative):
                    result=certify_join_event(extend(n,negative))
                    status="COUNTEREXAMPLE" if negative else "EQUIVALENT_IN_FRAGMENT"
                    self.assertEqual(result["status"],status)
                    self.assertEqual(result["root"]["orbits_checked"],bell_number(n+1))
                    self.assertEqual(result["required_partition"],
                                     "FALSE_ONLY" if negative else "TRUE_ONLY")
                    self.assertTrue(all(v["verified"]
                                        for v in result["independent_verification"]))

    def test_third_variable_equality_transitivity_control(self):
        a,b,c = [[attr(i)] for i in range(3)]
        before=expr("And",expr("EqualTo",a,b),expr("EqualTo",b,c))
        after=expr("And",expr("EqualTo",a,b),expr("EqualTo",a,c))
        positive=certify_join_event(join_event(before,after))
        self.assertEqual(positive["status"],"EQUIVALENT_IN_FRAGMENT")
        self.assertEqual(positive["root"]["orbits_checked"],15)
        self.assertTrue(positive["semantic_context_masking"])
        self.assertEqual(positive["local_guard"],"BLOCK")
        negative=certify_join_event(join_event(expr("Not",before),expr("Not",after)))
        self.assertEqual(negative["status"],"COUNTEREXAMPLE")
        self.assertEqual(negative["required_partition"],"FALSE_ONLY")

    def test_mismatched_or_unsupported_types_fail_closed(self):
        v=extend(3,True)
        v["new"][0]["condition"][1]["class"]="org.example.Unsupported"
        self.assertEqual(certify_join_event(v)["status"],"UNKNOWN")
        v=extend(3,True)
        v["new"][0]["joinType"]={"object":"org.apache.spark.sql.catalyst.plans.LeftOuter"}
        self.assertEqual(certify_join_event(v)["status"],"UNKNOWN")
        v=extend(3,True)
        v["new"][0]["condition"][0]["class"]="org.apache.spark.sql.catalyst.expressions.EqualTo"
        self.assertEqual(certify_join_event(v)["status"],"UNKNOWN")
        v=extend(3,True)
        # Changing the resolved type invalidates attribute identity.
        v["new"][0]["condition"][3]["dataType"]="boolean"
        self.assertEqual(certify_join_event(v)["status"],"UNKNOWN")

if __name__ == "__main__":
    unittest.main()
