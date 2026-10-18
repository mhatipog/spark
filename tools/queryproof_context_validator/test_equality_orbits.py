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


    def test_seeded_orbit_cartesian_equivalence(self):
        """300 generated expression-pair checks against full finite Cartesian grid."""
        import json
        import random
        from itertools import product
        from equality_orbits import attributes, evaluate, distinguish, PARTITIONS
        rng=random.Random(20261018)
        for n in (2,3,4):
            keys=[json.dumps({"id":attr(i)["exprId"],"type":"integer"},
                             sort_keys=True) for i in range(n)]
            def make(depth):
                if depth==0 or rng.random()<0.4:
                    x=("attr",rng.choice(keys))
                    y=("attr",rng.choice(keys))
                    return (rng.choice(("EqualTo","EqualNullSafe")),x,y)
                kind=rng.choice(("Not","And","Or","IsNull","IsNotNull"))
                if kind in ("Not","IsNull","IsNotNull"):
                    return (kind,make(depth-1))
                return (kind,make(depth-1),make(depth-1))
            for _ in range(100):
                a,b=make(3),make(3)
                orbit=classify_trees(a,b,"FULL_3VL",allow_remap=True)
                ids=sorted(attributes(a)|attributes(b))
                domain=(None,*range(n))
                all_pairs=[]
                for vals in product(domain,repeat=len(ids)):
                    bindings=dict(zip(ids,vals))
                    all_pairs.append((evaluate(a,bindings),evaluate(b,bindings)))
                brute=[name for name in PARTITIONS
                       if all(not distinguish(x,y,name) for x,y in all_pairs)]
                self.assertEqual(orbit["safe_partitions"],brute)
                self.assertEqual(orbit["orbits_checked"],bell_number(len(ids)+1))

    def test_filter_observer_certificates_and_fail_closed(self):
        from equality_orbits import certify_predicate_event
        before, after = known_pair()
        unary = {"class": "org.apache.spark.sql.catalyst.plans.logical.Filter",
                 "num-children": 1}
        child = {"class": "dummy.Table", "num-children": 0}
        def filter_event(p, q):
            return {"old": [{**unary, "condition": p}, child],
                    "new": [{**unary, "condition": q}, child],
                    "rule": "unseen_rule_identity"}
        for n in (2, 3, 4):
            p, q = before, after
            for i in range(2, n):
                sibling = expr("IsNotNull", [attr(i)])
                p, q = expr("And", p, sibling), expr("And", q, sibling)
            yes = certify_predicate_event(filter_event(p, q))
            self.assertEqual(yes["status"], "EQUIVALENT_IN_FRAGMENT")
            self.assertEqual(yes["operator"], "Filter")
            self.assertEqual(yes["root"]["orbits_checked"], bell_number(n + 1))
            self.assertTrue(all(x["verified"] for x in yes["independent_verification"]))
            no = certify_predicate_event(filter_event(expr("Not", p), expr("Not", q)))
            self.assertEqual(no["status"], "COUNTEREXAMPLE")
            self.assertEqual(no["operator"], "Filter")
            self.assertEqual(no["required_partition"], "FALSE_ONLY")
        case = filter_event(before, after)
        case["new"][1] = {"class": "dummy.DifferentTable", "num-children": 0}
        self.assertEqual(certify_predicate_event(case)["status"], "UNKNOWN")
        case = filter_event(before, after)
        case["new"][0]["extra"] = "metadata changed"
        self.assertEqual(certify_predicate_event(case)["status"], "UNKNOWN")
        case = filter_event(before, after)
        case["old"][0]["class"] = "org.apache.spark.sql.catalyst.plans.logical.Project"
        self.assertEqual(certify_predicate_event(case)["status"], "UNKNOWN")
        self.assertEqual(certify_join_event(filter_event(before, after))["status"], "UNKNOWN")

    def test_filter_context_transitive_masking(self):
        from equality_orbits import certify_predicate_event
        a, b, c = [[attr(i)] for i in range(3)]
        before = expr("And", expr("EqualTo", a, b), expr("EqualTo", b, c))
        after = expr("And", expr("EqualTo", a, b), expr("EqualTo", a, c))
        shape = {"class": "org.apache.spark.sql.catalyst.plans.logical.Filter",
                 "num-children": 1}
        event = {"old": [{**shape, "condition": before}],
                 "new": [{**shape, "condition": after}]}
        result = certify_predicate_event(event)
        self.assertEqual(result["operator"], "Filter")
        self.assertEqual(result["status"], "EQUIVALENT_IN_FRAGMENT")
        self.assertTrue(result["semantic_context_masking"])
        self.assertEqual(result["local_guard"], "BLOCK")

if __name__ == "__main__":
    unittest.main()
