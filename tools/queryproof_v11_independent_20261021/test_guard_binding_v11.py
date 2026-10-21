"""Adversarial, locally generated Catalyst-shaped trees: fail-closed binding tests."""
import copy
import unittest
from guard_binding_v11 import observe, Rejected, LOGICAL, EXPR

def attribute(i, name, dtype="integer"):
    return {"class": EXPR+"AttributeReference","num-children":0,
            "exprId":{"id":i,"jvmId":"test-jvm"},"name":name,
            "dataType":dtype,"nullable":True,"metadata":"{}","qualifier":[]}

def leaf(a):
    return ({"class":"org.apache.spark.sql.execution.LogicalRDD",
             "num-children":0,"output":[[copy.deepcopy(a)]]},[])

def side(a):
    return ({"class":LOGICAL+"Project","num-children":1,"child":0,
             "projectList":[[copy.deepcopy(a)]]},[leaf(a)])

def flatten(t):
    header, children = t
    return [header]+[n for child in children for n in flatten(child)]

def instance(kind="Inner", op="LessThan", guards=(False,True)):
    a,b=attribute(11,"a"),attribute(22,"b")
    compare={"class":EXPR+op,"num-children":2,"left":0,"right":1}
    j={"class":LOGICAL+"Join","num-children":2,"left":0,"right":1,
       "joinType":{"object":"org.apache.spark.sql.catalyst.plans."+kind+"$"},
       "condition":[compare,copy.deepcopy(a),copy.deepcopy(b)]}
    old_sides=[side(a),side(b)]
    new_sides=copy.deepcopy(old_sides)
    for i in (0,1):
        if guards[i]:
            value=(a,b)[i]
            f={"class":LOGICAL+"Filter","num-children":1,"child":0,
               "condition":[{"class":EXPR+"IsNotNull","num-children":1,"child":0},
                            copy.deepcopy(value)]}
            new_sides[i]=(f,[new_sides[i]])
    def wrap(parts):
        t=(copy.deepcopy(j),parts)
        for name in ("Project","Aggregate","Sort"):
            t=({"class":LOGICAL+name,"num-children":1,"child":0,
                **({"projectList":[[copy.deepcopy(a)]]} if name=="Project" else {})},[t])
        return t
    return {"case":"T","old":flatten(wrap(old_sides)),
            "new":flatten(wrap(new_sides))}

class BindingTests(unittest.TestCase):
    def test_valid_inner_lt_right(self):
        got=observe(instance())
        self.assertEqual((got["comparator"],got["join_kind"],
                          got["guard_left"],got["guard_right"]),
                         ("StrictLt","Inner",False,True))
    def test_valid_inner_eq_both(self):
        got=observe(instance(op="EqualTo",guards=(True,True)))
        self.assertTrue(got["guard_left"] and got["guard_right"])
    def test_valid_outer_right(self):
        got=observe(instance(kind="LeftOuter"))
        self.assertEqual(got["join_kind"],"LeftOuter")
    def reject(self, event):
        with self.assertRaises(Rejected):
            observe(event)
    def test_outer_preserved_side_rejected(self):
        self.reject(instance(kind="LeftOuter",guards=(True,False)))
    def test_null_safe_rejected(self):
        self.reject(instance(op="EqualNullSafe"))
    def test_or_operator_rejected(self):
        self.reject(instance(op="Or"))
    def test_no_changed_event_rejected(self):
        self.reject(instance(guards=(False,False)))
    def test_unbound_join_key_rejected(self):
        e=instance()
        for side in ("old","new"):
            j=next(x for x in e[side] if x["class"]==LOGICAL+"Join")
            j["condition"][2]["exprId"]["id"]=8675309
        self.reject(e)
    def test_guard_exprid_mismatch_rejected(self):
        e=instance()
        f=next(x for x in e["new"] if x["class"]==LOGICAL+"Filter")
        f["condition"][1]["exprId"]["id"]=123456
        self.reject(e)
    def test_unrecognized_join_fields_rejected(self):
        e=instance()
        next(x for x in e["new"] if x["class"]==LOGICAL+"Join")["unused"] = True
        self.reject(e)
    def test_guard_metadata_mismatch_rejected(self):
        e=instance()
        f=next(x for x in e["new"] if x["class"]==LOGICAL+"Filter")
        f["condition"][1]["metadata"]='{"test":"different"}'
        self.reject(e)
    def test_unsupported_sql_type_rejected(self):
        e=instance()
        f=next(x for x in e["new"] if x["class"]==LOGICAL+"Filter")
        f["condition"][1]["dataType"]="decimal"
        self.reject(e)
    def test_join_same_side_references_rejected(self):
        e=instance()
        for side in ("old","new"):
            j=next(x for x in e[side] if x["class"]==LOGICAL+"Join")
            j["condition"][2]=copy.deepcopy(j["condition"][1])
        self.reject(e)
    def test_malformed_preorder_rejected(self):
        e=instance()
        e["old"]=e["old"][:-1]
        self.reject(e)
    def test_incorrect_guard_form_rejected(self):
        e=instance()
        f=next(x for x in e["new"] if x["class"]==LOGICAL+"Filter")
        f["condition"][0]["class"]=EXPR+"IsNull"
        self.reject(e)

if __name__=="__main__": unittest.main()
