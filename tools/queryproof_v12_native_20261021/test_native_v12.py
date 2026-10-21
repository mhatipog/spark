"""V12 independently executable negative controls for source-attribute binding."""
import copy
import hashlib
import json
import unittest

from run_v12 import (AttestationRejected, native_frame, compare_one, verify, SCHEMA)

def sample():
    a={"class":"org.apache.spark.sql.catalyst.expressions.AttributeReference",
       "num-children":0,"exprId":{"id":13,"jvmId":"trusted-jvm"},
       "name":"key","dataType":"integer","nullable":True}
    raw=json.dumps([a],separators=(",",":"))
    native={"schema":SCHEMA,
            "json_sha256":hashlib.sha256(raw.encode()).hexdigest(),
            "source_plan_nodes":1,"source_expression_nodes":1,
            "source_attribute_occurrences":1,
            "nonempty_metadata_occurrences":0,"tagged_nodes":0,
            "attributes":[{
                "path":"/root/expr0",
                "expr_id":13,"jvm_id":"trusted-jvm",
                "class":a["class"],
                "name_sha256":hashlib.sha256(b"key").hexdigest(),
                "dtype":json.dumps("integer"),"nullable":True,
                "metadata_empty":True,
                "metadata_sha256":hashlib.sha256(b"{}").hexdigest()
            }]}
    return raw,native

class NativeBindingAdversarial(unittest.TestCase):
    def rejected(self,raw,frame,reason=None):
        with self.assertRaises(AttestationRejected) as caught:
            compare_one(raw,frame)
        if reason:
            self.assertEqual(str(caught.exception),reason)
    def test_good_source_metadata(self):
        raw,f=sample()
        self.assertEqual(compare_one(raw,native_frame(json.dumps(f)))
                         ["verified_json_attribute_bindings"],1)
        self.assertTrue(verify(raw,raw,{"pre":f,"post":f,"effective":True})
                        ["unknown_hidden_effects"])
    def test_erased_metadata_must_fail_closed(self):
        raw,f=sample()
        f["attributes"][0]["metadata_empty"]=False
        f["nonempty_metadata_occurrences"]=1
        self.rejected(raw,f,"SOURCE_NATIVE_METADATA_NOT_EMPTY")
    def test_native_metadata_counter_conflict(self):
        raw,f=sample()
        f["nonempty_metadata_occurrences"]=1
        self.rejected(raw,f,"NATIVE_METADATA_COUNT_CONTRADICTION")
    def test_jvm_id_swap_caught(self):
        raw,f=sample()
        f["attributes"][0]["jvm_id"]="other-jvm"
        self.rejected(raw,f,"NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    def test_expr_id_swap_caught(self):
        raw,f=sample()
        f["attributes"][0]["expr_id"]=222
        self.rejected(raw,f,"NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    def test_attribute_name_swap_caught(self):
        raw,f=sample()
        f["attributes"][0]["name_sha256"]=hashlib.sha256(b"other").hexdigest()
        self.rejected(raw,f,"NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    def test_nullable_swap_caught(self):
        raw,f=sample()
        f["attributes"][0]["nullable"]=False
        self.rejected(raw,f,"NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    def test_dtype_swap_caught(self):
        raw,f=sample()
        f["attributes"][0]["dtype"]=json.dumps("long")
        self.rejected(raw,f,"NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    def test_raw_json_hash_mismatch_caught(self):
        raw,f=sample()
        f["json_sha256"]="0"*64
        self.rejected(raw,f,"JVM_NATIVE_PRE_POST_JSON_BINDING_MISMATCH")
    def test_prepost_metadata_change_detected(self):
        raw,f=sample()
        f2=copy.deepcopy(f)
        f2["attributes"][0]["metadata_empty"]=False
        f2["nonempty_metadata_occurrences"]=1
        with self.assertRaises(AttestationRejected):
            verify(raw,raw,{"pre":f,"post":f2,"effective":True})
    def test_ineffective_rule_must_not_count(self):
        raw,f=sample()
        with self.assertRaises(AttestationRejected):
            verify(raw,raw,{"pre":f,"post":f,"effective":False})
    def test_duplicate_path_rejected(self):
        _,f=sample()
        f["attributes"].append(copy.deepcopy(f["attributes"][0]))
        f["source_attribute_occurrences"]=2
        with self.assertRaises(AttestationRejected):
            native_frame(json.dumps(f))
    def test_malformed_native_digest_rejected(self):
        _,f=sample()
        f["attributes"][0]["name_sha256"]="invalid"
        with self.assertRaises(AttestationRejected):
            native_frame(json.dumps(f))

if __name__=="__main__": unittest.main()
