"""V12 fail-closed paired JVM-source witness validator.

V10/V11 verify serialized Catalyst syntax against a Coq key-bag law.
V12 links those same events to actual, pre-rule JVM AttributeReferences and
their source Metadata, which Catalyst TreeNode.toJSON can erase.

This verifies observational binding for specific fields, NOT JVM completeness,
optimizer trace soundness, SQLConf, plan frame purity or Coq translation.
"""
import base64
from collections import defaultdict, Counter
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parents[2]
for item in ("queryproof_v11_independent_20261021",
             "queryproof_v10_guard_kernel_20261021",
             "queryproof_instrumented", "queryproof_holdout_20261018"):
    sys.path.insert(0,str(ROOT/"tools"/item))
import run_v11
import run_v10
import run_external
from guard_binding_v11 import observe, EXPR
from rank_ast import decode

SCHEMA="queryproof.v12.native-attribute-observer.v1"
HEX_SHA=re.compile(r"^[0-9a-f]{64}$")

class AttestationRejected(ValueError):
    pass

def insist(ok,reason):
    if not ok:
        raise AttestationRejected(reason)

def read_b64(line, marker, n_fields):
    insist(line.startswith(marker+"\t"),"WRONG_NATIVE_MARKER")
    parts=line.rstrip("\n").split("\t")[1:]
    insist(len(parts)==n_fields,"NATIVE_FIELD_ARITY")
    try:
        return [base64.b64decode(p,validate=True).decode("utf-8") for p in parts]
    except (ValueError,UnicodeDecodeError) as exc:
        raise AttestationRejected("INVALID_NATIVE_BASE64") from exc

def key(values):
    return tuple(values[:5])

def native_frame(frame_text):
    try: result=json.loads(frame_text)
    except (TypeError,ValueError) as exc:
        raise AttestationRejected("MALFORMED_NATIVE_JSON") from exc
    insist(isinstance(result,dict) and result.get("schema")==SCHEMA,
           "NATIVE_FRAME_SCHEMA_NOT_V12")
    for field in ("json_sha256",):
        insist(isinstance(result.get(field),str) and
               HEX_SHA.fullmatch(result[field]),"INVALID_SHA256")
    for field in ("source_plan_nodes","source_expression_nodes",
                  "source_attribute_occurrences",
                  "nonempty_metadata_occurrences","tagged_nodes"):
        insist(type(result.get(field)) is int and 0<=result[field]<=30000,
               "INVALID_NATIVE_FRAME_COUNTER_"+field)
    attrs=result.get("attributes")
    insist(isinstance(attrs,list) and
           result["source_attribute_occurrences"]==len(attrs) and len(attrs)>0,
           "NATIVE_ATTRIBUTES_MISSING")
    insist(result["source_plan_nodes"]>0,"NATIVE_PLAN_NOT_WALKED")
    paths=set()
    for a in attrs:
        insist(isinstance(a,dict) and set(a)=={
            "path","expr_id","jvm_id","class","name_sha256",
            "dtype","nullable","metadata_empty","metadata_sha256"},
            "NATIVE_ATTRIBUTE_FIELDS")
        insist(isinstance(a["path"],str) and a["path"].startswith("/root/") and
               len(a["path"])<512 and a["path"] not in paths,
               "NATIVE_ATTRIBUTE_PATH_COLLISION")
        paths.add(a["path"])
        insist(type(a["expr_id"]) is int and isinstance(a["jvm_id"],str) and
               type(a["nullable"]) is bool and
               type(a["metadata_empty"]) is bool and
               a["class"]==EXPR+"AttributeReference" and
               isinstance(a["dtype"],str) and len(a["dtype"])<16384 and
               HEX_SHA.fullmatch(a["name_sha256"]) and
               HEX_SHA.fullmatch(a["metadata_sha256"]),
               "BAD_NATIVE_ATTRIBUTE_TYPES")
    return result

def walk_json_objects(value):
    if isinstance(value,dict):
        yield value
        for x in value.values(): yield from walk_json_objects(x)
    elif isinstance(value,list):
        for x in value: yield from walk_json_objects(x)

def sig_native(a):
    try: dtype=json.loads(a["dtype"])
    except (TypeError,ValueError) as exc:
        raise AttestationRejected("SOURCE_DATATYPE_NOT_JSON") from exc
    return (a["expr_id"],a["jvm_id"],a["name_sha256"],
            dtype,a["nullable"])

def sig_json(a):
    ident=a.get("exprId")
    insist(isinstance(ident,dict) and type(ident.get("id")) is int and
           isinstance(ident.get("jvmId"),str) and
           isinstance(a.get("name"),str) and
           isinstance(a.get("dataType"),str) and
           type(a.get("nullable")) is bool,
           "JSON_ATTRIBUTE_BINDING_MALFORMED")
    digest=hashlib.sha256(a["name"].encode("utf-8")).hexdigest()
    return (ident["id"],ident["jvmId"],digest,
            a["dataType"],a["nullable"])

def compare_one(raw_json,native):
    expected_hash=hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    insist(expected_hash==native["json_sha256"],
           "JVM_NATIVE_PRE_POST_JSON_BINDING_MISMATCH")
    try: parsed=json.loads(raw_json)
    except ValueError as exc:
        raise AttestationRejected("NATIVE_RAW_AST_INVALID_JSON") from exc
    insist(isinstance(parsed,list),"NATIVE_RAW_AST_NOT_PREORDER")
    serialized={
        sig_json(node) for node in walk_json_objects(parsed)
        if node.get("class")==EXPR+"AttributeReference"
    }
    source={sig_native(a) for a in native["attributes"]}
    insist(serialized and serialized<=source,
           "NATIVE_SOURCE_ATTRIBUTE_BINDING_NOT_COVERING_JSON")
    insist(native["nonempty_metadata_occurrences"]==
           sum(not a["metadata_empty"] for a in native["attributes"]),
           "NATIVE_METADATA_COUNT_CONTRADICTION")
    insist(native["nonempty_metadata_occurrences"]==0,
           "SOURCE_NATIVE_METADATA_NOT_EMPTY")
    return {
        "verified_json_attribute_bindings":len(serialized),
        "source_attribute_occurrences":len(native["attributes"]),
        "metadata_empty":True,
        "native_tagged_nodes":native["tagged_nodes"],
    }

def verify(raw_before,raw_after,record):
    insist(record["effective"],"NATIVE_EVENT_NOT_EFFECTIVE")
    old=compare_one(raw_before,record["pre"])
    new=compare_one(raw_after,record["post"])
    return {
        "source_attribute_observation":"META_EMPTY_NATIVE_SOURCE",
        "before":old,"after":new,
        "unknown_hidden_effects":True, # never derive semantic noninterference
    }

def parse_events(path):
    ast=defaultdict(list)
    native=defaultdict(list)
    unknown=Counter()
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("QUERYPROOF_AST\t"):
            f=read_b64(line,"QUERYPROOF_AST",7)
            ast[key(f)].append(dict(raw_before=f[5],raw_after=f[6]))
        elif line.startswith("QUERYPROOF_NATIVE_V12\t"):
            f=read_b64(line,"QUERYPROOF_NATIVE_V12",8)
            insist(f[5] in ("true","false"),"NATIVE_EFFECT_FLAG_MALFORMED")
            native[key(f)].append({
                "effective":f[5]=="true",
                "pre":native_frame(f[6]),
                "post":native_frame(f[7]),
            })
        elif line.startswith("QUERYPROOF_NATIVE_V12_UNKNOWN\t"):
            f=read_b64(line,"QUERYPROOF_NATIVE_V12_UNKNOWN",7)
            unknown[f[6]]+=1
    return ast,native,unknown

def score_v12(path):
    report=run_v11.score_v11(path)
    expected_witnesses=report["v10_coq_guard_witnesses"]["witnesses"]
    ast,native,unknown=parse_events(path)
    accepted=[]
    rejected=[]
    for witness in expected_witnesses:
        case=witness["case"]
        matching=[(k,x) for k,series in ast.items() if k[0]==case and
                  k[3].endswith("InferFiltersFromConstraints") for x in series
                  if run_v10.parse_witness(dict(
                      case=case,rule=k[3],old=json.loads(x["raw_before"]),
                      new=json.loads(x["raw_after"]))) is not None]
        insist(len(matching)==1,"NATIVE_CANDIDATE_NOT_UNIQUE_"+case)
        k, raw=matching[0]
        group=native.get(k,[])
        if len(group)!=1:
            rejected.append({"case":case,"reason":"NATIVE_WITNESS_MISSING_OR_DUPLICATE"})
            continue
        try:
            attestation=verify(raw["raw_before"],raw["raw_after"],group[0])
        except AttestationRejected as exc:
            rejected.append({"case":case,"reason":str(exc)})
        else:
            accepted.append({"case":case,**attestation})
    report["v12_native_metadata_binding"]={
        "status":"RETROSPECTIVE_NATIVE_JVM_SOURCE_OBSERVATION",
        "source_metadata_attested_cases":sorted(x["case"] for x in accepted),
        "source_metadata_attested_count":len(accepted),
        "unattested_cases":rejected,
        "native_unknown_markers":dict(unknown),
        "all_live_coq_witnesses":len(expected_witnesses),
        "attestations":accepted,
        "limits":[
            "Attests actual JVM AttributeReference metadata and identities pre/post rule",
            "JVM observation excludes unmodelled state, tags, mutable caches, and SQLConf",
            "A hash binds raw observed JSON bytes but does not prove injective source encoding",
            "Source-visitor completeness and JVM-to-Coq meaning are NOT machine proved",
            "Coq proves only narrow nullable-key bag semantics, not full SQL outputs",
            "W32 is historical retrospective, never prospective",
        ],
    }
    return report

run_external.union_coverage=score_v12
run_external.OUTPUT=ROOT/"artifacts/queryproof_v12_native_source_metadata_observations.json"

if __name__=="__main__":
    raise SystemExit(run_external.parent())
