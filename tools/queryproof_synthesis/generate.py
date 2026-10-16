"""Generate Spark ordering counterexamples from a precommitted parameter manifest.

Expected output is computed independently using Python relational semantics;
discovery never reads a hardcoded expected Spark output. This is an empirical
test for one known bug family, not arbitrary SQL translation validation.
"""
from __future__ import annotations
import json,traceback
from collections import Counter
from dataclasses import dataclass,asdict,replace
from pathlib import Path
from typing import Any

ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT/"candidate_manifest.json").read_text())
OUT=Path("artifacts");OUT.mkdir(exist_ok=True)
@dataclass(frozen=True)
class Case:
    shape:str; join:str
    left_start:int; right_start:int
    left_n:int; right_n:int
    left_partitions:int; right_partitions:int
    modulus:int; offset:int; limit:int
    def valid(self):
        return (self.shape in ("cte","nested") and self.join in ("LEFT","INNER")
          and 1<=self.left_n<=40 and 1<=self.right_n<=40
          and 1<=self.left_partitions<=8 and 1<=self.right_partitions<=8
          and 1<=self.modulus<=10 and 0<=self.offset<self.left_n
          and 1<=self.limit<=self.left_n-self.offset
          and self.left_start>=0 and self.right_start>=0)

def sql(c):
    if not c.valid():raise ValueError(f"invalid case {c}")
    top=(f"SELECT id, CAST(id % {c.modulus} AS INT) AS k "
         f"FROM range({c.left_start},{c.left_start+c.left_n},1,{c.left_partitions}) "
         f"ORDER BY id ASC LIMIT {c.limit} OFFSET {c.offset}")
    rhs=(f"SELECT id AS bid, CAST(id % {c.modulus} AS INT) AS k "
         f"FROM range({c.right_start},{c.right_start+c.right_n},1,{c.right_partitions})")
    select=(f"SELECT top.id,top.k,rhs.bid FROM top {c.join} JOIN rhs USING(k) "
            "ORDER BY top.id ASC,rhs.bid ASC")
    if c.shape=="cte":return f"WITH top AS ({top}), rhs AS ({rhs}) "+select
    return (f"SELECT top.id,top.k,rhs.bid FROM ({top}) AS top {c.join} JOIN "
            f"({rhs}) AS rhs USING(k) ORDER BY top.id ASC,rhs.bid ASC")

def key(row):
    return (row[0], -1 if row[2] is None else row[2])

def reference(c):
    selected=list(range(c.left_start,c.left_start+c.left_n))[c.offset:c.offset+c.limit]
    right=range(c.right_start,c.right_start+c.right_n)
    out=[]
    for lid in selected:
        matching=[rid for rid in right if rid%c.modulus==lid%c.modulus]
        if matching:out.extend([[lid,lid%c.modulus,rid] for rid in matching])
        elif c.join=="LEFT":out.append([lid,lid%c.modulus,None])
    return sorted(out,key=key)

def bag(rows):
    return Counter(json.dumps(r,sort_keys=True,separators=(",",":")) for r in rows)

def diagnose(c,rows):
    expected=reference(c)
    same_bag=bag(rows)==bag(expected)
    monotonic=rows==sorted(rows,key=key)
    return {"oracle_bag_matches":same_bag,"order_monotonic":monotonic,
            "order_violation":bool(len(rows)>=2 and same_bag and not monotonic),
            "bag_violation":not same_bag,"row_count":len(rows)}

def settings(mode):
    cfg={"spark.sql.adaptive.enabled":"true","spark.sql.shuffle.partitions":"5"}
    if mode!="normal":
        cfg["spark.sql.optimizer.excludedRules"]=\
            "org.apache.spark.sql.catalyst.optimizer.InlineCTE"
    if mode=="aqe_off":cfg["spark.sql.adaptive.enabled"]="false"
    if mode=="broadcast_off":
        cfg["spark.sql.autoBroadcastJoinThreshold"]="-1"
        cfg["spark.sql.adaptive.autoBroadcastJoinThreshold"]="-1"
    return cfg

def run_query(spark,c,mode="candidate",probe=None):
    ss=spark.newSession()
    for name,val in settings(mode).items():ss.conf.set(name,val)
    q=ss.sql(sql(c))
    rows=[list(row) for row in q.collect()]
    result={"case":asdict(c),"sql":sql(c),"mode":mode,"rows":rows,
            "diagnosis":diagnose(c,rows)}
    if probe is not None:
        try:result["ordering_metadata"]=json.loads(probe.inspect(q._jdf.queryExecution()))
        except Exception as ex:result["ordering_metadata"]={"verdict":"UNKNOWN","error":repr(ex)}
    if result["diagnosis"]["order_violation"] or result["diagnosis"]["bag_violation"]:
        result["physical_plan"]=q._jdf.queryExecution().executedPlan().toString()
        result["optimized_plan"]=q._jdf.queryExecution().optimizedPlan().toString()
    return result

def complexity(c):
    return (c.left_n+c.right_n,c.left_partitions+c.right_partitions,
            c.offset+c.limit,c.left_start+c.right_start,c.modulus,
            0 if c.shape=="nested" else 1)

def reductions(c):
    seen=set()
    for name in ("left_n","right_n","left_partitions","right_partitions",
                 "offset","limit","left_start","right_start","modulus"):
        old=getattr(c,name)
        options=([1,old-1] if name in
            ("left_n","right_n","left_partitions","right_partitions","modulus","limit")
            else [0,old-1])
        for val in options:
            t=replace(c,**{name:val})
            if t.valid() and complexity(t)<complexity(c) and t not in seen:
                seen.add(t);yield t
    if c.shape=="cte":
        t=replace(c,shape="nested")
        if t.valid() and complexity(t)<complexity(c):yield t

def minimize(spark,initial,probe=None,budget=26):
    current=initial;history=[];attempts=0
    while attempts<budget:
        improved=False
        for candidate in sorted(reductions(current),key=complexity):
            if attempts>=budget:break
            attempts+=1
            try:
                a=run_query(spark,candidate,probe=probe)
                if not a["diagnosis"]["order_violation"]:continue
                b=run_query(spark,candidate,probe=probe)
                repeat=b["diagnosis"]["order_violation"]
                history.append({"params":asdict(candidate),"reproduced_twice":repeat})
                if repeat:current=candidate;improved=True;break
            except Exception as ex:
                history.append({"params":asdict(candidate),"error":repr(ex)})
        if not improved:break
    repeats=[run_query(spark,current,probe=probe) for _ in range(3)]
    return {"initial":asdict(initial),"minimized":asdict(current),
            "attempts":attempts,"history":history,
            "three_repetitions":repeats,
            "confirmed_three_of_three":all(x["diagnosis"]["order_violation"] for x in repeats)}

def main():
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master("local[2]")
           .appName("QueryProof-Counterexample-Synthesis")
           .config("spark.ui.enabled","false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    probe=spark._jvm.org.queryproof.PhysicalOrderingProbe
    result={"spark_version":spark.version,"seed":MANIFEST["seed"],
            "manifest_candidate_count":len(MANIFEST["cases"]),
            "cases":{},"summary":{},"minimization":None,"controls":{}}
    try:
        seed=MANIFEST["cases"][0]
        base=Case(**{k:v for k,v in seed.items() if k not in ("case_id","tier")})
        correct=reference(base)
        result["controls"]["reversed_detected"]=diagnose(base,list(reversed(correct)))["order_violation"]
        result["controls"]["correct_not_flagged"]=not diagnose(base,correct)["order_violation"]
        if not all(result["controls"].values()):raise RuntimeError("oracle sanity controls failed")
        witnesses=[]
        for entry in MANIFEST["cases"]:
            cid=entry["case_id"]
            case=Case(**{k:v for k,v in entry.items() if k not in ("case_id","tier")})
            try:
                row=run_query(spark,case,probe=probe)
                record={"status":"OBSERVED","tier":entry["tier"],"params":asdict(case),
                        "rows":row["rows"],"diagnosis":row["diagnosis"],
                        "ordering_metadata":row.get("ordering_metadata",{})}
                if row["diagnosis"]["order_violation"] or row["diagnosis"]["bag_violation"]:
                    record["sql"]=row["sql"]
                    record["physical_plan"]=row.get("physical_plan")
                if row["diagnosis"]["order_violation"]:witnesses.append(cid)
                result["cases"][cid]=record
            except Exception as ex:
                result["cases"][cid]={"status":"ERROR","tier":entry["tier"],
                                     "params":asdict(case),"error":repr(ex),
                                     "trace":traceback.format_exc()[-2000:]}
            rec=result["cases"][cid]
            print("QUERYPROOF_GENERATED",spark.version,cid,entry["tier"],
                  rec["status"],rec.get("diagnosis",{}).get("order_violation"),
                  rec.get("ordering_metadata",{}).get("verdict"),flush=True)
        generated=[cid for cid in witnesses if result["cases"][cid]["tier"]!="known_calibration"]
        if generated:
            smallest=min(generated,key=lambda cid:complexity(Case(**result["cases"][cid]["params"])))
            initial=Case(**result["cases"][smallest]["params"])
            result["minimization"]=minimize(spark,initial,probe)
            if result["minimization"]["confirmed_three_of_three"]:
                c=Case(**result["minimization"]["minimized"])
                for mode in ("normal","aqe_off","broadcast_off"):
                    result["controls"][mode]=[run_query(spark,c,mode,probe=probe)
                                               for _ in range(3)]
        result["summary"]={
            "cases_run":sum(r["status"]=="OBSERVED" for r in result["cases"].values()),
            "error_cases":sum(r["status"]=="ERROR" for r in result["cases"].values()),
            "generated_order_violations":len(generated),
            "calibration_order_violations":len(witnesses)-len(generated),
            "bag_violations":sum(r.get("diagnosis",{}).get("bag_violation",False)
                                 for r in result["cases"].values()),
            "minimized_3_of_3":bool(result["minimization"] and
                                   result["minimization"]["confirmed_three_of_three"]),
            "minimized_case":result["minimization"]["minimized"] if result["minimization"] else None
        }
    finally:
        spark.stop()
        (OUT/f"generated_counterexamples_{result['spark_version']}.json").write_text(
            json.dumps(result,indent=2,default=str))
        print("QUERYPROOF_GENERATED_SUMMARY",spark.version,json.dumps(result["summary"]),flush=True)
    # Failure to generate new cases is a legitimate result. Query errors are not.
    return 2 if (result["summary"]["error_cases"] or
                 result["summary"]["cases_run"]!=len(MANIFEST["cases"])) else 0

if __name__=="__main__":
    raise SystemExit(main())
