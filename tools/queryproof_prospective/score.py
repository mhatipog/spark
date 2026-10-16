"""Read-only score of preregistered held-out Spark runs.

This module is invoked AFTER the Spark queries and never imports the discovery
tools. It reports reproduced bugs, controls, false positives and unsupported.
"""
import json,sys
from collections import Counter
from pathlib import Path
ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT/"manifest.json").read_text())
def canon(rows,mode):
    return json.dumps(rows,sort_keys=True,default=str) if mode=="sequence" else \
        sorted(json.dumps(r,sort_keys=True,default=str) for r in rows)
def main(path):
    data=json.loads(Path(path).read_text())
    output={"version":data["spark_version"],"manifest_sha256":data["manifest_sha256"],
            "cases":{},"summary":{}}
    for c in MANIFEST["cases"]:
        record=data["cases"][c["id"]]
        if record["status"]!="OBSERVED":
            out={"status":"UNAVAILABLE","detail":record.get("error")}
        else:
            values=[canon(x["rows"],c["comparison"]) for x in record["observations"]]
            expected=canon(c["expected"],c["comparison"])
            control=record.get("control")
            ctrl=canon(control["rows"],c["comparison"]) if control else None
            wrong=[x!=expected for x in values]
            disagreement=bool(ctrl is not None and values[0]!=ctrl)
            out={"status":("BUG_REPRODUCED" if c["kind"]=="bug" and any(wrong) else
                          "CONTROL_INCORRECT" if c["kind"]=="negative_control" and any(wrong) else
                          "BUG_NOT_REPRODUCED" if c["kind"]=="bug" else "CONTROL_CORRECT"),
                 "wrong_observations":sum(wrong),"total_observations":len(values),
                 "baseline_repeated_consistent":len(set(map(str,values)))==1,
                 "config_or_sql_disagreement":disagreement,
                 "control_correct":bool(ctrl==expected) if ctrl is not None else None,
                 "expected":c["expected"]}
        output["cases"][c["id"]]=out
    from collections import Counter
    output["summary"]=dict(Counter(r["status"] for r in output["cases"].values()))
    dest=Path("artifacts")/("prospective_score_"+data["spark_version"]+".json")
    dest.write_text(json.dumps(output,indent=2))
    print("QUERYPROOF_SCORE",data["spark_version"],output["summary"],flush=True)
    for k,r in output["cases"].items():
        print("QUERYPROOF_SCORE_CASE",k,r["status"],
              "disagreement",r.get("config_or_sql_disagreement"),flush=True)
if __name__=="__main__":main(sys.argv[1])
