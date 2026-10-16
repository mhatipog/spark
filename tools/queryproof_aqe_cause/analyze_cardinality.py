"""Decode JVM cardinality observations. Fail closed on unknown field structure."""
import base64,json,sys
from pathlib import Path

FIELDS=["case","sort","child","child_maxRows","child_stats_rowCount",
        "join_maxRows","left_maxRows","right_maxRows",
        "left_stats_rowCount","right_stats_rowCount",
        "left_node","right_node","sort_tree"]
def main():
    source=Path(sys.argv[1]).read_text(errors="replace").splitlines()
    events=[];errors=[]
    for i,line in enumerate(source,1):
        loc=line.find("QUERYPROOF_CARDINALITY\t")
        if loc<0:continue
        raw=line[loc:].split("\t")[1:]
        if len(raw)!=len(FIELDS):
            errors.append({"line":i,"fields":len(raw)});continue
        try:
            obj=dict(zip(FIELDS,[
                base64.b64decode(x,validate=True).decode() for x in raw
            ]))
            obj["line"]=i;events.append(obj)
        except Exception as exc:
            errors.append({"line":i,"error":repr(exc)})
    bycase={}
    for e in events:
        bycase.setdefault(e["case"],[]).append(e)
    summary={name:[
         {k:v for k,v in x.items() if k!="sort_tree"} for x in rows
         if x["child_maxRows"].startswith("Some(1)") or
            x["join_maxRows"].startswith("Some(1)")
       ] for name,rows in bycase.items()}
    out={"events":len(events),"errors":errors,"cases":bycase,
         "suspicious_one_row_claims":summary}
    Path("artifacts/cardinality_analysis.json").write_text(json.dumps(out,indent=2))
    for name,rows in summary.items():
        print("QUERYPROOF_CARDINALITY_SUMMARY",name,len(rows),"one-row-claim events",
              "examples",json.dumps(rows[:2]),flush=True)
    if not events or errors:raise SystemExit(2)
if __name__=="__main__":main()
