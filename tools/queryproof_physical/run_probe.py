"""Run unmodified official Spark wheel with independently compiled ordering metadata probe."""
import json,sys,traceback
from pathlib import Path
from pyspark.sql import SparkSession
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"queryproof_prospective"))
from official_wheel_repro import SQL,EXPECTED,SETTINGS

def inspect(probe,q):
    return json.loads(probe.inspect(q._jdf.queryExecution()))

def run():
    spark=(SparkSession.builder.master("local[2]").appName("QueryProof-Physical-Ordering-Properties")
        .config("spark.ui.enabled","false")
        .config("spark.sql.shuffle.partitions","5")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    jvm=spark._jvm
    probe=jvm.org.queryproof.PhysicalOrderingProbe
    out={"version":1,"spark_version":spark.version,
         "claim":"Spark metadata observations, NOT a correctness proof or independent oracle",
         "settings":{},"controls":{}}
    try:
        for name,config in SETTINGS:
            trials=[]
            for iteration in range(5):
                ss=spark.newSession()
                for k,v in config.items():ss.conf.set(k,v)
                try:
                    df=ss.sql(SQL)
                    before=inspect(probe,df)
                    actual=[list(r) for r in df.collect()]
                    after=inspect(probe,df)
                    trials.append({"iteration":iteration,"rows":actual,"matches_sql_oracle":actual==EXPECTED,
                                   "before":before,"after":after})
                except Exception as e:
                    trials.append({"iteration":iteration,"error":repr(e),"trace":traceback.format_exc()[-1500:]})
            out["settings"][name]={"config":config,"trials":trials}
            verdicts=[t.get("after",{}).get("verdict","ERROR") for t in trials]
            print("QUERYPROOF_ORDERING",spark.version,name,
                  "verdicts",verdicts,
                  "wrong_answers",sum(t.get("matches_sql_oracle") is False for t in trials),flush=True)
        controls=[
          ("order_by_global","SELECT id FROM range(0,50,1,4) ORDER BY id DESC",True),
          ("order_by_global_two_keys","SELECT id, id % 3 AS k FROM range(0,50,1,4) ORDER BY k ASC,id DESC",True),
          ("sort_by_partition","SELECT id FROM range(0,50,1,4) SORT BY id DESC",False),
          ("no_sort","SELECT id FROM range(0,50,1,4)",False),
          ("top_k","SELECT id FROM range(0,50,1,4) ORDER BY id DESC LIMIT 3",None),
          ("order_by_explicit_null","SELECT id FROM VALUES (1),(NULL),(2) AS t(id) ORDER BY id ASC NULLS LAST",True)
        ]
        for name,sql,expect_global in controls:
            ss=spark.newSession()
            try:
                df=ss.sql(sql)
                rows=[list(x) for x in df.collect()]
                out["controls"][name]={"rows":rows,"expect_root_sort":expect_global,
                                       "inspection":inspect(probe,df)}
            except Exception as e:
                out["controls"][name]={"error":repr(e)}
            print("QUERYPROOF_ORDERING_CONTROL",spark.version,name,
                  out["controls"][name].get("inspection",{}).get("verdict","ERROR"),flush=True)
    finally:
        spark.stop()
    Path("artifacts").mkdir(exist_ok=True)
    path=Path(f"artifacts/queryproof_physical_ordering_{out['spark_version']}.json")
    path.write_text(json.dumps(out,indent=2))
    if all("error" in t for section in out["settings"].values() for t in section["trials"]):
        return 2
    return 0

if __name__=="__main__":
    raise SystemExit(run())
