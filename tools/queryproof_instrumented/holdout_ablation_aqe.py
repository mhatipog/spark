"""Interleaved real-Spark AQE cardinality intervention for frozen-method holdout.

Does excluding EliminateLimits change the observed frequency of LIMIT violation?
The expected cardinality comes from SQL LIMIT 1 semantics, not from QueryProof.
"""
import json,traceback
from pathlib import Path
from pyspark.sql import SparkSession

QUERY="""
WITH top AS (
 SELECT id % 1 AS k FROM range(0, 2, 1, 1) OFFSET 1
), b AS (SELECT * FROM VALUES (0), (0) AS b(k))
SELECT 1 AS c FROM top LEFT JOIN b USING (k) LIMIT 1
"""
RULE='org.apache.spark.sql.catalyst.optimizer.EliminateLimits'
CONDITIONS=(('aqe_default',True,None),
            ('aqe_exclude_candidate',True,RULE),
            ('aqe_off_control',False,None))

def main():
    spark=(SparkSession.builder.master('local[2]')
      .appName('QueryProof-AQE-intervention')
      .config('spark.ui.enabled','false')
      .config('spark.sql.adaptive.enabled','true')
      .config('spark.sql.autoBroadcastJoinThreshold','-1')
      .config('spark.sql.adaptive.autoBroadcastJoinThreshold','1048576')
      .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    out={'spark_version':spark.version,'candidate_rule':RULE,
         'design':'Interleaved 8 repeats per condition, same independent SQL cardinality oracle.',
         'observations':[]}
    try:
        for repeat in range(8):
            for name,aqe,exclude in CONDITIONS:
                s=spark.newSession()
                s.conf.set('spark.sql.adaptive.enabled',str(aqe).lower())
                if exclude is not None:
                    s.conf.set('spark.sql.optimizer.excludedRules',exclude)
                record={'repeat':repeat,'condition':name,'aqe':aqe,'excluded':exclude}
                try:
                    df=s.sql(QUERY)
                    record['optimized_plan']=df._jdf.queryExecution().optimizedPlan().toString()
                    rows=[list(r) for r in df.collect()]
                    record['rows']=rows
                    record['cardinality']=len(rows)
                    record['status']='LIMIT_VIOLATION' if len(rows)>1 else 'WITHIN_LIMIT'
                    record['executed_plan']=df._jdf.queryExecution().executedPlan().toString()
                except Exception as error:
                    record['status']='INCONCLUSIVE_ERROR'
                    record['error']=repr(error)
                    record['traceback']=traceback.format_exc()[:7000]
                out['observations'].append(record)
                print('QUERYPROOF_AQE_INTERVENTION',repeat,name,record['status'],
                      record.get('cardinality'),flush=True)
    finally:
        spark.stop()
    summary={}
    for name,_,_ in CONDITIONS:
        vals=[r for r in out['observations'] if r['condition']==name]
        summary[name]={'runs':len(vals),
                       'limit_violations':sum(x['status']=='LIMIT_VIOLATION' for x in vals),
                       'inconclusive':sum(x['status']=='INCONCLUSIVE_ERROR' for x in vals)}
    out['summary']=summary
    Path('artifacts').mkdir(exist_ok=True)
    Path('artifacts/aqe-intervention.json').write_text(json.dumps(out,indent=2))
    print('QUERYPROOF_AQE_INTERVENTION_SUMMARY',json.dumps(summary),flush=True)
if __name__=='__main__':
    main()
