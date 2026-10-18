"""Independent finite-source IN-subquery witness against prior frozen cases."""
import json
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from in_interpreter import check

ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT.parent/'queryproof_prospective'/'manifest.json').read_text())
B02=next(x for x in MANIFEST['cases'] if x['id']=='B02')
N02=next(x for x in MANIFEST['cases'] if x['id']=='N02')

CONTROLS={
 'S11_in_is_not_null': 'SELECT c FROM tn WHERE (c IN (SELECT c FROM t)) IS NOT NULL',
 'S12_not_in': 'SELECT c FROM tn WHERE NOT (c IN (SELECT c FROM t))',
 'S13_or_in': 'SELECT c FROM tn WHERE c IN (SELECT c FROM t) OR c IS NULL',
 'U11_derived_input': 'SELECT c+1 AS v FROM tn WHERE c IN (SELECT c FROM t)',
 'U12_filtered_subquery': 'SELECT c FROM tn WHERE c IN (SELECT c FROM t WHERE c=1)',
 'U13_no_in_subquery': 'SELECT c FROM tn WHERE c IS NULL',
 'U14_in_projection_not_filter': 'SELECT c IN (SELECT c FROM t) AS v FROM tn',
}


def main():
    spark=(SparkSession.builder.master('local[2]')
           .appName('QueryProof analyzed nullable IN witness')
           .config('spark.ui.enabled','false')
           .config('spark.sql.shuffle.partitions','5').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    results=[]
    try:
        with tempfile.TemporaryDirectory(prefix='qp-in-t3-') as d:
            for aqe in (False,True):
                s=spark.newSession()
                s.conf.set('spark.sql.adaptive.enabled',str(aqe).lower())
                s.createDataFrame([(1,),(2,)],'c INT').write.mode('overwrite').parquet(d+'/t-'+str(aqe))
                s.createDataFrame([(1,),(2,),(None,)],'c INT').write.mode('overwrite').parquet(d+'/tn-'+str(aqe))
                s.read.parquet(d+'/t-'+str(aqe)).createOrReplaceTempView('t')
                s.read.parquet(d+'/tn-'+str(aqe)).createOrReplaceTempView('tn')
                for name,sql in [('B02',B02['sql']),('N02',N02['sql']),*CONTROLS.items()]:
                    try:
                        observation=check(s,s.sql(sql))
                    except Exception as exc:
                        observation={'status':'ERROR','error':type(exc).__name__+': '+str(exc)[:400]}
                    results.append({'name':name,'aqe':aqe,'result':observation})
                    print('QUERYPROOF_3VL_IN',spark.version,name,aqe,
                          observation['status'],observation.get('reason',''),
                          observation.get('truth_histogram',{}),flush=True)
        by_name={n:[x['result']['status'] for x in results if x['name']==n]
                 for n in ['B02','N02',*CONTROLS]}
        summary={
          'version':spark.version,'cases':len(results),
          'B02_wrong_results_witnessed':sum(v=='ROW_BAG_VIOLATION' for v in by_name['B02']),
          'N02_false_accusations':sum(v=='ROW_BAG_VIOLATION' for v in by_name['N02']),
          'unsupported_outside_scope':[{'name':n,'statuses':values} for n,values in by_name.items()
                                       if n.startswith('U') and any(x!='UNKNOWN' for x in values)],
          'error_cases':[x['name'] for x in results if x['result']['status']=='ERROR'],
          'per_case':by_name,
          'novelty':'Known nullable IN wrong-result family B02; this is an independent in-sample source evaluator.'}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts')/('queryproof_3vl_in_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'observations':results},indent=2),encoding='utf8')
        print('QUERYPROOF_3VL_IN_SUMMARY',json.dumps(summary),flush=True)
        return 1 if summary['error_cases'] or summary['unsupported_outside_scope'] or \
            summary['N02_false_accusations'] or summary['B02_wrong_results_witnessed']==0 else 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
