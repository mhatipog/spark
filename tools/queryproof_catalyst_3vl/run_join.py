"""Independent Catalyst 3VL join witness on frozen historical cases plus safe controls."""
import json
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from join_interpreter import check,extract,canonical_bag

ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT.parent/'queryproof_prospective'/'manifest.json').read_text())
CASES={
 'B01':next(x for x in MANIFEST['cases'] if x['id']=='B01'),
 'N01':next(x for x in MANIFEST['cases'] if x['id']=='N01'),
}
SYNTHETIC={
 'S01_direct_positive_match':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON tl.a=tr.x OR (tl.a IS NULL AND tr.x IS NULL)''',
 'S02_nested_not_both_null':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON NOT (tl.a=tr.x OR (tl.a IS NULL AND tr.x IS NULL))''',
 'S03_nested_is_null':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON (tl.a=tr.x OR (tl.a IS NULL AND tr.x IS NULL)) IS NULL''',
 'S04_safe_null_safe_equality':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON tl.a <=> tr.x''',
 'S05_direct_not':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON NOT (tl.a = tr.x)''',
 'U01_left_outer_unsupported':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl LEFT JOIN tr
    ON tl.a=tr.x''',
 'U02_derived_projection':'''SELECT tl.a+1 AS a1,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON tl.a=tr.x''',
 'U03_less_than_predicate':'''SELECT tl.a,tl.b,tr.x,tr.y FROM tl JOIN tr
    ON tl.a < tr.x''',
 'U04_no_join':'''SELECT a,b FROM tl''',
}

def main():
    spark=(SparkSession.builder.master('local[2]')
           .appName('QueryProof Catalyst independent 3VL row-bag oracle')
           .config('spark.ui.enabled','false')
           .config('spark.sql.shuffle.partitions','5').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    results=[]
    try:
        with tempfile.TemporaryDirectory(prefix='qp-t3-bag-') as d:
            for aqe in (False,True):
                s=spark.newSession()
                s.conf.set('spark.sql.adaptive.enabled',str(aqe).lower())
                s.createDataFrame([(0,10),(None,11)],'a INT,b INT').write.mode('overwrite').parquet(d+'/tl-'+str(aqe))
                s.createDataFrame([(None,20),(0,21),(1,22)],'x INT,y INT').write.mode('overwrite').parquet(d+'/tr-'+str(aqe))
                s.read.parquet(d+'/tl-'+str(aqe)).createOrReplaceTempView('tl')
                s.read.parquet(d+'/tr-'+str(aqe)).createOrReplaceTempView('tr')
                for name,sql in (
                   [('B01',CASES['B01']['sql']),('N01',CASES['N01']['sql'])]
                   +list(SYNTHETIC.items())):
                    try:
                        result=check(s,s.sql(sql))
                    except Exception as exc:
                        result={'status':'ERROR','exception':type(exc).__name__,
                                'reason':str(exc)[:400]}
                    observed={'case':name,'aqe':aqe,'result':result}
                    results.append(observed)
                    print('QUERYPROOF_3VL_JOIN',spark.version,name,'AQE_ON' if aqe else 'AQE_OFF',
                          result['status'],result.get('reason',''),
                          result.get('truth_histogram',{}),flush=True)
        bycase={name:[x['result']['status'] for x in results if x['case']==name]
                for name in ['B01','N01',*SYNTHETIC]}
        errors=[x for x in results if x['result']['status']=='ERROR']
        false_accusations=[x for x in results if x['case'] in ('N01','S01_direct_positive_match',
                            'S04_safe_null_safe_equality','S05_direct_not')
                            and x['result']['status']=='ROW_BAG_VIOLATION']
        unintended=[x for x in results if x['case'].startswith('U') and x['result']['status']!='UNKNOWN']
        summary={'version':spark.version,'tested':len(results),'by_case':bycase,
                 'false_accusations_on_safe_controls':len(false_accusations),
                 'unsupported_not_unknown':len(unintended),
                 'in_sample_B01_detected':sum(y=='ROW_BAG_VIOLATION' for y in bycase['B01']),
                 'errors':errors,
                 'scope':'Only direct small source INNER JOIN, supported 3VL operator trees; in-sample known B01, not general verifier'}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts')/('queryproof_3vl_join_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'cases':results},indent=2),encoding='utf8')
        print('QUERYPROOF_3VL_JOIN_SUMMARY',json.dumps(summary),flush=True)
        return 1 if errors or unintended or false_accusations or summary['in_sample_B01_detected']==0 else 0
    finally:
        spark.stop()

if __name__=='__main__':
    raise SystemExit(main())
