"""Known global aggregate correctness calibration, plus independent controls.

All expected outputs for supported cases are recovered from Catalyst Range
and scalar IN trees; no manifest expected answer is passed to the interpreter.
"""
import json
from pathlib import Path
from pyspark.sql import SparkSession
from interpreter import check

HERE=Path(__file__).resolve().parent
MANIFEST=json.loads((HERE.parent/'queryproof_prospective'/'manifest.json').read_text())
CASES={
 'B03':next(c['sql'] for c in MANIFEST['cases'] if c['id']=='B03'),
 'N03':next(c['sql'] for c in MANIFEST['cases'] if c['id']=='N03'),
 'S01_empty_nonmember':"SELECT count(*) AS n, 2 IN (SELECT id FROM range(1,2)) AS pred FROM range(0)",
 'S02_empty_inner':"SELECT count(*) AS n, 1 IN (SELECT id FROM range(0)) AS pred FROM range(0)",
 'S03_two_rows_member':"SELECT count(*) AS n, 1 IN (SELECT id FROM range(1,2)) AS pred FROM range(2)",
 'S04_two_rows_nonmember':"SELECT count(*) AS n, 2 IN (SELECT id FROM range(1,2)) AS pred FROM range(2)",
 'U01_grouped':"SELECT id,count(*) AS n, 1 IN (SELECT id FROM range(1,2)) AS pred FROM range(2) GROUP BY id",
 'U02_filter_input':"SELECT count(*) AS n, 1 IN (SELECT id FROM range(1,2)) AS pred FROM range(2) WHERE id>0",
 'U03_derived_inner':"SELECT count(*) AS n, 1 IN (SELECT id+1 FROM range(2)) AS pred FROM range(0)",
 'U04_nonrange_input':"SELECT count(*) AS n, 1 IN (SELECT id FROM range(1,2)) AS pred FROM VALUES (1),(2) AS t(x)",
 'U05_no_subquery':"SELECT count(*) AS n, true AS pred FROM range(0)",
 'U06_cast_null_probe':"SELECT count(*) AS n, CAST(NULL AS BIGINT) IN (SELECT id FROM range(1,2)) AS pred FROM range(0)",
}


def main():
    spark=(SparkSession.builder.master('local[2]')
           .appName('QueryProof small global aggregate source-cardinality certificate')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    observations=[]
    try:
        for adaptive in (False,True):
            ss=spark.newSession()
            ss.conf.set('spark.sql.adaptive.enabled',str(adaptive).lower())
            for name,sql in CASES.items():
                try:
                    value=check(ss.sql(sql))
                except Exception as exc:
                    value={'status':'ERROR','reason':type(exc).__name__+':'+str(exc)[:400]}
                observations.append({'name':name,'aqe':adaptive,'observation':value})
                print('QUERYPROOF_CARDINALITY',spark.version,name,adaptive,
                      value['status'],value.get('cardinality_observer',''),
                      value.get('actual',[]),value.get('reason','')[:100],flush=True)
    finally:
        spark.stop()
    bycase={name:[x['observation'] for x in observations if x['name']==name]
            for name in CASES}
    wrong_b03=sum(x['status']=='VALUE_VIOLATION' for x in bycase['B03'])
    correct_n03=sum(x['status']=='NO_VIOLATION_OBSERVED' for x in bycase['N03'])
    errors=[name for name,v in bycase.items() if any(x['status']=='ERROR' for x in v)]
    unsupported=[name for name,v in bycase.items()
                 if name.startswith('U') and any(x['status']!='UNKNOWN' for x in v)]
    controls=[name for name,v in bycase.items()
              if name.startswith(('N','S')) and any(x['status']=='VALUE_VIOLATION' for x in v)]
    summary={'release':spark.version,'observations':len(observations),
             'known_empty_aggregate_B03_wrong_detected':wrong_b03,
             'nonempty_aggregate_N03_correct':correct_n03,
             'negative_controls_unexpectedly_accepted':unsupported,
             'other_possible_value_violations':controls,
             'errors':errors,
             'no_manually_specified_expected_output':True,
             'interpretation':'In-sample narrow Count and uncorrelated scalar IN range interpreter, not general Spark proof'}
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/('queryproof_cardinality_'+spark.version+'.json')).write_text(
        json.dumps({'summary':summary,'observations':observations},indent=2))
    print('QUERYPROOF_CARDINALITY_SUMMARY',json.dumps(summary),flush=True)
    return 1 if errors or unsupported or wrong_b03<2 or correct_n03<2 else 0


if __name__=='__main__':raise SystemExit(main())
