"""Frozen stage scope benchmark: no manually specified source/key/identity/limit."""
import json
from pathlib import Path
from pyspark.sql import SparkSession
from auto_topk import check

HERE=Path(__file__).resolve().parent
PROTOCOL=json.loads((HERE/'FROZEN_STAGE_PROTOCOL.json').read_text())
BASE_ROWS=[(1,'a',30),(2,'b',20),(3,'c',40),(4,'d',60),(5,'e',10),(6,'f',50)]
REL=[(1,2),(1,3),(1,5),(2,3)]
PROJECTIONS={
'plain':'',
'deterministic_project':', length(name) AS n',
'uncorrelated_scalar':', (SELECT max(b) FROM k) AS c',
'uncorrelated_exists':', EXISTS(SELECT 1 FROM k) AS e',
'correlated_scalar_count':', (SELECT count(*) FROM k WHERE k.a=s.id) AS c',
'correlated_exists':', EXISTS(SELECT 1 FROM k WHERE k.a=s.id) AS e',
}
SLICES={'LIMIT 3':'LIMIT 3','OFFSET 3':'OFFSET 3'}


def query(slice_name,expression,source='p',sort='age DESC'):
    return ('WITH s AS (SELECT id,name FROM '+source+' ORDER BY '+sort+' '+slice_name+') '
            'SELECT name'+expression+' FROM s')


def new_session(spark, adaptive):
    ss=spark.newSession()
    ss.conf.set('spark.sql.adaptive.enabled',str(adaptive).lower())
    ss.conf.set('spark.sql.shuffle.partitions','5')
    ss.conf.set('spark.sql.autoBroadcastJoinThreshold','-1')
    ss.createDataFrame(BASE_ROWS,'id INT, name STRING, age INT').repartition(5).createOrReplaceTempView('p')
    ss.createDataFrame(REL,'a INT, b INT').createOrReplaceTempView('k')
    return ss


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof auto CTE stage TopK')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    spark.sparkContext.addPyFile(str((HERE.parent/'queryproof_observed_contract'/'topk_certificate.py').resolve()))
    experiments=[]; negative=[]
    try:
        for aqe in PROTOCOL['experiment']['adaptive']:
            ss=new_session(spark,aqe)
            for slice_name in SLICES:
                for family in PROJECTIONS:
                    for repeat in range(PROTOCOL['experiment']['repetitions']):
                        statement=query(slice_name,PROJECTIONS[family])
                        df=ss.sql(statement)
                        observed=check(ss,df)
                        experiments.append({'aqe':aqe,'slice':slice_name,
                                            'family':family,'repeat':repeat,
                                            'observation':observed})
                        print('QUERYPROOF_AUTO_TOPK',spark.version,aqe,slice_name,
                              family,repeat,observed['status'],
                              observed.get('reason',''),flush=True)
            if aqe:  # negative controls only once per version
                ss.createDataFrame([(1,'a',30),(2,'b',30),(3,'c',10)],
                                   'id INT,name STRING,age INT').createOrReplaceTempView('p_duplicate')
                ss.createDataFrame([(i,'v'+str(i),10+i) for i in range(70)],
                                   'id INT,name STRING,age INT').createOrReplaceTempView('p_large')
                negatives={
                    'filter_in_source':"WITH s AS (SELECT id,name FROM p WHERE age>20 ORDER BY age DESC LIMIT 3) SELECT name FROM s",
                    'derived_identity':"WITH s AS (SELECT id,name FROM p ORDER BY age DESC LIMIT 3) SELECT upper(name) AS name FROM s",
                    'sort_expression':query('LIMIT 3','',sort='age+1 DESC'),
                    'nonunique_order_key':query('LIMIT 3','',source='p_duplicate'),
                    'too_many_rows':query('LIMIT 3','',source='p_large'),
                    'multiple_ctes':"WITH s AS (SELECT id,name FROM p ORDER BY age DESC LIMIT 3),t AS (SELECT id FROM k) SELECT name FROM s",
                }
                for name,sql in negatives.items():
                    try:
                        observation=check(ss,ss.sql(sql))
                    except Exception as exc:
                        observation={'status':'ERROR','error':repr(exc)}
                    negative.append({'name':name,'observation':observation})
                    print('QUERYPROOF_AUTO_TOPK_NEGATIVE',spark.version,name,
                          observation['status'],observation.get('reason',''),flush=True)
        statuses=[x['observation']['status'] for x in experiments]
        status_by_family={
            f:{'runs':sum(x['family']==f for x in experiments),
               'violations':sum(x['family']==f and x['observation']['status']=='MEMBERSHIP_VIOLATION'
                                for x in experiments),
               'unknown':sum(x['family']==f and x['observation']['status']=='UNKNOWN'
                             for x in experiments)}
            for f in PROJECTIONS}
        invalid_negatives=[x for x in negative if x['observation']['status']!='UNKNOWN']
        unexpected_positive_unknown=sum(v=='UNKNOWN' for v in statuses)
        correlated_violations=sum(x['observation']['status']=='MEMBERSHIP_VIOLATION'
            for x in experiments if x['family'].startswith('correlated'))
        safe_violations=sum(x['observation']['status']=='MEMBERSHIP_VIOLATION'
            for x in experiments if not x['family'].startswith('correlated'))
        summary={'spark_version':spark.version,'cases':len(experiments),
                 'negative_controls':len(negative),'by_family':status_by_family,
                 'correlated_violations':correlated_violations,
                 'safe_violations':safe_violations,
                 'positive_unknown':unexpected_positive_unknown,
                 'negative_controls_unexpected':invalid_negatives,
                 'source_manually_specified_to_verifier':False,
                 'interpretation':'Only constrained in-memory, unique-key, single-CTE cases; known PR #59445 family.'}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts')/('queryproof_auto_topk_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'positive':experiments,'negative':negative},indent=2))
        print('QUERYPROOF_AUTO_TOPK_SUMMARY',json.dumps(summary),flush=True)
        return 1 if invalid_negatives or unexpected_positive_unknown or safe_violations or not correlated_violations else 0
    finally:
        spark.stop()


if __name__=='__main__':raise SystemExit(main())
