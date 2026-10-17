"""Independent stage-level top-k membership check for Spark PR #59445 bug family.

This is in-sample calibration, NOT discovery of a new bug or a proof for
arbitrary optimized queries. The source input is independently read once for
a bounded top-k certificate before evaluating correlated projections.
"""
import json
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from topk_certificate import local_topq, merge_topq, witness

ROWS=[(1,'a',30),(2,'b',20),(3,'c',40),(4,'d',60),(5,'e',10),(6,'f',50)]
REL=[(1,2),(1,3),(1,5),(2,3)]


def certificate(p,q):
    # Disjoint Spark input partitions, each supplying at most q candidates;
    # no execution of the correlated subquery under evaluation.
    candidates=p.select('id','name','age').rdd.mapPartitions(
        lambda it: iter(local_topq(((int(r.id),str(r.name),int(r.age)) for r in it),q))).collect()
    return merge_topq([candidates],q)


def run(spark,adaptive):
    ss=spark.newSession()
    ss.conf.set('spark.sql.adaptive.enabled',str(adaptive).lower())
    ss.conf.set('spark.sql.shuffle.partitions','5')
    ss.conf.set('spark.sql.autoBroadcastJoinThreshold','-1')
    p=ss.createDataFrame(ROWS,'id INT, name STRING, age INT').repartition(5)
    p.createOrReplaceTempView('p')
    ss.createDataFrame(REL,'a INT, b INT').createOrReplaceTempView('k')
    checks=[]
    for slice_sql,offset,q,k in [('LIMIT 3',0,3,3),('OFFSET 3',3,6,3)]:
        expected=[x[3] for x in certificate(p,q)[offset:offset+k]]
        for kind,projection,repeats in [
            ('plain', None, 2),
            ('deterministic_project', 'length(name) AS n', 2),
            ('uncorrelated_scalar', '(SELECT max(b) FROM k) AS c', 2),
            ('uncorrelated_exists', 'EXISTS(SELECT 1 FROM k) AS e', 2),
            ('scalar_count','(SELECT count(*) FROM k WHERE k.a = s.id) AS c', 4),
            ('exists','EXISTS(SELECT 1 FROM k WHERE k.a = s.id) AS e', 4)]:
            query=('WITH s AS (SELECT id,name FROM p ORDER BY age DESC '+slice_sql+') '
                   'SELECT name'+(', '+projection if projection else '')+' FROM s')
            for repeat in range(repeats):
                try:
                    rows=[list(row) for row in ss.sql(query).collect()]
                    got=[str(row[0]) for row in rows]
                    finding=witness(expected,got)
                    result={'adaptive':adaptive,'slice':slice_sql,'kind':kind,'repeat':repeat,
                            'verdict':finding['verdict'],'expected':expected,'actual':got}
                except Exception as exc:
                    result={'adaptive':adaptive,'slice':slice_sql,'kind':kind,'repeat':repeat,
                            'verdict':'ERROR','error':repr(exc)}
                checks.append(result)
                print('QUERYPROOF_TOPK',spark.version,adaptive,slice_sql,kind,repeat,result['verdict'],flush=True)
    return checks


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof top-k stage certificate')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    spark.sparkContext.addPyFile(str(Path(__file__).resolve().with_name('topk_certificate.py')))
    try:
        checks=run(spark,False)+run(spark,True)
        per_kind = {kind: {'runs':sum(x['kind']==kind for x in checks),
                           'violations':sum(x['kind']==kind and x['verdict']=='MEMBERSHIP_VIOLATION' for x in checks)}
                    for kind in sorted(set(x['kind'] for x in checks))}
        summary={'spark_version':spark.version,'runs':len(checks), 'per_kind':per_kind,
                 'membership_violations':sum(x['verdict']=='MEMBERSHIP_VIOLATION' for x in checks),
                 'unknown':sum(x['verdict']=='UNKNOWN' for x in checks),
                 'errors':[x for x in checks if x['verdict']=='ERROR'],
                 'interpretation':'In-sample Spark #59445 regression family, source-stage certificate only; not general verifier'}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts')/('queryproof_topk_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'observations':checks},indent=2),encoding='utf8')
        print('QUERYPROOF_TOPK_SUMMARY',json.dumps(summary),flush=True)
        return 1 if summary['errors'] else 0
    finally:
        spark.stop()


if __name__=='__main__': sys.exit(main())
