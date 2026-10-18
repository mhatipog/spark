"""Run frozen prior SQL issues once with experimental RuleExecutor AST trace.

No original source files or published expectations used in this runner; it
saves observed output and Catalyst observer demands for postexecution scoring.
The instrumented JAR is a disposable GitHub Actions environment.
"""
import hashlib,json,sys,tempfile,time
from pathlib import Path
from pyspark.sql import SparkSession

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'queryproof_prospective'))
sys.path.insert(0,str(HERE.parent/'queryproof_observer_lattice'))
from run import fixture,enc
from catalyst_observer import observe_analyzed

MANIFEST_PATH=HERE.parent/'queryproof_prospective'/'manifest.json'
MANIFEST=json.loads(MANIFEST_PATH.read_text())
IDS={'B01','B02','B03','N01','N02','N03'}

def run():
    spark=(SparkSession.builder.master('local[2]')
           .appName('QueryProof first rule semantic provenance')
           .config('spark.ui.enabled','false')
           .config('spark.sql.adaptive.enabled','false')
           .config('spark.sql.shuffle.partitions','5').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    jvm=spark._jvm.java.lang.System
    results={}
    try:
        with tempfile.TemporaryDirectory(prefix='qp-rule-') as td:
            for c in MANIFEST['cases']:
                if c['id'] not in IDS:continue
                session=spark.newSession()
                for k,v in c.get('settings',{}).items():
                    session.conf.set(k,str(v).lower() if isinstance(v,bool) else str(v))
                fixture(session,c['fixture'],td+'/'+c['id'])
                jvm.setProperty('queryproof.ast.enabled','true')
                jvm.setProperty('queryproof.query.id',c['id'])
                started=time.perf_counter()
                try:
                    df=session.sql(c['sql'])
                    observer=observe_analyzed(df)
                    # Force rule executor after labeling the synthetic query.
                    optimized=df._jdf.queryExecution().optimizedPlan().toString()
                    rows=[enc(list(x)) for x in df.collect()]
                    results[c['id']]={'status':'OBSERVED','rows':rows,
                        'observer_requirements':observer,
                        'optimized_plan':optimized[:16000],
                        'elapsed_ms':round(1000*(time.perf_counter()-started),1)}
                    print('QUERYPROOF_RULE_RUN',c['id'],'OBSERVED',len(rows),flush=True)
                except Exception as exc:
                    results[c['id']]={'status':'ERROR','reason':type(exc).__name__+':'+str(exc)[:500]}
                    print('QUERYPROOF_RULE_RUN',c['id'],'ERROR',repr(exc),flush=True)
                finally:
                    jvm.clearProperty('queryproof.ast.enabled')
                    jvm.clearProperty('queryproof.query.id')
    finally:
        spark.stop()
    Path('artifacts').mkdir(exist_ok=True)
    output={'release':spark.version,'manifest_sha256':hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest(),
            'observations':results,
            'warning':'RuleExecutor trace lists structural changes; this file does not assert rule unsoundness'}
    (Path('artifacts')/'queryproof_rule_trace_observations.json').write_text(json.dumps(output,indent=2))
    return 1 if len(results)!=len(IDS) or any(x['status']=='ERROR' for x in results.values()) else 0

if __name__=='__main__':raise SystemExit(run())
