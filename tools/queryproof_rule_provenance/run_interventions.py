"""Causal rule-skip observations on six preregistered synthetic cases.

All rules were frozen from preceding structural trace, not selected by an
expected-answer scorer. The scorer is a separate script; nothing herein
compares output against expected SQL results.
"""
import hashlib,json,re,sys,tempfile,time
from pathlib import Path
from pyspark.sql import SparkSession
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent))
from queryproof_prospective.run import MANIFEST,fixture,enc

PROTOCOL=json.loads((HERE/'FROZEN_INTERVENTION_PROTOCOL.json').read_text())


def normalized(p):
    return re.sub(r'#\d+L?', '#EXPR',p)


def run_arm(spark,c,arm,rule,path):
    ss=spark.newSession()
    ss.conf.set('spark.sql.adaptive.enabled','false')
    ss.conf.set('spark.sql.shuffle.partitions','5')
    for k,v in c.get('settings',{}).items():
        ss.conf.set(k,str(v).lower() if isinstance(v,bool) else str(v))
    if arm=='sqlconf':
        ss.conf.set('spark.sql.optimizer.excludedRules',rule)
    fixture(ss,c['fixture'],path)
    jvm=spark._jvm.java.lang.System
    t=time.perf_counter()
    try:
        if arm=='forced':
            jvm.setProperty('queryproof.force.skipRule',rule)
            jvm.setProperty('queryproof.force.skipCount','0')
            jvm.setProperty('queryproof.force.enabled','true')
        df=ss.sql(c['sql'])
        plan=df._jdf.queryExecution().optimizedPlan().toString()
        rows=[enc(list(row)[:c.get('observed_columns')]
                  if c.get('observed_columns') else list(row)) for row in df.collect()]
        out={'status':'OBSERVED','rows':rows,
             'normalized_plan':normalized(plan)[:20000],
             'elapsed_ms':round(1000*(time.perf_counter()-t),1)}
    except Exception as exc:
        out={'status':'ERROR','error':type(exc).__name__+': '+str(exc)[:700]}
    finally:
        if arm=='forced':
            out['skip_invocations']=int(jvm.getProperty('queryproof.force.skipCount','0'))
            for prop in ('queryproof.force.enabled','queryproof.force.skipRule','queryproof.force.skipCount'):
                jvm.clearProperty(prop)
    return out


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof causal rule interventions')
           .config('spark.ui.enabled','false')
           .config('spark.sql.adaptive.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    results={}
    try:
        with tempfile.TemporaryDirectory(prefix='qp-rule-ablate-') as td:
            for case in MANIFEST['cases']:
                cid=case['id']
                if cid not in PROTOCOL['cases']:continue
                rule=PROTOCOL['candidates'][cid]
                arms={}
                for arm in ('baseline','sqlconf','forced'):
                    result=run_arm(spark,case,arm,rule,td+'/'+cid+'/'+arm)
                    arms[arm]=result
                    print('QUERYPROOF_CAUSAL_ARM',spark.version,cid,arm,
                          result['status'],result.get('skip_invocations',''),
                          len(result.get('rows',[])),
                          result.get('error','')[:100],flush=True)
                baseline=arms['baseline']
                for arm in ('sqlconf','forced'):
                    obj=arms[arm]
                    obj['plan_changed_vs_baseline']=(baseline['status']=='OBSERVED' and
                        obj['status']=='OBSERVED' and
                        obj['normalized_plan']!=baseline['normalized_plan'])
                    obj['rows_changed_vs_baseline']=(baseline['status']=='OBSERVED' and
                        obj['status']=='OBSERVED' and obj['rows']!=baseline['rows'])
                results[cid]={'candidate_rule':rule,'arms':arms}
    finally:
        spark.stop()
    Path('artifacts').mkdir(exist_ok=True)
    dest=Path('artifacts/queryproof_causal_interventions.json')
    dest.write_text(json.dumps({'version':spark.version,'protocol_sha256':hashlib.sha256(
        (HERE/'FROZEN_INTERVENTION_PROTOCOL.json').read_bytes()).hexdigest(),
        'observations':results,'warning':'No source SQL expected answers loaded during interventions.'},indent=2))
    return 1 if len(results)!=len(PROTOCOL['cases']) or any(
        v['arms']['baseline']['status']!='OBSERVED' for v in results.values()) else 0


if __name__=='__main__':raise SystemExit(main())
