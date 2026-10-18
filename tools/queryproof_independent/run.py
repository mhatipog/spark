"""Retrospective independent-family benchmark: run first, score only afterward.

No manual SQL answers, oracle queries or expected counts enter QueryProof.
Also run a cheap DQP-inspired Spark-configuration differential (NOT SQLancer).
"""
import hashlib
import json
import sys
import tempfile
import traceback
from collections import Counter
from pathlib import Path

from pyspark.sql import SparkSession

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'queryproof_prospective'))
sys.path.insert(0,str(ROOT.parent/'queryproof_auto_obligations'))
sys.path.insert(0,str(ROOT.parent/'queryproof_observed_contract'))
from run import fixture,enc
from plan_extractor import extract
from contract import audit

MANIFEST_PATH=ROOT.parent/'queryproof_prospective'/'manifest.json'
MANIFEST=json.loads(MANIFEST_PATH.read_text(encoding='utf8'))
PROTOCOL=json.loads((ROOT/'FROZEN_PROTOCOL.json').read_text(encoding='utf8'))
ALTERNATES={
 'aqe_flip': {'spark.sql.adaptive.enabled':None},
 'broadcast_disabled': {
    'spark.sql.autoBroadcastJoinThreshold':'-1',
    'spark.sql.adaptive.autoBroadcastJoinThreshold':'-1',
 },
 'join_optimizer_exclusion':{
    'spark.sql.optimizer.excludedRules':'org.apache.spark.sql.catalyst.optimizer.OptimizeJoinCondition',
 }
}


def canonical(rows, comparison):
    serial=[json.dumps(row,sort_keys=True,default=str) for row in rows]
    return serial if comparison=='sequence' else sorted(serial)


def inspect_execution(s, case):
    df=s.sql(case['sql'])
    q=df._jdf.queryExecution()
    obligations=extract(q)
    # Critical: no manifest['expected'] or SQL reference is read by this function.
    rows=[enc(list(x)[:case.get('observed_columns')]
         if case.get('observed_columns') else list(x)) for x in df.collect()]
    order=(audit(rows,obligations['keys']) if obligations['verdict']=='EXPOSED_ORDER_CONTRACT'
           and not case.get('observed_columns')
           else {'verdict':'UNKNOWN','reason':obligations.get('reason','TRUNCATED_RESULT_COLUMNS')})
    return {'rows':rows,'obligation':obligations,'order_verdict':order,
            'plan':str(q.executedPlan().toString())[:15000],
            'optimized_plan':str(q.optimizedPlan().toString())[:9000]}


def configure(s, case, alt=None):
    original=dict(case.get('settings',{}))
    cfg=original.copy()
    if alt=='aqe_flip':
        orig=str(cfg.get('spark.sql.adaptive.enabled','false')).lower()
        cfg['spark.sql.adaptive.enabled']='false' if orig=='true' else 'true'
    elif alt:
        cfg.update(ALTERNATES[alt])
    for name,val in cfg.items():
        s.conf.set(name,str(val).lower() if isinstance(val,bool) else str(val))
    return cfg


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof independent-family holdout')
           .config('spark.sql.adaptive.enabled','false')
           .config('spark.sql.shuffle.partitions','5')
           .config('spark.ui.enabled','false')
           .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    trials={}
    try:
        with tempfile.TemporaryDirectory(prefix='queryproof-independent-') as directory:
            for case in MANIFEST['cases']:
                cid=case['id']
                trial={'issue':case['issue'],'kind':case['kind'],
                       'comparison':case['comparison'],'category':case['category'],'runs':{}}
                trials[cid]=trial
                for arm in ('baseline',*ALTERNATES):
                    session=spark.newSession()
                    settings=configure(session,case,None if arm=='baseline' else arm)
                    try:
                        fixture(session,case['fixture'],directory+'/'+cid+'-'+arm)
                        data=inspect_execution(session,case)
                        trial['runs'][arm]={'status':'OK','settings':settings,**data}
                    except Exception as exc:
                        trial['runs'][arm]={'status':'ERROR','settings':settings,
                            'error':type(exc).__name__+': '+str(exc)[:600],
                            'traceback':traceback.format_exc()[-1300:]}
                    o=trial['runs'][arm]
                    print('QUERYPROOF_INDEPENDENT',spark.version,cid,arm,o['status'],
                          o.get('obligation',{}).get('verdict','NA'),
                          o.get('order_verdict',{}).get('verdict','NA'),
                          flush=True)
                baseline=trial['runs']['baseline']
                comparison=case['comparison']
                trial['differential']={}
                for arm in ALTERNATES:
                    alt=trial['runs'][arm]
                    if baseline['status']=='OK' and alt['status']=='OK':
                        divergence=canonical(baseline['rows'],comparison)!=canonical(alt['rows'],comparison)
                        plans_differ=baseline['plan']!=alt['plan']
                        trial['differential'][arm]={'result_divergence':divergence,
                            'physical_plans_differ':plans_differ,
                            'configuration_only_baseline':'Not SQLancer DQP'}
                    else:
                        trial['differential'][arm]={'status':'UNKNOWN'}
    finally:
        spark.stop()
    path=Path('artifacts')
    path.mkdir(exist_ok=True)
    output={'version':spark.version,'protocol_sha256':hashlib.sha256(
        (ROOT/'FROZEN_PROTOCOL.json').read_bytes()).hexdigest(),
        'manifest_sha256':hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest(),
        'raw_results':trials,
        'notice':'Expected answers are intentionally absent: scored in separate score.py'}
    (path/('queryproof_independent_'+spark.version+'.json')).write_text(
        json.dumps(output,indent=2,sort_keys=True),encoding='utf8')
    print('QUERYPROOF_INDEPENDENT_RUN_COMPLETE',spark.version,
          {k:[v['status'] for v in x['runs'].values()] for k,x in trials.items()},flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
