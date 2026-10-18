"""Score AST-extracted observer requirements against frozen retrospective cases.

The plan observer never sees expected labels, issue status, or SQL text beyond
the analyzed plan supplied by Spark. The harness scores AFTER extraction.
"""
import json
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from catalyst_observer import observe_analyzed

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'queryproof_prospective'))
from run import fixture

MANIFEST=json.loads((ROOT.parent/'queryproof_prospective'/'manifest.json').read_text())
FROZEN=json.loads((ROOT/'FROZEN_CATALYST_OBSERVER.json').read_text())
CASE_BY_ID={c['id']:c for c in MANIFEST['cases']}


def main():
    spark=(SparkSession.builder.master('local[2]')
           .appName('QueryProof Catalyst five-partition observer extraction')
           .config('spark.ui.enabled','false')
           .config('spark.sql.adaptive.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    outcomes=[]
    try:
        with tempfile.TemporaryDirectory(prefix='qp-observer-') as d:
            for expected in FROZEN['cases']:
                case=CASE_BY_ID[expected['id']]
                ss=spark.newSession()
                for k,v in case.get('settings',{}).items():
                    ss.conf.set(k,str(v).lower() if isinstance(v,bool) else str(v))
                try:
                    fixture(ss,case['fixture'],d+'/'+case['id'])
                    observed=observe_analyzed(ss.sql(case['sql']))
                    req=observed.get('requirements',[])
                    matches=[x for x in req
                             if x.get('target')==expected['target']
                             and x.get('partition')==expected['required']]
                    all_target=[x for x in req if x.get('target')==expected['target']]
                    result={'case_id':case['id'],'issue':case['issue'],
                            'expected_label_not_passed_to_extractor':expected['required'],
                            'expected_target':expected['target'],
                            'target_matched':bool(matches),
                            'matching_nodes':matches,'target_nodes':all_target,
                            'observer':observed}
                except Exception as exc:
                    result={'case_id':case['id'],'status':'ERROR',
                            'error':type(exc).__name__+': '+str(exc)[:500]}
                outcomes.append(result)
                print('QUERYPROOF_CATALYST_OBSERVER',spark.version,case['id'],
                      result.get('target_matched',False),
                      [(x.get('target'),x.get('partition')) for x in result.get('target_nodes',[])],
                      flush=True)
    finally:
        spark.stop()
    summary={
      'spark_version':spark.version,
      'cases':len(outcomes),
      'matches':sum(x.get('target_matched',False) for x in outcomes),
      'mismatches':[x['case_id'] for x in outcomes if not x.get('target_matched',False)],
      'errors':[x for x in outcomes if x.get('status')=='ERROR'],
      'interpretation':'AST consumer demand extracted automatically; no optimizer rewrite or wrong-result proof asserted; in-sample historical cases.'
    }
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/('queryproof_catalyst_observer_'+spark.version+'.json')).write_text(
        json.dumps({'summary':summary,'cases':outcomes},indent=2),encoding='utf8')
    print('QUERYPROOF_CATALYST_OBSERVER_SUMMARY',json.dumps(summary),flush=True)
    return 1 if summary['mismatches'] or summary['errors'] else 0


if __name__=='__main__':
    raise SystemExit(main())
