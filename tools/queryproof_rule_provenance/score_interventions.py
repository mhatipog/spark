"""Postexecution causal scoring, independent of candidate selection or trace.

Ablation alone cannot prove rule unsoundness, especially where the rule is
mandatory and skipping it produces invalid/unevaluable intermediate plans.
"""
import json,sys
from pathlib import Path
from collections import Counter

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent))
from queryproof_prospective.run import MANIFEST


def canon(rows,mode):
    texts=[json.dumps(x,sort_keys=True,default=str) for x in rows]
    return texts if mode=='sequence' else sorted(texts)


def analyze(data):
    cases={x['id']:x for x in MANIFEST['cases']}
    scored={}
    for cid,e in data['observations'].items():
        c=cases[cid]
        expected=canon(c['expected'],c['comparison'])
        base=e['arms']['baseline']
        obs={}
        for arm,v in e['arms'].items():
            executable=v['status']=='OBSERVED'
            correct=canon(v['rows'],c['comparison'])==expected if executable else None
            causal=(arm!='baseline' and executable and correct and
                not (canon(base['rows'],c['comparison'])==expected) and
                bool(v.get('plan_changed_vs_baseline')))
            obs[arm]={'status':v['status'],'correct_against_frozen_answer':correct,
                      'plan_changed':v.get('plan_changed_vs_baseline',False),
                      'rows_changed':v.get('rows_changed_vs_baseline',False),
                      'forced_skips':v.get('skip_invocations',None),
                      'error':v.get('error'),
                      'causal_restoration_candidate':bool(causal)}
            print('QUERYPROOF_CAUSAL_SCORE',cid,arm,
                  obs[arm]['status'],correct,
                  'RESTORED' if causal else 'NOT_RESTORED',
                  'CHANGED_PLAN' if obs[arm]['plan_changed'] else 'SAME_PLAN',
                  flush=True)
        scored[cid]={'rule':e['candidate_rule'],'kind':c['kind'],'arms':obs}
    neg=[v for v in scored.values() if v['kind']=='negative_control']
    bug=[v for v in scored.values() if v['kind']=='bug']
    summary={'version':data['version'],
             'baseline_wrong_bug_cases':[k for k,v in scored.items() if
                v['kind']=='bug' and v['arms']['baseline']['correct_against_frozen_answer'] is False],
             'restored_by_sqlconf':[k for k,v in scored.items()
                                    if v['arms']['sqlconf']['causal_restoration_candidate']],
             'restored_by_force':[k for k,v in scored.items()
                                  if v['arms']['forced']['causal_restoration_candidate']],
             'control_regressions_by_sqlconf':[k for k,v in scored.items() if
                  v['kind']=='negative_control' and v['arms']['sqlconf']['correct_against_frozen_answer'] is False],
             'control_regressions_by_force':[k for k,v in scored.items() if
                  v['kind']=='negative_control' and v['arms']['forced']['correct_against_frozen_answer'] is False],
             'inconclusive_forced':[k for k,v in scored.items() if
                  v['arms']['forced']['correct_against_frozen_answer'] is None],
             'caution':'Ablation restoring answer is intervention evidence, not proof rewrite violates obligation for all SQL. Mandatory rule errors are INCONCLUSIVE.'}
    return {'summary':summary,'cases':scored}


def main(path):
    data=json.loads(Path(path).read_text())
    out=analyze(data)
    Path('artifacts/queryproof_causal_score.json').write_text(json.dumps(out,indent=2))
    print('QUERYPROOF_CAUSAL_SUMMARY',json.dumps(out['summary']),flush=True)
    return 0


if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
