"""Replay actual Catalyst certificates with unrestricted Z3 nullable-integer proof.

SMT proof generation never consults the expected SQL output rows. The
historical case labels are used only in post-proof calibration assertions.
"""
import json
from pathlib import Path

from smt_validator import certify
from sqlite_check import emit

import sqlite3


def replay_sat_witness(cert,smt):
    witness=smt['root'].get('witness')
    if not witness:return {'status':'NO_WITNESS'}
    keys=sorted(witness)
    if len(keys)!=2:return {'status':'BAD_ATTRIBUTE_SET'}
    assignments={k:v for k,v in witness.items()}
    names={keys[0]:'a',keys[1]:'b'}
    query='SELECT '+emit(cert['before_expression'],names)+', '+emit(cert['after_expression'],names)+' FROM (SELECT ? AS a, ? AS b)'
    with sqlite3.connect(':memory:') as conn:
        before,after=conn.execute(query,tuple(assignments[k] for k in keys)).fetchone()
    return {'status':'REPLAYED' if (before==1)!=(after==1) else 'NOT_DISTINGUISHED',
            'values':[assignments[k] for k in keys],
            'before':None if before is None else bool(before),
            'after':None if after is None else bool(after)}


def main():
    data=json.loads(Path('artifacts/queryproof_contextual_rule_certificates.json').read_text())
    results={}
    for cid,events in data['events'].items():
        certificates=[]
        for event in events:
            if event['status'] not in ('COUNTEREXAMPLE','EQUIVALENT_IN_FRAGMENT'):
                continue
            z3_result=certify(event)
            if z3_result['status']=='COUNTEREXAMPLE':
                z3_result['independent_sqlite_witness']=replay_sat_witness(event,z3_result)
            certificates.append({'rule':event['rule'],
                                 'original_context_demand':event['required_partition'],
                                 'original_local_guard':event['synthesized_guard_decision'],
                                 'solver':z3_result})
            print('QUERYPROOF_SMT_REAL_RULE',cid,event['rule'],
                  z3_result['status'],z3_result.get('guard','UNKNOWN'),
                  z3_result.get('safe_observation_partitions',[]),flush=True)
        if certificates:results[cid]=certificates
    b=results.get('B01',[])
    n=results.get('N01',[])
    confirmed=(len(b)==1 and len(n)==1
       and b[0]['solver']['status']=='COUNTEREXAMPLE'
       and b[0]['solver']['root']['solver_status']=='SAT'
       and b[0]['solver']['guard']=='BLOCK'
       and b[0]['solver']['independent_sqlite_witness']['status']=='REPLAYED'
       and n[0]['solver']['status']=='PROVED_WITHIN_FRAGMENT'
       and n[0]['solver']['root']['solver_status']=='UNSAT'
       and n[0]['solver']['guard']=='ALLOW'
       and b[0]['solver']['safe_observation_partitions']==['ANY','TRUE_ONLY']
       and n[0]['solver']['safe_observation_partitions']==['ANY','TRUE_ONLY'])
    summary={'solver':'Z3','confirmed':confirmed,'supported_actual_cases':sorted(results),
             'in_sample':True,'no_known_expected_output_used_for_proofs':True,
             'not_proved':'Full relational query equivalence, attribute reachability in arbitrary datasets, arbitrary Spark rules',
             'claim':'SMT SAT/UNSAT for unrestricted mathematical integer equality plus independent nullable flags under real matched JOIN condition rule'}
    Path('artifacts/queryproof_context_smt.json').write_text(
        json.dumps({'summary':summary,'results':results},indent=2),encoding='utf8')
    Path('artifacts/queryproof_context_smt2').mkdir(exist_ok=True)
    for cid,items in results.items():
        for ix,e in enumerate(items):
            results_=e['solver']
            for observer,out in [('ROOT_TRUE',results_['root']),*list(results_['partition_results'].items())]:
                (Path('artifacts/queryproof_context_smt2')/
                    (cid+'_'+str(ix)+'_'+observer+'.smt2')).write_text(out['smt2'])
    print('QUERYPROOF_SMT_SUMMARY',json.dumps(summary),flush=True)
    return 0 if confirmed else 1

if __name__=='__main__':raise SystemExit(main())
