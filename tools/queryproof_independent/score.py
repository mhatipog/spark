"""Post-execution scoring; manifest expected SQL results are read ONLY HERE."""
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent
manifest=json.loads((ROOT.parent/'queryproof_prospective'/'manifest.json').read_text())


def canonical(rows,comparison):
    x=[json.dumps(r,sort_keys=True,default=str) for r in rows]
    return x if comparison=='sequence' else sorted(x)


def score(data):
    per_case={}
    for case in manifest['cases']:
        entry=data['raw_results'][case['id']]
        base=entry['runs']['baseline']
        expected=canonical(case['expected'],case['comparison'])
        if base['status']=='OK':
            actual=canonical(base['rows'],case['comparison'])
            bug_reproduced=(actual!=expected)
            order=base.get('order_verdict',{}).get('verdict','UNKNOWN')
            claim=order=='CONTRACT_VIOLATED'
            # Do not run auto_topk on arbitrary issue families: it only accepts a
            # single CTE direct source and identity, which these cases lack.
            stage='UNKNOWN_OUTSIDE_TOPK_STAGE_SCOPE'
        else:
            bug_reproduced=None;claim=False;stage='UNKNOWN_EXECUTION_ERROR';order='UNKNOWN'
        diffs=[k for k,v in entry['differential'].items()
               if v.get('result_divergence') is True and v.get('physical_plans_differ') is True]
        mismatches_no_change=[k for k,v in entry['differential'].items()
                              if v.get('result_divergence') is True and
                              not v.get('physical_plans_differ')]
        per_case[case['id']]={
            'issue':case['issue'],'kind':case['kind'],'category':case['category'],
            'baseline_wrong_by_independent_expected_answer':bug_reproduced,
            'automatic_order_verdict':order,'automatic_topk_verdict':stage,
            'queryproof_witnessed_incorrect_output':claim,
            'alternate_plan_result_disagreements':diffs,
            'alternate_result_disagreements_without_plan_change':mismatches_no_change,
            'any_dqp_inspired_divergence':bool(diffs),
            'counted_as_out_of_family':case['id']!='B04',
            'notes':'Divergence is evidence of inconsistency, not which arm is correct.'}
    unseen=[v for k,v in per_case.items() if k in ['B01','B02','B03','B05']]
    reproducible=[v for v in unseen if v['baseline_wrong_by_independent_expected_answer'] is True]
    negative=[v for v in per_case.values() if v['kind']=='negative_control']
    summary={
        'spark_version':data['version'],'historical_bugs_count':len(unseen),
        'historical_bugs_reproduced_count':len(reproducible),
        'known_ordering_calibration_B04_excluded_from_out_of_family':True,
        'queryproof_true_witnesses_on_reproduced':sum(v['queryproof_witnessed_incorrect_output'] for v in reproducible),
        'queryproof_unknown_on_reproduced':sum(not v['queryproof_witnessed_incorrect_output'] for v in reproducible),
        'queryproof_false_accusations_controls':sum(v['queryproof_witnessed_incorrect_output'] for v in negative),
        'dqp_inspired_disagreements_reproduced':sum(v['any_dqp_inspired_divergence'] for v in reproducible),
        'dqp_inspired_disagreements_controls':sum(v['any_dqp_inspired_divergence'] for v in negative),
        'failed_baseline_executions':[k for k,v in per_case.items()
                                      if v['baseline_wrong_by_independent_expected_answer'] is None],
        'caveats':['Only four historical independent-family bug cases, three earlier reproducible',
                   'No newly unseen independent Spark bugs found by this scoring',
                   'SQLancer and its DQP oracle were NOT executed',
                   'No guarantee that a plan-text difference implies distinct physical semantics',
                   'All expected SQL answers were withheld from detector during execution',
                   'Zero queryproof witnesses does not imply correct Spark answers'],
    }
    return {'summary':summary,'cases':per_case}


def main(path):
    raw=json.loads(Path(path).read_text())
    results=score(raw)
    dest=Path('artifacts')/('queryproof_independent_score_'+raw['version']+'.json')
    dest.write_text(json.dumps(results,indent=2),encoding='utf8')
    print('QUERYPROOF_INDEPENDENT_SCORE',json.dumps(results['summary']),flush=True)
    for k,v in results['cases'].items():
        print('QUERYPROOF_INDEPENDENT_CASE',k,v['issue'],
              'oracle_wrong',v['baseline_wrong_by_independent_expected_answer'],
              'contract',v['automatic_order_verdict'],
              'differential_disagreements',v['alternate_plan_result_disagreements'],
              'nonplan_disagreement',v['alternate_result_disagreements_without_plan_change'],
              flush=True)
    return 1 if results['summary']['failed_baseline_executions'] or results['summary']['queryproof_false_accusations_controls'] else 0


if __name__=='__main__':
    raise SystemExit(main(sys.argv[1]))
