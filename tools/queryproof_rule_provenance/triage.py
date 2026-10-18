"""Read instrumented Catalyst rule transitions and attach minimal observation demands.

Before/after AST JSON is examined structurally, never using a rule-name
allowlist or reading the manifest expected SQL answers. Candidate generation
does NOT prove unsoundness: only a finite semantic witness or a controlled
intervention can support that separate claim.
"""
import base64,json,sys
from collections import Counter,defaultdict
from pathlib import Path

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'queryproof_instrumented'))
from rank_ast import decode

TRACK={'Or','Not','InSubquery','ListQuery','Exists','EqualTo','EqualNullSafe',
       'IsNull','IsNotNull','Join','ExistenceJoin','Aggregate','First',
       'Project','Filter','Sort','ScalarSubquery','GlobalLimit','LocalLimit'}

def nested_classes(data):
    """Include expression fields (not only main LogicalPlan child sequence)."""
    acc=Counter()
    def visit(x):
        if isinstance(x,dict):
            v=x.get('class')
            if isinstance(v,str):
                acc[v.rsplit('.',1)[-1]]+=1
            for child in x.values():
                visit(child)
        elif isinstance(x,list):
            for child in x:visit(child)
    visit(data)
    return acc


def demanded(observation,case):
    req=observation['observations'].get(case,{}).get('observer_requirements',{})
    if req.get('status')!='OBSERVED':
        return []
    return [dict(target=x['target'],partition=x['partition'],path=x['path'])
            for x in req.get('requirements',[])
            if x.get('status')=='REQUIREMENT'
            and x.get('target') in ('Or','InSubquery','Not')]


def candidate(ev,requirements):
    old,new=nested_classes(ev['old']),nested_classes(ev['new'])
    lost={k:old[k]-new[k] for k in TRACK if old[k]>new[k]}
    gained={k:new[k]-old[k] for k in TRACK if new[k]>old[k]}
    # This is a *structural risk* only; no rewrite correctness assertion.
    signatures=[]
    for req in requirements:
        key=req['target']; demand=req['partition']
        if lost.get(key,0):
            if demand!='TRUE_ONLY':
                signatures.append({'target':key,'demand':demand,
                                   'kind':'NULL_OBSERVABLE_OPERATOR_LOST',
                                   'strength':'candidate'})
            else:
                signatures.append({'target':key,'demand':demand,
                                   'kind':'TRUE_ONLY_OPERATOR_LOST',
                                   'strength':'weak'})
    if lost.get('Aggregate',0):
        signatures.append({'target':'Aggregate','kind':'AGGREGATE_STAGE_REMOVED',
                           'strength':'weak'})
    return lost,gained,signatures


def run(log_path,observations_path):
    observation=json.loads(Path(observations_path).read_text())
    cases=observation['observations'].keys()
    all_events=defaultdict(list)
    parse_errors=[]
    for number,line in enumerate(Path(log_path).read_text(errors='replace').splitlines(),1):
        if not line.startswith('QUERYPROOF_AST\t'):continue
        try:
            ev=decode(line)
            if ev['case'] not in cases:continue
            required=demanded(observation,ev['case'])
            lost,gained,signatures=candidate(ev,required)
            all_events[ev['case']].append({'executor':ev['executor'],
                'batch':ev['batch'],'rule':ev['rule'],'iteration':ev['iteration'],
                'lost':lost,'gained':gained,'risk_signatures':signatures,
                'requirements':[{'target':r['target'],'partition':r['partition']}
                                for r in required]})
        except Exception as exc:
            parse_errors.append({'line':number,'error':type(exc).__name__+':'+str(exc)[:160]})
    cases_out={}
    for cid in cases:
        events=all_events[cid]
        candidates=[dict(index=i,**x) for i,x in enumerate(events) if x['risk_signatures']]
        tier=lambda v:sum(5 if x['strength']=='candidate' else 1 for x in v['risk_signatures'])
        rank=sorted(candidates,key=lambda x:(-tier(x),x['index']))
        cases_out[cid]={'events_seen':len(events),'rule_transitions':events,
                        'first_structural_risk':candidates[0] if candidates else None,
                        'ranked_candidates':rank[:12],
                        'report_only':'Risk candidate, NOT demonstrated violation'}
        print('QUERYPROOF_FIRST_RULE',cid,'events',len(events),
              'first',candidates[0]['rule'] if candidates else 'NONE',
              'top',rank[0]['rule'] if rank else 'NONE',flush=True)
    output={'experiment':'rule provenance, existing cases','cases':cases_out,
            'errors':parse_errors,
            'caution':'May include nested subquery AST representations; class disappearance is NOT semantic violation or proof of causal error'}
    Path('artifacts/queryproof_rule_triage.json').write_text(json.dumps(output,indent=2))
    print('QUERYPROOF_RULE_TRIAGE_SUMMARY',json.dumps({
         'cases':{k:{'events':v['events_seen'],'candidates':len(v['ranked_candidates'])}
                  for k,v in cases_out.items()},'errors':len(parse_errors)}),flush=True)
    return 1 if parse_errors or not any(v['events_seen'] for v in cases_out.values()) else 0


if __name__=='__main__':
    raise SystemExit(run(sys.argv[1],sys.argv[2]))
