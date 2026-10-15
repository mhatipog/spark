"""QueryProof experimental AST-level change ranking. NOT a soundness proof.
Scores use only structural changes, not issue IDs, known rule names or reference answers.
"""
import argparse,base64,json
from collections import Counter,defaultdict
from pathlib import Path

def decode(line):
    if not line.startswith('QUERYPROOF_AST\t'):return None
    parts=line.rstrip('\n').split('\t')[1:]
    if len(parts)!=7:raise ValueError('bad AST event')
    values=[base64.b64decode(p,validate=True).decode('utf8') for p in parts]
    case,executor,batch,rule,iteration,before,after=values
    old,new=json.loads(before),json.loads(after)
    if not isinstance(old,list) or not isinstance(new,list):raise ValueError('bad AST payload')
    return dict(case=case,executor=executor,batch=batch,rule=rule,
                iteration=int(iteration),old=old,new=new)

def parse_tree(nodes):
    def descend(pos):
        if pos>=len(nodes):raise ValueError('truncated')
        node=nodes[pos];amount=node.get('num-children')
        if type(amount) is not int or amount<0:raise ValueError('bad child count')
        children=[];end=pos+1
        for _ in range(amount):
            child,end=descend(end);children.append(child)
        return (node['class'].split('.')[-1],children),end
    root,end=descend(0)
    if end!=len(nodes):raise ValueError('extra nodes')
    return root

def project_above_boundary(tree):
    def visit(tree,above):
        name,children=tree
        if above and name in ('GlobalLimit','LocalLimit','Offset'):return True
        return any(visit(c,above or name=='Project') for c in children)
    return visit(tree,False)

def changes(ev):
    a=Counter(n['class'].split('.')[-1] for n in ev['old'])
    b=Counter(n['class'].split('.')[-1] for n in ev['new'])
    scores={}
    if a['GlobalLimit']>b['GlobalLimit']:scores['global_limit_count_decreased']=10
    if a['LocalLimit']>b['LocalLimit']:scores['local_limit_count_decreased']=5
    if a['Sort']>b['Sort']:scores['sort_count_decreased']=8
    if b['Join']>a['Join'] and b['GlobalLimit']>a['GlobalLimit']:
        scores['subquery_rewrite_introduces_join_and_limit']=7
    if project_above_boundary(parse_tree(ev['old'])) and not project_above_boundary(parse_tree(ev['new'])):
        scores['project_moved_below_selection_boundary']=6
    return scores

def analyze(path):
    events=[];errors=[]
    for lineno,line in enumerate(Path(path).read_text(errors='replace').splitlines(),1):
        if line.startswith('QUERYPROOF_AST\t'):
            try:events.append(decode(line))
            except (ValueError,KeyError) as e:errors.append({'line':lineno,'error':str(e)})
    ranked=defaultdict(lambda:defaultdict(lambda:dict(score=0,features=set(),indices=[])))
    for idx,e in enumerate(events):
        try:feat=changes(e)
        except (ValueError,KeyError,RecursionError) as err:
            errors.append({'event':idx,'error':str(err)});continue
        if feat:
            item=ranked[e['case']][e['rule']]
            item['score']+=sum(feat.values());item['features'].update(feat)
            item['indices'].append(idx)
    out={}
    for case,rules in ranked.items():
        out[case]=sorted([dict(rule=k,score=v['score'],features=sorted(v['features']),
                               event_indices=v['indices']) for k,v in rules.items()],
                         key=lambda x:(-x['score'],x['rule']))
    return {'n_events':len(events),'errors':errors,'ranked_candidates':out,
            'warning':'Candidates only. No semantic safety or causality proof.'}

def main():
    p=argparse.ArgumentParser();p.add_argument('log');p.add_argument('--out',required=True)
    args=p.parse_args();out=analyze(args.log)
    Path(args.out).write_text(json.dumps(out,indent=2))
    print('QUERYPROOF_TRIAGE',out['n_events'],'events',len(out['errors']),'errors',flush=True)
    if not out['n_events'] or out['errors']:raise SystemExit(2)
if __name__=='__main__':main()
