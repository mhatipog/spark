"""Read a synthetic RuleExecutor log and summarize B01/N01 Join condition ASTs.

Only structural AST shape and synthetic fixture expression IDs are emitted.
No expected answers are read. This is exploratory format validation before
implementing a certified before/after comparison.
"""
import json,sys
from pathlib import Path

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'queryproof_instrumented'))
from rank_ast import decode


def compress(obj,depth=0):
    if depth>8:return 'MAX_DEPTH'
    if isinstance(obj,list):
        return [compress(v,depth+1) for v in obj[:30]]
    if isinstance(obj,dict):
        keep=['class','num-children','exprId','name','left','right','child',
              'dataType','direction','value','nullOrdering']
        return {k:compress(obj[k],depth+1) for k in keep if k in obj}
    if isinstance(obj,(str,int,float,bool)) or obj is None:
        return str(obj)[:70] if isinstance(obj,str) else obj
    return 'UNKNOWN'


def get_joins(ast):
    return [x for x in ast if x.get('class','').rsplit('.',1)[-1]=='Join']


def main(path):
    seen={}
    count=0
    for line in Path(path).read_text(errors='replace').splitlines():
        if not line.startswith('QUERYPROOF_AST\t'):continue
        ev=decode(line)
        if ev['case'] not in ('B01','N01'):continue
        count+=1
        old,new=get_joins(ev['old']),get_joins(ev['new'])
        if not old or not new:continue
        if json.dumps([x.get('condition') for x in old],sort_keys=True)==json.dumps([x.get('condition') for x in new],sort_keys=True):
            continue
        key=(ev['case'],ev['rule'],ev['iteration'])
        if key in seen:continue
        seen[key]=True
        print('QUERYPROOF_CONDITION_FORMAT',json.dumps({
          'case':ev['case'],'rule':ev['rule'],'iteration':ev['iteration'],
          'old_join_count':len(old),'new_join_count':len(new),
          'old_conditions':[compress(j.get('condition')) for j in old],
          'new_conditions':[compress(j.get('condition')) for j in new],
          'old_join_type':[compress(j.get('joinType')) for j in old],
          'new_join_type':[compress(j.get('joinType')) for j in new],
        }),flush=True)
    print('QUERYPROOF_FORMAT_SUMMARY',json.dumps({'events':count,'changed_join_condition_events':len(seen)}),flush=True)
    return 1 if not seen else 0

if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
