"""Independent certificate checking using explicit SQL truth tables, not producer code."""
import json,sys
from pathlib import Path
from itertools import product

T=(None,False,True)
V=(None,0,1)
SAFE={'filter','and_filter','or_filter','double_not_filter'}
UNSAFE={'not_filter','null_filter','projection'}

def unary_not(x):
    return {True:False,False:True,None:None}[x]
def truth_or(x,y):
    return True if True in (x,y) else None if None in (x,y) else False
def truth_and(x,y):
    return False if False in (x,y) else None if None in (x,y) else True

def evaluate(a,b,s,context,use_rewrite):
    eq=None if a is None or b is None else a==b
    bothnull=(a is None and b is None)
    p=(a==b) if use_rewrite else truth_or(eq,bothnull)
    if context=='filter':return p is True
    if context=='not_filter':return unary_not(p) is True
    if context=='null_filter':return p is None
    if context=='projection':return 'NULL' if p is None else p
    if context=='and_filter':return truth_and(p,s) is True
    if context=='or_filter':return truth_or(p,s) is True
    if context=='double_not_filter':return unary_not(unary_not(p)) is True
    raise ValueError(context)

def check_one(item):
    context=item['context']
    siblings=T if context in ('and_filter','or_filter') else (None,)
    grid=[(a,b,s) for a,b,s in product(V,V,siblings)]
    differences=[(a,b,s) for a,b,s in grid
       if evaluate(a,b,s,context,False)!=evaluate(a,b,s,context,True)]
    claimed=item['witness']
    if differences:
        assert item['status']=='COUNTEREXAMPLE' and claimed is not None,context
        a,b,s=claimed['a'],claimed['b'],claimed['sibling']
        assert (a,b,s) in differences, context
        assert claimed['lhs']==evaluate(a,b,s,context,False),context
        assert claimed['rhs']==evaluate(a,b,s,context,True),context
    else:
        assert item['status']=='BOUNDED_EQUIVALENT' and claimed is None,context
    assert item['checked_inputs']==len(grid)
    assert item['counterexample_count']==len(differences),context

def run(filepath):
    artifact=json.loads(Path(filepath).read_text())
    cases=artifact['cases']
    assert {x['context'] for x in cases}==SAFE|UNSAFE
    for case in cases:check_one(case)
    observed_safe={x['context'] for x in cases if x['status']=='BOUNDED_EQUIVALENT'}
    observed_bad={x['context'] for x in cases if x['status']=='COUNTEREXAMPLE'}
    assert observed_safe==SAFE and observed_bad==UNSAFE
    print('QUERYPROOF_INDEPENDENT_CERTIFICATE_CHECK',len(cases),'PASSED',flush=True)

if __name__=='__main__':run(sys.argv[1])
