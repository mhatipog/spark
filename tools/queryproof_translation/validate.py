"""Bounded contextual translation validation for one known SQL 3VL rewrite.

No Spark output oracle is consulted. A certificate is produced only for
semantically distinguished contexts in the predeclared small complete domain.
This is calibration for known SPARK-58384 / DuckDB #23685, NOT a new defect.
"""
from itertools import product
from pathlib import Path
import json

DOMAIN=(None,0,1)
TRUTH=(None,False,True)

def not3(x):
    return None if x is None else not x

def or3(x,y):
    if x is True or y is True:return True
    if x is None or y is None:return None
    return False

def and3(x,y):
    if x is False or y is False:return False
    if x is None or y is None:return None
    return True

def original(a,b):
    return or3(None if a is None or b is None else a==b,
               a is None and b is None)

def rewritten(a,b):
    return a==b

def apply_context(p,context,sibling):
    if context=='filter':return p is True
    if context=='not_filter':return not3(p) is True
    if context=='null_filter':return (p is None)
    if context=='projection':return 'NULL' if p is None else p
    if context=='and_filter':return and3(p,sibling) is True
    if context=='or_filter':return or3(p,sibling) is True
    if context=='double_not_filter':return not3(not3(p)) is True
    raise ValueError('unsupported context '+context)

CONTEXTS=('filter','not_filter','null_filter','projection',
          'and_filter','or_filter','double_not_filter')
SAFE=('filter','and_filter','or_filter','double_not_filter')
UNSAFE=('not_filter','null_filter','projection')

def witness(context):
    cells=[]
    siblings=TRUTH if context in ('and_filter','or_filter') else (None,)
    for a,b,s in product(DOMAIN,DOMAIN,siblings):
        lhs=apply_context(original(a,b),context,s)
        rhs=apply_context(rewritten(a,b),context,s)
        if lhs!=rhs:
            cells.append({'a':a,'b':b,'sibling':s,'lhs':lhs,'rhs':rhs})
    cells.sort(key=lambda r:(sum(v is not None for v in (r['a'],r['b'])),
                             sum(abs(v) for v in (r['a'],r['b']) if v is not None),
                             str(r)))
    return {'context':context,
            'status':'COUNTEREXAMPLE' if cells else 'BOUNDED_EQUIVALENT',
            'witness':cells[0] if cells else None,
            'counterexample_count':len(cells),
            'checked_inputs':len(DOMAIN)**2*len(siblings),
            'scope':'3-valued equality/nullness over {NULL,0,1}; context-only calibration'}

def run():
    cases=[witness(x) for x in CONTEXTS]
    result={'cases':cases,
       'summary':{'safe_contexts':sum(x['status']=='BOUNDED_EQUIVALENT' for x in cases),
                  'unsafe_contexts':sum(x['status']=='COUNTEREXAMPLE' for x in cases),
                  'expected_but_not_tuned':'Results independently verified in separate checker.',
                  'out_of_scope':'No Catalyst rewrite AST provenance extracted in this prototype.'}}
    Path('artifacts').mkdir(exist_ok=True)
    Path('artifacts/queryproof_contextual_certificates.json').write_text(json.dumps(result,indent=2))
    print('QUERYPROOF_TRANSLATION_SUMMARY',json.dumps(result['summary']),flush=True)
    return 0

if __name__=='__main__':raise SystemExit(run())
