"""Symbolic cardinality-observation model for value movement across Aggregate.

NOT a parser of an actual Spark rule transition: this is an explicitly
abstracted sub-mechanism motivated by historical SPARK-58429.

For ungrouped global aggregation, COUNT(*) yields exactly one output row for
every input multiplicity n>=0. Moving an independent scalar v into per-input
rows and recovering it with FIRST produces NULL if n==0, else v. The exact
safety precondition for this abstract transformation is:
  n > 0 OR v = NULL.
"""
import json,hashlib
import z3

TRUE=1
FALSE=0
UNKNOWN=-1

def solve(guard='unconditional',timeout_ms=3000):
    n=z3.Int('source_row_count')
    v=z3.Int('scalar_three_valued')
    domain=z3.And(n>=0,z3.Or(v==TRUE,v==FALSE,v==UNKNOWN))
    before=v
    after=z3.If(n==0,z3.IntVal(UNKNOWN),v)
    guards={
        'unconditional':z3.BoolVal(True),
        'nonempty_source':n>0,
        'scalar_always_null':v==UNKNOWN,
        'synthesized_exact':z3.Or(n>0,v==UNKNOWN),
        'nonnegative_source':n>=0,
        'empty_source':n==0,
    }
    if guard not in guards:raise ValueError('UNKNOWN_GUARD')
    s=z3.Solver()
    s.set(timeout=timeout_ms)
    s.add(domain,guards[guard],before!=after)
    smt=s.to_smt2()
    res=s.check()
    out={'guard':guard,'smt_result':str(res).upper(),'smt2':smt,
         'sha256':hashlib.sha256(smt.encode()).hexdigest()}
    if res==z3.sat:
        model=s.model()
        out['witness']={'n':model.eval(n,model_completion=True).as_long(),
                        'v':model.eval(v,model_completion=True).as_long()}
    elif res==z3.unknown:
        out['reason']=s.reason_unknown()
    return out


def derive():
    checks={name:solve(name) for name in (
        'unconditional','nonempty_source','scalar_always_null',
        'synthesized_exact','nonnegative_source','empty_source')}
    finite=[]
    for n in (0,1,2):
        for v in (TRUE,FALSE,UNKNOWN):
            actual=UNKNOWN if n==0 else v
            safe=(v==actual)
            guard=(n>0 or v==UNKNOWN)
            finite.append({'n':n,'v':v,'safe':safe,'guard_allows':guard})
            if safe!=guard:raise AssertionError('FINITE_GUARD_UNSOUND')
    correct=(checks['unconditional']['smt_result']=='SAT'
             and checks['nonempty_source']['smt_result']=='UNSAT'
             and checks['scalar_always_null']['smt_result']=='UNSAT'
             and checks['synthesized_exact']['smt_result']=='UNSAT'
             and checks['nonnegative_source']['smt_result']=='SAT'
             and checks['empty_source']['smt_result']=='SAT'
             and all(x['witness']['n']==0 and
                     x['witness']['v'] in (TRUE,FALSE)
                     for key,x in checks.items() if x['smt_result']=='SAT'))
    return {'passed':correct,'checks':checks,'finite_replay':finite,
            'exact_abstract_guard':'source_row_count > 0 OR scalar_is_NULL',
            'semantics':'UNGROUPED global aggregate returns one row; per-source FIRST(NULL on empty)',
            'limitations':'Abstract mechanistic model only. Actual Catalyst rule mapping not proved.'}


if __name__=='__main__':
    import pathlib
    out=derive()
    pathlib.Path('artifacts').mkdir(exist_ok=True)
    pathlib.Path('artifacts/queryproof_cardinality_smt.json').write_text(json.dumps(out,indent=2))
    pathlib.Path('artifacts/queryproof_cardinality_smt2').mkdir(exist_ok=True)
    for name,check in out['checks'].items():
        (pathlib.Path('artifacts/queryproof_cardinality_smt2')/(name+'.smt2')).write_text(check['smt2'])
    print('QUERYPROOF_CARDINALITY_SMT',json.dumps({
        'passed':out['passed'],
        'results':{name:v['smt_result'] for name,v in out['checks'].items()},
        'exact_abstract_guard':out['exact_abstract_guard']}),flush=True)
    raise SystemExit(0 if out['passed'] else 1)
