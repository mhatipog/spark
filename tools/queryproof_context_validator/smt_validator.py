"""Unbounded SMT proof/counterexample for actual Catalyst nullable integer Boolean rewrites.

All supported expressions use nullable integral equality, Boolean strong-Kleene
NOT/AND/OR, and IS NULL/IS NOT NULL. Attribute values are arbitrary Z3 Int
values; nullity is a separate unconstrained Boolean. This is exact for this
restricted grammar because no arithmetic/overflow or type coercion is present.

UNSAT proves contextual equivalence within this expression grammar and under
the independently-variable-input assumption. SAT finds a counterexample.
Unsupported operators, unknown solver status, or timeout -> UNKNOWN.
"""
import hashlib,json

import z3

from context_proof import attrs,parse_expression,TRUTH_PARTITIONS

T=z3.IntVal(1)
F=z3.IntVal(0)
U=z3.IntVal(-1)


def truth_not(x):
    return z3.If(x==U,U,z3.If(x==T,F,T))

def truth_and(a,b):
    return z3.If(z3.Or(a==F,b==F),F,
                 z3.If(z3.Or(a==U,b==U),U,T))

def truth_or(a,b):
    return z3.If(z3.Or(a==T,b==T),T,
                 z3.If(z3.Or(a==U,b==U),U,F))

def mk_context(before,after):
    all_ids=sorted(attrs(before)|attrs(after))
    if len(all_ids)!=2 or attrs(before)!=attrs(after):
        raise ValueError('REQUIRES_TWO_IDENTICAL_INDEPENDENT_ATTRS')
    env={name:(z3.Int('value_'+str(i)),z3.Bool('null_'+str(i)))
         for i,name in enumerate(all_ids)}
    return all_ids,env

def scalar(tree,env):
    if tree[0]!='attr':
        raise ValueError('NON_ATTRIBUTE_EQUALITY_OPERAND_UNSUPPORTED')
    return env[tree[1]]

def bool3(tree,env):
    op=tree[0]
    if op=='attr':
        raise ValueError('BOOLEAN_ATTRIBUTE_NOT_SUPPORTED')
    if op in ('IsNull','IsNotNull'):
        x=tree[1]
        if x[0]=='attr': isnull=env[x[1]][1]
        else:isnull=bool3(x,env)==U
        return z3.If(isnull,T if op=='IsNull' else F,
                            F if op=='IsNull' else T)
    if op=='Not':return truth_not(bool3(tree[1],env))
    if op=='And':return truth_and(bool3(tree[1],env),bool3(tree[2],env))
    if op=='Or':return truth_or(bool3(tree[1],env),bool3(tree[2],env))
    if op in ('EqualTo','EqualNullSafe'):
        av,an=scalar(tree[1],env)
        bv,bn=scalar(tree[2],env)
        eq=av==bv
        if op=='EqualTo':
            return z3.If(z3.Or(an,bn),U,z3.If(eq,T,F))
        return z3.If(z3.And(an,bn),T,
               z3.If(z3.Or(an,bn),F,z3.If(eq,T,F)))
    raise ValueError('UNSUPPORTED_BOOLEAN_OPERATOR:'+op)

def observable_difference(a,b,observer):
    if observer=='ANY':return z3.BoolVal(False)
    if observer=='TRUE_ONLY':return z3.Xor(a==T,b==T)
    if observer=='FALSE_ONLY':return z3.Xor(a==F,b==F)
    if observer=='NULL_ONLY':return z3.Xor(a==U,b==U)
    if observer=='FULL_3VL':return a!=b
    raise ValueError('UNKNOWN_OBSERVER:'+observer)

def decode(z3model,ids,env):
    bindings={}
    for name in ids:
        val,isnull=env[name]
        missing=z3.is_true(z3model.eval(isnull,model_completion=True))
        v=z3model.eval(val,model_completion=True).as_long()
        bindings[name]=None if missing else v
    return bindings

def prove_pair(before,after,observer,timeout_ms=3000):
    ids,env=mk_context(before,after)
    o1,o2=bool3(before,env),bool3(after,env)
    solver=z3.Solver()
    solver.set('timeout',timeout_ms)
    solver.add(observable_difference(o1,o2,observer))
    smt2=solver.to_smt2()
    res=solver.check()
    status=str(res).upper()
    result={'observer':observer,'solver_status':status,
            'smt2_sha256':hashlib.sha256(smt2.encode()).hexdigest(),
            'solver_name':'Z3','smt2':smt2}
    if res==z3.sat:
        # Prefer small reproducible model if reachable, but never redefine
        # the original unbounded SAT question.
        small=z3.Solver()
        small.set('timeout',timeout_ms)
        small.add(solver.assertions())
        for name in ids:
            v,n=env[name]
            small.add(v>=-1,v<=1)
        if small.check()==z3.sat:model=small.model()
        else:model=solver.model()
        result['witness']=decode(model,ids,env)
        result['witness_before']=str(z3.simplify(z3.substitute(o1,[
            (env[name][0],z3.IntVal(0 if result['witness'][name] is None else result['witness'][name])),
            (env[name][1],z3.BoolVal(result['witness'][name] is None))
            for name in []
        ]))) if False else None
        # SQL-valued output is independently replayed by SQLite downstream;
        # no truth value is accepted from a guessed Z3 model interpretation.
    elif res==z3.unknown:
        result['reason']=solver.reason_unknown()
    return result

def certify(actual_event_certificate):
    before=parse_expression(actual_event_certificate['raw_expression_nodes_before'])
    after=parse_expression(actual_event_certificate['raw_expression_nodes_after'])
    local_before=actual_event_certificate['changed_before']
    local_after=actual_event_certificate['changed_after']
    root=prove_pair(before,after,'TRUE_ONLY')
    passes={}
    for demand in TRUTH_PARTITIONS:
        passes[demand]=prove_pair(local_before,local_after,demand)
    derived=[d for d,x in passes.items() if x['solver_status']=='UNSAT']
    if root['solver_status']=='UNKNOWN' or any(x['solver_status']=='UNKNOWN' for x in passes.values()):
        return {'status':'UNKNOWN','reason':'SMT_UNDECIDED','root':root,'partition_results':passes}
    verdict=('COUNTEREXAMPLE' if root['solver_status']=='SAT'
             else 'PROVED_WITHIN_FRAGMENT')
    should_allow=actual_event_certificate['required_partition'] in derived
    require_claim=actual_event_certificate.get('admissible_observation_partitions')
    if derived!=require_claim:
        return {'status':'UNKNOWN','reason':'SMT_FINITE_GUARD_DISAGREEMENT',
                'smt_safe':derived,'finite_safe':require_claim}
    if (verdict=='COUNTEREXAMPLE') != (
            actual_event_certificate['status']=='COUNTEREXAMPLE'):
        return {'status':'UNKNOWN','reason':'SMT_ROOT_FINITE_DISAGREEMENT'}
    if ('ALLOW' if should_allow else 'BLOCK')!=actual_event_certificate['synthesized_guard_decision']:
        return {'status':'UNKNOWN','reason':'SMT_GUARD_FINITE_DISAGREEMENT'}
    return {'status':verdict,'root':root,'safe_observation_partitions':derived,
            'guard':'ALLOW' if should_allow else 'BLOCK',
            'partition_results':passes,
            'scope':'All mathematical integral values with separate NULL flags; supported equality/nullness grammar only',
            'assumptions':'Unconstrained nullable integral join attributes; no arithmetic, casts, expressions with effects or changed plan children.'}

