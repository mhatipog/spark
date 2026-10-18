"""Automatically derive exact 3VL observation demands from Catalyst analyzed ASTs.

This inspects operator / expression *structure* through Spark's JVM API.
It never reads SQL strings or expected answers, and makes no wrong-result
claim. A requirement is a promise that a LOCAL substitution should preserve
a given observational partition; it is not evidence a substitution occurred.
"""
import time
from lattice import LABELS,requirement

OPERATOR={
    'Not':'NOT',
    'And':'AND',
    'Or':'OR',
    'IsNull':'IS_NULL',
    'IsNotNull':'IS_NOT_NULL',
    'IsTrue':'IS_TRUE',
    'IsFalse':'IS_FALSE',
    'EqualNullSafe':'EQUAL_NULL_SAFE',
}
TARGETS={'Or','InSubquery','EqualNullSafe','Not','IsNull'}
VALID_ROOTS={'Filter','Join','Project','Aggregate','Sort'}
PARENT_PRESERVE={'Alias','SortOrder'}


def kind(node):
    return str(node.getClass().getSimpleName())


def items(s,cap=256):
    n=int(s.size())
    if n>cap:
        raise ValueError('PLAN_SEQUENCE_OVER_LIMIT')
    return [s.apply(i) for i in range(n)]


def name_partition(partition):
    return next((name for name,p in LABELS.items() if p==partition),'UNKNOWN_PARTITION')


def scalar_type(exp):
    try:
        return str(exp.dataType().typeName())
    except Exception:
        return 'UNKNOWN'


def visit_expression(expr,demand,consumer,path,records,depth=0):
    if depth>32:
        records.append({'status':'UNKNOWN','reason':'EXPRESSION_DEPTH_LIMIT','path':path})
        return
    k=kind(expr)
    node_path=path+[k]
    if k in TARGETS:
        records.append({'status':'REQUIREMENT','target':k,
                        'partition':name_partition(demand),
                        'consumer':consumer,'path':node_path,
                        'expr_type':scalar_type(expr)})
    if k in ('InSubquery','ScalarSubquery','Exists','ListQuery'):
        # Subquery plan is a separate relation; no "child" demand inference.
        return
    try:
        children=items(expr.children(),32)
    except Exception as exc:
        records.append({'status':'UNKNOWN','reason':'EXPR_CHILD_ACCESS:'+str(exc)[:120],
                        'path':node_path})
        return
    for idx,child in enumerate(children):
        if k in PARENT_PRESERVE:
            new_demand=demand
        elif k in OPERATOR and scalar_type(child)=='boolean':
            try:
                new_demand=requirement(demand,OPERATOR[k],idx)
            except Exception:
                new_demand=LABELS['FULL_3VL']
        else:
            # The consumer may inspect the exact value. Never assume less
            # than FULL_3VL in an unsupported evaluation context.
            new_demand=LABELS['FULL_3VL']
        visit_expression(child,new_demand,consumer,node_path,records,depth+1)


def observe_analyzed(df):
    started=time.perf_counter()
    result=[]
    try:
        analyzed=df._jdf.queryExecution().analyzed()
        pending=[(analyzed,[])]
        seen=0
        while pending:
            plan,path=pending.pop()
            seen+=1
            if seen>200:
                return {'status':'UNKNOWN','reason':'PLAN_TOO_LARGE'}
            k=kind(plan)
            pp=path+[k]
            roots=[]
            if k=='Filter':
                roots=[('condition',plan.condition(),LABELS['TRUE_ONLY'])]
            elif k=='Join':
                option=plan.condition()
                if bool(option.isDefined()):
                    roots=[('condition',option.get(),LABELS['TRUE_ONLY'])]
            elif k=='Project':
                roots=[('project_'+str(i),v,LABELS['FULL_3VL'])
                       for i,v in enumerate(items(plan.projectList()))]
            elif k=='Aggregate':
                roots=[('aggregate_'+str(i),v,LABELS['FULL_3VL'])
                       for i,v in enumerate(items(plan.aggregateExpressions()))]
            elif k=='Sort':
                roots=[('ordering_'+str(i),v,LABELS['FULL_3VL'])
                       for i,v in enumerate(items(plan.order()))]
            else:
                # Unsupported operator's expressions are not interpreted.
                # This is narrower but more defensible than guessing their observer.
                roots=[]
            for label,expr,demand in roots:
                visit_expression(expr,demand,k+'.'+label,pp,result)
            pending.extend((x,pp) for x in reversed(items(plan.children())))
        return {'status':'OBSERVED','requirements':result,
                'analyzed_plan_nodes':seen,
                'inspection_ms':round((time.perf_counter()-started)*1000,2)}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':type(exc).__name__+': '+str(exc)[:300]}
