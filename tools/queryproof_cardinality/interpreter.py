"""Independent exact oracle for *one narrow analyzed* Spark global aggregate.

Supported form:
  SELECT count(*), (literal_int IN (SELECT id FROM range(...))) FROM range(...)

The analyzed Catalyst tree, not SQL text, supplies Count, InSubquery,
range bounds and output positions. No equivalent Spark SQL query is used
to compute the reference. Anything outside this proof scope -> UNKNOWN.
"""
from __future__ import annotations
import time


class Scope(Exception): pass
def demand(test,why):
    if not test:raise Scope(why)
def kind(x):return str(x.getClass().getSimpleName())
def seq(s,max_size=24):
    n=int(s.size());demand(n<=max_size,'TOO_MANY_EXPRESSION_CHILDREN')
    return [s.apply(i) for i in range(n)]


def range_values(node,limit=256):
    demand(kind(node)=='Range','INPUT_NOT_DIRECT_RANGE')
    first,last,step=int(node.start()),int(node.end()),int(node.step())
    demand(step!=0,'ZERO_RANGE_STEP')
    if (last-first)*step<=0:n=0
    else:n=(abs(last-first)+abs(step)-1)//abs(step)
    demand(n<=limit,'RANGE_TOO_LARGE')
    return [first+i*step for i in range(n)]


def literal_number(e):
    demand(kind(e)=='Literal','PROBE_NOT_LITERAL')
    typ=str(e.dataType().typeName())
    demand(typ in ('integer','long','null'),'UNSUPPORTED_LITERAL_TYPE')
    v=e.value()
    return None if v is None else int(v)


def unwrap_named(e):
    demand(kind(e)=='Alias','EXPECTED_ALIAS')
    return e.child()


def count_star(expr):
    demand(kind(expr)=='AggregateExpression','COUNT_WRAPPER_MISSING')
    demand(not bool(expr.isDistinct()),'DISTINCT_COUNT_UNSUPPORTED')
    fn=expr.aggregateFunction()
    demand(kind(fn)=='Count','AGGREGATE_NOT_COUNT')
    children=seq(fn.children())
    demand(len(children)==1,'COUNT_NOT_SINGLE_ARGUMENT')
    arg=children[0]
    demand(kind(arg)=='Literal','COUNT_NOT_CONSTANT_NON_NULL_LITERAL')
    demand(arg.value() is not None,'COUNT_LITERAL_IS_NULL')


def scalar_in(expr):
    demand(kind(expr)=='InSubquery','AGGREGATE_VALUE_NOT_IN_SUBQUERY')
    vals=seq(expr.values())
    demand(len(vals)==1,'MULTICOLUMN_IN_UNSUPPORTED')
    value=literal_number(vals[0])
    query=expr.query()
    demand(kind(query)=='ListQuery','EXPECTED_LIST_QUERY')
    demand(len(seq(query.outerAttrs()))==0,'CORRELATED_IN_UNSUPPORTED')
    demand(len(seq(query.joinCond()))==0,'JOIN_CONDITIONS_UNSUPPORTED')
    plan=query.plan()
    demand(kind(plan)=='Project','IN_QUERY_NOT_PROJECT')
    selections=seq(plan.projectList())
    demand(len(selections)==1 and kind(selections[0])=='AttributeReference',
           'IN_SUBQUERY_DERIVED_PROJECTION')
    source=plan.child()
    domain=range_values(source)
    actual=seq(source.output())
    demand(len(actual)==1,'IN_SOURCE_MULTICOLUMN')
    demand(int(actual[0].exprId().id())==int(selections[0].exprId().id()),
           'IN_SOURCE_ATTRIBUTE_ID_MISMATCH')
    return value,domain


def in_3vl(v,domain):
    if not domain:return False
    if v is None:return None
    if v in domain:return True
    return None if any(x is None for x in domain) else False


def extract(df):
    try:
        node=df._jdf.queryExecution().analyzed()
        demand(kind(node)=='Aggregate','ROOT_NOT_AGGREGATE')
        demand(len(seq(node.groupingExpressions()))==0,'GROUPED_AGGREGATE_UNSUPPORTED')
        output=seq(node.aggregateExpressions())
        demand(len(output)==2,'AGGREGATE_OUTPUT_NOT_TWO')
        first,second=(unwrap_named(x) for x in output)
        count_star(first)
        probe,inner=scalar_in(second)
        outer=range_values(node.child())
        return {'status':'READY','outer_count':len(outer),'inner_count':len(inner),
                'probe':probe,'expected':[len(outer),in_3vl(probe,inner)],
                'observer':'FULL_3VL','cardinality_observer':'ZERO' if not outer else 'NONZERO',
                'provenance':'analyzed Catalyst global Aggregate + Count literal + direct Range + one InSubquery'}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':str(exc)[:320],'exception':type(exc).__name__}


def check(df):
    t=time.perf_counter()
    bound=extract(df)
    if bound['status']!='READY':return bound
    try:
        rows=[list(r) for r in df.collect()]
        expected=[bound['expected']]
        return {**bound,'status':'VALUE_VIOLATION' if rows!=expected else 'NO_VIOLATION_OBSERVED',
                'actual':rows,'expected_rows':expected,
                'duration_ms':round(1000*(time.perf_counter()-t),2)}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':'ENGINE_EXECUTION:'+type(exc).__name__+':'+str(exc)[:200]}
