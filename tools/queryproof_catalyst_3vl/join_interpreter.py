"""Independent finite-row interpreter for a *narrow analyzed Catalyst inner join*.

Never infers a result from plan text, never asks Spark to run equivalent JOIN.
Reads both simple immutable source views, interprets a supported SQL three-valued
predicate in Python, and compares row *bags* (not output order). Unsupported is
UNKNOWN, including source multiplicity/coverage conditions we cannot certify.
"""
import json
import time
from collections import Counter
from itertools import product


class ScopeError(Exception):
    pass


def require(ok, reason):
    if not ok:
        raise ScopeError(reason)


def kind(jvm_obj):
    return str(jvm_obj.getClass().getSimpleName())


def seq(scala_seq, cap=128):
    n=int(scala_seq.size())
    require(n <= cap, 'TOO_MANY_CATALYST_ELEMENTS')
    return [scala_seq.apply(i) for i in range(n)]


def expr_id(x):
    return int(x.exprId().id())


def compile_expression(e, depth=0):
    require(depth < 20, 'EXPR_DEPTH')
    k=kind(e)
    children=seq(e.children(),16)
    if k=='AttributeReference':
        require(len(children)==0, 'ATTRIBUTE_HAS_CHILDREN')
        return {'op':'attr','id':expr_id(e),'type':str(e.dataType().typeName())}
    if k=='Literal':
        v=e.value()
        t=str(e.dataType().typeName())
        require(t in ('boolean','integer','long'), 'LITERAL_TYPE_UNSUPPORTED')
        return {'op':'literal','value':None if v is None else (bool(v) if t=='boolean' else int(v))}
    unary={'Not':'not','IsNull':'is_null','IsNotNull':'is_not_null'}
    binary={'And':'and','Or':'or','EqualTo':'equal','EqualNullSafe':'equal_null_safe'}
    if k in unary:
        require(len(children)==1,'UNARY_ARITY')
        return {'op':unary[k],'args':[compile_expression(children[0],depth+1)]}
    if k in binary:
        require(len(children)==2,'BINARY_ARITY')
        return {'op':binary[k],'args':[compile_expression(x,depth+1) for x in children]}
    raise ScopeError('EXPRESSION_UNSUPPORTED:'+k)


def kleene_not(a):
    return None if a is None else not a


def kleene_and(a,b):
    return False if a is False or b is False else None if (a is None or b is None) else True


def kleene_or(a,b):
    return True if a is True or b is True else None if (a is None or b is None) else False


def eval_tree(tree, env):
    op=tree['op']
    if op=='attr':
        require(tree['id'] in env,'ATTR_NOT_BOUND')
        return env[tree['id']]
    if op=='literal':
        return tree['value']
    x=eval_tree(tree['args'][0],env)
    if op=='not':
        require(x in (True,False,None),'BOOLEAN_OPERAND_TYPE')
        return kleene_not(x)
    if op=='is_null':
        return x is None
    if op=='is_not_null':
        return x is not None
    y=eval_tree(tree['args'][1],env)
    if op=='and':
        require(x in (True,False,None) and y in (True,False,None),'BOOLEAN_OPERAND_TYPE')
        return kleene_and(x,y)
    if op=='or':
        require(x in (True,False,None) and y in (True,False,None),'BOOLEAN_OPERAND_TYPE')
        return kleene_or(x,y)
    if op=='equal':
        return None if x is None or y is None else x==y
    if op=='equal_null_safe':
        return x==y
    raise ScopeError('UNKNOWN_COMPILED_OPERATOR:'+op)


def direct_source(node):
    require(kind(node)=='SubqueryAlias','SOURCE_NOT_DIRECT_ALIAS')
    alias=str(node.identifier().name())
    require(kind(node.child())=='View','SOURCE_NOT_VIEW')
    schema=seq(node.output())
    require(len(set(expr_id(x) for x in schema))==len(schema),'DUPLICATE_EXPR_IDS')
    cols=[str(x.name()) for x in schema]
    require(len(set(cols))==len(cols),'DUPLICATE_COLUMN_NAMES')
    require(all(str(x.dataType().typeName()) in ('integer','long','boolean')
                for x in schema),'SOURCE_TYPE_OUT_OF_SCOPE')
    return {'view':alias,'column_names':cols,'expr_ids':[expr_id(x) for x in schema]}


def extract(df):
    """Extract one direct INNER JOIN, possibly wrapped by Sort, and identity Project."""
    try:
        analyzed=df._jdf.queryExecution().analyzed()
        node=analyzed
        if kind(node)=='Sort':
            require(bool(getattr(node,'global')()),'SORT_NOT_GLOBAL')
            node=node.child()
        require(kind(node)=='Project','ROOT_NOT_PROJECT')
        outputs=seq(node.projectList())
        require(outputs and all(kind(x)=='AttributeReference' for x in outputs),
                'PROJECT_NOT_DIRECT_ATTRIBUTES')
        child=node.child()
        require(kind(child)=='Join','ROOT_NOT_INNER_JOIN')
        require(str(child.joinType().sql())=='INNER','JOIN_NOT_INNER:'+str(child.joinType().sql()))
        opt=child.condition()
        require(bool(opt.isDefined()),'JOIN_MISSING_CONDITION')
        predicate=compile_expression(opt.get())
        left=direct_source(child.left())
        right=direct_source(child.right())
        bound=set(left['expr_ids'])|set(right['expr_ids'])
        require(len(bound)==len(left['expr_ids'])+len(right['expr_ids']),
                'INPUT_EXPRESSION_ID_COLLISION')
        def used(t):
            return {t['id']} if t['op']=='attr' else {
                k for q in t.get('args',[]) for k in used(q)}
        require(used(predicate).issubset(bound),'UNBOUND_PREDICATE_ATTR')
        out_ids=[expr_id(x) for x in outputs]
        require(set(out_ids).issubset(bound),'OUTPUT_ATTR_NOT_IN_JOIN')
        return {'status':'READY','left':left,'right':right,
                'predicate_ast':predicate,'output_expr_ids':out_ids,
                'obligation':'COMPLETE_3VL_INNER_JOIN_ROW_BAG',
                'source_assumption':'immutable small captured views'}
    except Exception as ex:
        return {'status':'UNKNOWN','reason':str(ex)[:350],'exception':type(ex).__name__}


def canonical_bag(rows):
    return Counter(json.dumps(x,sort_keys=True,default=str) for x in rows)


def check(session,df,max_rows=16):
    start=time.perf_counter()
    stage=extract(df)
    if stage['status']!='READY':
        return stage
    try:
        sides=[]
        for side in (stage['left'],stage['right']):
            source=session.table(side['view'])
            require(source.columns==side['column_names'],'SOURCE_SCHEMA_MISMATCH')
            source_rows=[list(x) for x in source.limit(max_rows+1).collect()]
            require(len(source_rows)<=max_rows,'SOURCE_TOO_LARGE')
            require(all(all(x is None or type(x) in (int,bool) for x in row)
                        for row in source_rows),'ROW_TYPE_UNSUPPORTED')
            sides.append(source_rows)
        expected=[]; evaluated=0; truth_hist={'TRUE':0,'FALSE':0,'NULL':0}
        for left,right in product(*sides):
            env=dict(zip(stage['left']['expr_ids'],left))
            env.update(zip(stage['right']['expr_ids'],right))
            value=eval_tree(stage['predicate_ast'],env)
            require(value in (True,False,None),'ROOT_PREDICATE_NOT_BOOLEAN')
            evaluated+=1
            truth_hist['NULL' if value is None else 'TRUE' if value else 'FALSE']+=1
            if value is True:
                expected.append([env[k] for k in stage['output_expr_ids']])
        actual=[list(x) for x in df.collect()]
        wrong=canonical_bag(actual)!=canonical_bag(expected)
        mismatch={'missing_from_actual':list((canonical_bag(expected)-canonical_bag(actual)).elements()),
                  'extra_in_actual':list((canonical_bag(actual)-canonical_bag(expected)).elements())}
        return {**stage,'status':'ROW_BAG_VIOLATION' if wrong else 'NO_VIOLATION_OBSERVED',
                'evaluated_pairs':evaluated,'truth_histogram':truth_hist,
                'expected_bag':expected,'actual_bag':actual,
                'counterexample':mismatch if wrong else None,
                'elapsed_ms':round((time.perf_counter()-start)*1000,2)}
    except Exception as ex:
        return {'status':'UNKNOWN','reason':str(ex)[:350], 'exception':type(ex).__name__,
                'diagnostic_scope':stage.get('obligation')}
