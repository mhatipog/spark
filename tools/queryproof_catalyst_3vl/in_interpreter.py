"""Finite-source SQL three-valued interpreter for one uncorrelated IN subquery.

Scope is deliberately narrow: one direct source view, an analyzed Filter
containing one scalar InSubquery, one non-correlated direct single-column
source view, integral comparisons, identity root projection, max 16 rows
per view. Never calls Spark to compute an alternative IN predicate.
"""
import time
from collections import Counter

from join_interpreter import (
    kind,seq,expr_id,direct_source,require,ScopeError,
    compile_expression,eval_tree,canonical_bag,kleene_and,kleene_or,kleene_not,
)


def in_three_valued(x, candidates):
    """SQL IN including the empty set (FALSE even for NULL probe)."""
    if not candidates:
        return False
    has_unknown=False
    for y in candidates:
        eq = None if x is None or y is None else x==y
        if eq is True:
            return True
        if eq is None:
            has_unknown=True
    return None if has_unknown else False


def compile_in_tree(e, targets, depth=0):
    require(depth<20,'IN_EXPR_DEPTH')
    k=kind(e)
    if k=='InSubquery':
        require(not targets,'MULTIPLE_IN_SUBQUERIES_UNSUPPORTED')
        require(len(seq(e.values()))==1,'MULTICOLUMN_IN_UNSUPPORTED')
        query=e.query()
        require(kind(query)=='ListQuery','EXPECTED_LIST_QUERY')
        require(len(seq(query.outerAttrs()))==0,'CORRELATED_IN_UNSUPPORTED')
        require(len(seq(query.joinCond()))==0,'SUBQUERY_JOIN_CONDITION_UNSUPPORTED')
        subplan=query.plan()
        require(kind(subplan)=='Project','IN_SUBQUERY_PROJECT_REQUIRED')
        named=seq(subplan.projectList())
        require(len(named)==1 and kind(named[0])=='AttributeReference',
                'IN_SUBQUERY_DERIVED_PROJECTION')
        source=direct_source(subplan.child())
        attr_id=expr_id(named[0])
        require(attr_id in source['expr_ids'],'IN_SOURCE_ATTR_MISMATCH')
        at=source['expr_ids'].index(attr_id)
        require(str(named[0].dataType().typeName()) in ('integer','long'),
                'IN_SOURCE_KEY_NONINTEGER')
        targets.append({'source':source,'source_column_index':at})
        value=seq(e.values())[0]
        require(kind(value)=='AttributeReference','IN_PROBE_NOT_ATTRIBUTE')
        require(str(value.dataType().typeName()) in ('integer','long'),
                'IN_PROBE_NONINTEGER')
        return {'op':'in_query','args':[{'op':'attr','id':expr_id(value)}],
                'target_index':len(targets)-1}
    children=seq(e.children(),16)
    unary={'Not':'not','IsNull':'is_null','IsNotNull':'is_not_null'}
    binary={'And':'and','Or':'or'}
    if k in unary:
        require(len(children)==1,'IN_UNARY_ARITY')
        return {'op':unary[k],'args':[compile_in_tree(children[0],targets,depth+1)]}
    if k in binary:
        require(len(children)==2,'IN_BINARY_ARITY')
        return {'op':binary[k],'args':[compile_in_tree(v,targets,depth+1) for v in children]}
    return compile_expression(e,depth)


def evaluate_in_tree(tree,environment,pools):
    op=tree['op']
    if op=='in_query':
        x=evaluate_in_tree(tree['args'][0],environment,pools)
        return in_three_valued(x,pools[tree['target_index']])
    if op=='attr':
        require(tree['id'] in environment,'IN_UNBOUND_ATTRIBUTE')
        return environment[tree['id']]
    if op=='literal':
        return tree['value']
    if op in ('not','is_null','is_not_null'):
        a=evaluate_in_tree(tree['args'][0],environment,pools)
        if op=='not':return kleene_not(a)
        return (a is None) if op=='is_null' else (a is not None)
    if op in ('and','or'):
        a,b=(evaluate_in_tree(v,environment,pools) for v in tree['args'])
        return kleene_and(a,b) if op=='and' else kleene_or(a,b)
    return eval_tree(tree,environment)


def extract(df):
    try:
        node=df._jdf.queryExecution().analyzed()
        if kind(node)=='Sort':
            require(bool(getattr(node,'global')()),'IN_LOCAL_SORT_UNSUPPORTED')
            node=node.child()
        require(kind(node)=='Project','IN_ROOT_NOT_PROJECT')
        outputs=seq(node.projectList())
        require(outputs and all(kind(v)=='AttributeReference' for v in outputs),
                'IN_PROJECTION_NOT_IDENTITY')
        filtering=node.child()
        require(kind(filtering)=='Filter','IN_FILTER_REQUIRED')
        source=direct_source(filtering.child())
        bound=set(source['expr_ids'])
        out_ids=[expr_id(x) for x in outputs]
        require(set(out_ids).issubset(bound),'IN_OUT_NOT_SOURCE_ATTR')
        targets=[]
        tree=compile_in_tree(filtering.condition(),targets)
        require(len(targets)==1,'IN_SUBQUERY_COUNT_NOT_ONE')
        def used(t):
            if t['op']=='attr':return {t['id']}
            return set().union(*(used(q) for q in t.get('args',[])))
        require(used(tree).issubset(bound),'IN_PREDICATE_NOT_SOURCE_BOUND')
        return {'status':'READY','outer':source,'inner':targets[0],
                'predicate_ast':tree,'output_expr_ids':out_ids,
                'obligation':'UNCORRELATED_SCALAR_IN_FILTER_3VL_ROW_BAG',
                'source_assumption':'immutable raw views scanned independently'}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':str(exc)[:350],'exception':type(exc).__name__}


def check(session,df,max_rows=16):
    start=time.perf_counter()
    stage=extract(df)
    if stage['status']!='READY':return stage
    try:
        outer=stage['outer']
        inner=stage['inner']
        ss=session.table(outer['view'])
        tt=session.table(inner['source']['view'])
        require(ss.columns==outer['column_names'],'IN_OUTER_VIEW_SCHEMA_CHANGED')
        require(tt.columns==inner['source']['column_names'],'IN_INNER_VIEW_SCHEMA_CHANGED')
        rows=[list(v) for v in ss.limit(max_rows+1).collect()]
        inner_rows=[list(v) for v in tt.limit(max_rows+1).collect()]
        require(len(rows)<=max_rows and len(inner_rows)<=max_rows,'IN_SOURCE_TOO_LARGE')
        require(all(all(v is None or type(v) is int for v in row) for row in rows+inner_rows),
                'IN_SOURCE_ROW_UNSUPPORTED')
        pool=[v[inner['source_column_index']] for v in inner_rows]
        expected=[]; hist={'TRUE':0,'FALSE':0,'NULL':0}
        for row in rows:
            env=dict(zip(outer['expr_ids'],row))
            match=evaluate_in_tree(stage['predicate_ast'],env,[pool])
            require(match in (True,False,None),'IN_RESULT_NOT_BOOLEAN')
            hist['NULL' if match is None else 'TRUE' if match else 'FALSE']+=1
            if match is True:
                expected.append([env[x] for x in stage['output_expr_ids']])
        actual=[list(r) for r in df.collect()]
        ea,aa=canonical_bag(expected),canonical_bag(actual)
        wrong=ea!=aa
        return {**stage,'status':'ROW_BAG_VIOLATION' if wrong else 'NO_VIOLATION_OBSERVED',
                'expected_bag':expected,'actual_bag':actual,
                'truth_histogram':hist,'inner_row_count':len(inner_rows),
                'outer_row_count':len(rows),
                'counterexample':{'missing':list((ea-aa).elements()),
                                  'extra':list((aa-ea).elements())} if wrong else None,
                'elapsed_ms':round((time.perf_counter()-start)*1000,2)}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':str(exc)[:300],'exception':type(exc).__name__}
