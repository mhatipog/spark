"""Independently replay a Catalyst finite certificate using SQLite SQL semantics.

This does not import the Python semantic evaluator's interpret() function.
The verifier translates two separately decoded Catalyst expression trees into
SQLite scalar SQL and compares results for all nine equality/NULL classes.
SQLite is a corroborating independent implementation, NOT a complete oracle
for arbitrary Spark SQL or a substitute for actual Spark replay.
"""
import json,sqlite3
from itertools import product

from context_proof import DOMAIN,parse_expression,attrs

def emit(tree,columns):
    op=tree[0]
    if op=='attr':
        if tree[1] not in columns:raise ValueError('UNBOUND_ATTRIBUTE')
        return columns[tree[1]]
    a=emit(tree[1],columns)
    if op=='Not':return '(NOT '+a+')'
    if op=='IsNull':return '('+a+' IS NULL)'
    if op=='IsNotNull':return '('+a+' IS NOT NULL)'
    b=emit(tree[2],columns)
    clauses={'And':'AND','Or':'OR','EqualTo':'=','EqualNullSafe':'IS'}
    if op not in clauses:raise ValueError('UNSUPPORTED_SQLITE_OPERATOR:'+op)
    return '('+a+' '+clauses[op]+' '+b+')'

def decode_bool(value):
    if value is None:return None
    if value in (0,1):return bool(value)
    raise ValueError('NON_BOOLEAN_SQLITE_RESULT')

def independently_verify(certificate):
    old=parse_expression(certificate['raw_expression_nodes_before'])
    new=parse_expression(certificate['raw_expression_nodes_after'])
    if set(attrs(old))!=set(attrs(new)) or len(attrs(old))!=2:
        raise ValueError('MISMATCHED_INPUT_IDENTITIES')
    identities=sorted(attrs(old))
    columns=dict(zip(identities,('a','b')))
    left,right=emit(old,columns),emit(new,columns)
    sql='SELECT '+left+', '+right+' FROM (SELECT ? AS a, ? AS b)'
    rows=[]
    local_sql='SELECT '+emit(certificate['changed_before'],columns)+', '+emit(certificate['changed_after'],columns)+' FROM (SELECT ? AS a, ? AS b)'
    locals_=[]
    with sqlite3.connect(':memory:') as con:
        for a,b in product(DOMAIN,repeat=2):
            x,y=con.execute(sql,(a,b)).fetchone()
            p,q=con.execute(local_sql,(a,b)).fetchone()
            rows.append({'values':[a,b],
                         'before':decode_bool(x),'after':decode_bool(y)})
            locals_.append((decode_bool(p),decode_bool(q)))
    # Independent SQLite truth-table derivation of every admissible observer.
    tests={'ANY':lambda p,q:True,
           'TRUE_ONLY':lambda p,q:(p is True)==(q is True),
           'FALSE_ONLY':lambda p,q:(p is False)==(q is False),
           'NULL_ONLY':lambda p,q:(p is None)==(q is None),
           'FULL_3VL':lambda p,q:p==q}
    admissible=[name for name,test in tests.items()
                if all(test(p,q) for p,q in locals_)]
    if admissible!=certificate.get('admissible_observation_partitions'):
        raise ValueError('GUARD_PARTITION_SET_NOT_INDEPENDENTLY_REPRODUCED')
    guard='ALLOW' if certificate.get('required_partition') in admissible else 'BLOCK'
    if guard!=certificate.get('synthesized_guard_decision'):
        raise ValueError('GUARD_DECISION_NOT_INDEPENDENTLY_REPRODUCED')
    false_rows=[r for r in rows if (r['before'] is True)!=(r['after'] is True)]
    verdict='COUNTEREXAMPLE' if false_rows else 'EQUIVALENT_IN_FRAGMENT'
    claimed=certificate.get('status')
    if verdict!=claimed:raise ValueError('CERTIFICATE_VERDICT_MISMATCH')
    if certificate.get('bindings_checked')!=9:
        raise ValueError('SCOPE_ENUMERATION_NOT_COMPLETE')
    counterexample=certificate.get('minimal_counterexample')
    if verdict=='COUNTEREXAMPLE':
        if counterexample is None:raise ValueError('MISSING_WITNESS')
        vars=counterexample['bindings']
        if sorted(vars)!=identities:raise ValueError('WITNESS_IDENTITY_MISMATCH')
        vals=[vars[x] for x in identities]
        if not any(x['values']==vals and
                   x['before']==counterexample['before'] and
                   x['after']==counterexample['after'] for x in rows):
            raise ValueError('WITNESS_NOT_REPRODUCED_IN_SQLITE')
    elif counterexample is not None:
        raise ValueError('SPURIOUS_WITNESS')
    return {'verified':True,'sqlite_version':sqlite3.sqlite_version,
            'assignments_checked':len(rows),
            'observable_differences':len(false_rows),
            'root_full_value_differences':sum(x['before']!=x['after'] for x in rows),
            'verdict':verdict,
            'independently_derived_safe_observers':admissible,
            'guard_decision':guard,
            'scope':'nullable integral equality fragment only',
            'query_template':'parameterized SQLite scalar projection (no Spark optimized SQL reuse)'}

