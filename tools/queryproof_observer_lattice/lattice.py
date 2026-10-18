"""Minimal observational equivalence demands for SQL's three truth values.

Observation is a partition of D={T,F,U}. For a parent f and output
partition P, child values x,y are indistinguishable iff f(x,s) and f(y,s)
land in the same P-block for EVERY independent sibling assignment s.

This is the coarsest sound input partition under arbitrary sibling
valuations (a standard partition pullback / abstract interpretation result).
It is not original mathematics nor a general SQL correctness proof.
"""
from __future__ import annotations
from itertools import product

D=('T','F','U')


def generate_partitions():
    partitions=[]
    for a in range(1):
        for b in range(2):
            for c in range(max(a,b)+2):
                code=(a,b,c)
                # restricted-growth encoding: first occurrence contiguous
                if all(code[i] <= max(code[:i],default=-1)+1 for i in range(3)):
                    partitions.append(code)
    return tuple(sorted(set(partitions),key=lambda p:(len(set(p)),p)))


PARTITIONS=generate_partitions()
LABELS={
    'ANY': (0,0,0),
    'TRUE_ONLY': (0,1,1),
    'FALSE_ONLY': (0,1,0),
    'NULL_ONLY': (0,0,1),
    'FULL_3VL': (0,1,2),
}
assert len(PARTITIONS)==5
assert set(PARTITIONS)==set(LABELS.values()),(PARTITIONS,LABELS)


def normalize(labels):
    remap={}
    result=[]
    for label in labels:
        if label not in remap:
            remap[label]=len(remap)
        result.append(remap[label])
    return tuple(result)


def index(value):
    return D.index(value)


def block(partition,value):
    return partition[index(value)]


def sql_not(a):
    return 'F' if a=='T' else 'T' if a=='F' else 'U'


def sql_and(a,b):
    if 'F' in (a,b):return 'F'
    return 'U' if 'U' in (a,b) else 'T'


def sql_or(a,b):
    if 'T' in (a,b):return 'T'
    return 'U' if 'U' in (a,b) else 'F'


def sql_is_null(a):
    return 'T' if a=='U' else 'F'


def sql_is_not_null(a):
    return 'F' if a=='U' else 'T'


def sql_is_true(a):
    return 'T' if a=='T' else 'F'


def sql_is_false(a):
    return 'T' if a=='F' else 'F'


def sql_equal_null_safe(a,b):
    return 'T' if a==b else 'F'


OPERATORS={
    'NOT':(1,sql_not),
    'AND':(2,sql_and),
    'OR':(2,sql_or),
    'IS_NULL':(1,sql_is_null),
    'IS_NOT_NULL':(1,sql_is_not_null),
    'IS_TRUE':(1,sql_is_true),
    'IS_FALSE':(1,sql_is_false),
    'EQUAL_NULL_SAFE':(2,sql_equal_null_safe),
}


def apply(operator,values):
    arity,fn=OPERATORS[operator]
    if len(values)!=arity:raise ValueError('arity mismatch')
    return fn(*values)


def siblings(operator,child):
    arity,_=OPERATORS[operator]
    if child not in range(arity):
        raise ValueError('invalid child')
    return list(product(D,repeat=arity-1))


def insert_at(operator,child,x,other):
    arity,_=OPERATORS[operator]
    it=iter(other)
    return [x if i==child else next(it) for i in range(arity)]


def requirement(partition,operator,child=0):
    """Exact coarsest observation partition on specified child position."""
    signatures=[]
    for x in D:
        signatures.append(tuple(
            block(partition,apply(operator,insert_at(operator,child,x,rest)))
            for rest in siblings(operator,child)))
    return normalize(signatures)


def follow_path(path,observer=LABELS['TRUE_ONLY']):
    """Path from root observer down to candidate (operator,child-index)."""
    needed=observer
    for op,child in path:
        needed=requirement(needed,op,child)
    return needed


def equivalent_map(f,g,partition):
    """Finite unary-map contextual equivalence over the 3-valued input."""
    return all(block(partition,f[x])==block(partition,g[x]) for x in D)


def enumerate_substitution_power():
    functions=[dict(zip(D,c)) for c in product(D,repeat=3)]
    counts={}
    for label,p in LABELS.items():
        pairs=sum(equivalent_map(a,b,p) for a in functions for b in functions)
        counts[label]={'safe_pairs_of_729':pairs,
                       'full_value_safe_pairs':27,
                       'additional_vs_full':pairs-27}
    return counts


def signature_witnesses(partition,operator,child):
    """Witness for each distinguished child-pair; empty for equivalent."""
    result=[]
    demand=requirement(partition,operator,child)
    for a,b in product(D,repeat=2):
        examples=[list(x) for x in siblings(operator,child)
                  if block(partition,apply(operator,insert_at(operator,child,a,x))) !=
                  block(partition,apply(operator,insert_at(operator,child,b,x)))]
        result.append({'a':a,'b':b,'same_required_block':
                       block(demand,a)==block(demand,b),'witnesses':examples})
    return result


def report():
    return {
       'domain':list(D),'number_of_partitions':len(PARTITIONS),
       'partitions':LABELS,
       'examples':{
           'NOT_under_TRUE_ONLY':follow_path([('NOT',0)]),
           'IS_NULL_under_TRUE_ONLY':follow_path([('IS_NULL',0)]),
           'AND_under_TRUE_ONLY':follow_path([('AND',0)]),
           'OR_under_TRUE_ONLY':follow_path([('OR',0)]),
           'NOT_then_OR':follow_path([('NOT',0),('OR',0)]),
           'AND_then_NOT':follow_path([('AND',0),('NOT',0)]),
       },
       'enumerated_unary_rewrite_pairs':enumerate_substitution_power(),
       'interpretation':'Coarsest sound local demand under independent siblings; not a general optimizer correctness proof.'
    }


if __name__=='__main__':
    import json
    print('QUERYPROOF_OBSERVER_LATTICE',json.dumps(report(),sort_keys=True))
