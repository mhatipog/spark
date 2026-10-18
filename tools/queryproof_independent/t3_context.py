"""Context-sensitive equivalence for SQL three-valued Boolean optimizer rewrites.

Finite abstraction and scope:
- Compare deterministic Boolean-valued expressions of nullable integral values.
- The 0, 1, NULL domain suffices for the equality/distinctness rewrite:
  equality / inequality / a nullable operand are the only distinctions.
- General SQL functions, nondeterminism, collations and subquery effects require
  additional proof obligations: unsupported -> FULL_3VL, not optimism.
"""
from __future__ import annotations

from itertools import product

TRUTH = (True, False, None)
VALUES = (0, 1, None)
TRUE_SET = 'TRUE_SET'
FULL_3VL = 'FULL_3VL'


def tri_not(x):
    return None if x is None else not x


def tri_and(x,y):
    if x is False or y is False:
        return False
    return None if x is None or y is None else True


def tri_or(x,y):
    if x is True or y is True:
        return True
    return None if x is None or y is None else False


def original(a,b):
    eq = None if a is None or b is None else a == b
    return tri_or(eq, a is None and b is None)


def rewrite(a,b):
    # SQL IS NOT DISTINCT FROM: fully two-valued
    return a == b


def keep(x):
    return x is True


def obligation_for_child(parent,needed):
    """Conservative top-down context effect. FULL_3VL absorbs all operators.

    A root positive AND/OR observes only which inputs evaluate to TRUE.
    NOT, IS NULL, arbitrary scalar/value contexts can distinguish FALSE/NULL.
    """
    if needed == FULL_3VL:
        return FULL_3VL
    if parent in ('AND','OR','FILTER','HAVING','JOIN_ON_ROOT','CASE_WHEN'):
        return TRUE_SET
    # Include NOT, IS_NULL, IS_NOT_NULL, scalar functions and unknown ops.
    return FULL_3VL


def classify(path, root=TRUE_SET):
    """Path runs from consumer root to candidate rewrite, excluding candidate.

    E.g. ('JOIN_ON_ROOT','NOT','OR') requires FULL_3VL;
    ('JOIN_ON_ROOT','AND','OR') requires only TRUE_SET.
    """
    result=root
    for op in path:
        result=obligation_for_child(op,result)
    return result


def equivalence(left,right,required=FULL_3VL,domain=VALUES):
    """Exhaustive within the declared (0,1,NULL) scalar abstraction."""
    failures=[]
    for a,b in product(domain,repeat=2):
        x,y=left(a,b),right(a,b)
        same=(keep(x)==keep(y)) if required==TRUE_SET else (x==y)
        if not same:
            failures.append({'a':a,'b':b,'original':x,'rewrite':y})
    return {'required':required,'equivalent':not failures,'counterexamples':failures}


def report():
    return {
        'root_filter':equivalence(original,rewrite,TRUE_SET),
        'value_projection':equivalence(original,rewrite,FULL_3VL),
        'under_not':equivalence(lambda a,b:tri_not(original(a,b)),
                                lambda a,b:tri_not(rewrite(a,b)),TRUE_SET),
        'under_is_null':equivalence(lambda a,b:original(a,b) is None,
                                    lambda a,b:rewrite(a,b) is None,TRUE_SET),
        'detected_context':{
            'positive_join':classify(('JOIN_ON_ROOT','AND','OR')),
            'negative_join':classify(('JOIN_ON_ROOT','NOT','OR')),
            'nested_null_observer':classify(('FILTER','AND','IS_NULL','OR')),
            'case_when':classify(('CASE_WHEN','OR')),
        },
        'novelty_warning':'Known three-valued semantics and context awareness; not a novel mathematical theorem.'
    }


if __name__=='__main__':
    import json
    print(json.dumps(report(),indent=2))
