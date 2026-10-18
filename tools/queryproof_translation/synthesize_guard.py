"""Derive a minimally restrictive guard for a specific SQL 3VL rewrite.

The guard uses *observation partitions*, not a hard-coded safe context list.
It separately checks the rewrite's three-valued mapping on finite complete
equality/nullness classes. A guard being permitted is a bounded proof ONLY
for this known local rewrite and the encoded observation semantics.
"""
import json
from pathlib import Path

# Observational partitions over output values (T,F,U).
T=(True,False,None)
PARTITIONS={
    'ANY':(0,0,0),
    'TRUE_ONLY':(0,1,1),
    'FALSE_ONLY':(0,1,0),
    'NULL_ONLY':(0,0,1),
    'FULL_3VL':(0,1,2),
}
def partition(value,labels):
    return labels[T.index(value)]
def mapping():
    # Compute all actual (original, rewritten) 3VL outcomes for {NULL,0,1}
    from validate import original,rewritten
    return {(original(a,b),rewritten(a,b)) for a in (None,0,1) for b in (None,0,1)}
def allows(partition_name):
    p=PARTITIONS[partition_name]
    return all(partition(a,p)==partition(b,p) for a,b in mapping())
def synthesize():
    answers={name:allows(name) for name in PARTITIONS}
    # The conjunction of all requirements it satisfies is the exact set of
    # supported observation partitions, no condition names hard-coded.
    assert answers['TRUE_ONLY'] and answers['FALSE_ONLY'] is False
    assert answers['NULL_ONLY'] is False and answers['FULL_3VL'] is False
    assert answers['ANY']
    return {'rewrite':'(a=b OR both_null) -> (a IS NOT DISTINCT FROM b)',
            'precondition':'integer equality semantics, no side effects, nullable operands',
            'permitted_observation_partitions':[
                 name for name,yes in answers.items() if yes],
            'forbidden_observation_partitions':[
                 name for name,yes in answers.items() if not yes],
            'basis':'exhaustive equality/nullness classes over {NULL,0,1}',
            'not_sufficient_for':'unknown expressions, row multiplicity, aggregate cardinality, order, exceptions'}
if __name__=='__main__':
    Path('artifacts').mkdir(exist_ok=True)
    report=synthesize()
    Path('artifacts/queryproof_rewrite_guard.json').write_text(json.dumps(report,indent=2))
    print('QUERYPROOF_MINIMAL_GUARD',json.dumps(report),flush=True)
