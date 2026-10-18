"""Sound-in-scope rule-level counterexample certificates from actual Catalyst AST deltas.

Only supports unchanged INNER JOIN input/output plan with exactly ONE changed
JOIN condition, two uniquely resolved nullable integral AttributeReferences,
and the deterministic grammar {Not,And,Or,EqualTo,EqualNullSafe,IsNull,IsNotNull,
AttributeReference}. For this *two-input equality/nullness* fragment,
{NULL,0,1} represents all equal/unequal/null valuation classes. It is NOT a
complete proof over arbitrary SQL, complex expression trees or joins.

The checker does not read SQL text, historical issue labels, expected outputs,
or rule names to decide the result. It evaluates actual rule before/after ASTs.
"""
import json
import sys
from pathlib import Path
from itertools import product

# Use the pre-existing, exhaustively tested five-partition effect calculus
# rather than maintaining another hand-written approximation of operators.
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'queryproof_observer_lattice'))
from lattice import LABELS, requirement

DOMAIN=(None,0,1)
OPS={'Not':1,'And':2,'Or':2,'EqualTo':2,'EqualNullSafe':2,'IsNull':1,'IsNotNull':1}
TRUTH_PARTITIONS={'ANY':(0,0,0),'TRUE_ONLY':(0,1,1),'FALSE_ONLY':(0,1,0),
                  'NULL_ONLY':(0,0,1),'FULL_3VL':(0,1,2)}
TRUTH=(True,False,None)

class OutOfScope(Exception):
    pass

def require(ok,reason):
    if not ok:raise OutOfScope(reason)

def kind(data):
    return data.get('class','').rsplit('.',1)[-1]

def key_for_attr(node):
    require(kind(node)=='AttributeReference','NOT_ATTRIBUTE_REFERENCE')
    require(node.get('dataType') in ('integer','long','short','byte'),
            'NON_INTEGRAL_ATTRIBUTE_TYPE')
    raw=node.get('exprId')
    require(isinstance(raw,dict) and ('id' in raw), 'MISSING_EXPRESSION_ID')
    return json.dumps(raw,sort_keys=True)

def parse_expression(seq):
    require(isinstance(seq,list) and seq,'EMPTY_SERIALIZED_EXPRESSION')
    def rec(pos):
        require(pos<len(seq),'TRUNCATED_SERIALIZED_EXPRESSION')
        node=seq[pos]
        require(isinstance(node,dict),'INVALID_SERIALIZED_EXPRESSION')
        k=kind(node)
        arity=node.get('num-children')
        require(type(arity)==int and 0<=arity<=2,'UNSUPPORTED_ARITY')
        if k=='AttributeReference':
            require(arity==0,'ATTRIBUTE_HAS_CHILDREN')
            return ('attr',key_for_attr(node)),pos+1
        require(k in OPS,'UNSUPPORTED_EXPRESSION_CLASS:'+k)
        require(arity==OPS[k],'EXPRESSION_ARITY_MISMATCH:'+k)
        children=[]
        nextpos=pos+1
        for _ in range(arity):
            child,nextpos=rec(nextpos)
            children.append(child)
        return (k,*children),nextpos
    tree,end=rec(0)
    require(end==len(seq),'EXTRA_SERIALIZED_EXPRESSION_NODES')
    return tree

def attrs(tree):
    if tree[0]=='attr':return {tree[1]}
    result=set()
    for child in tree[1:]:result.update(attrs(child))
    return result

def sql_and(a,b):
    if a is False or b is False:return False
    return None if a is None or b is None else True
def sql_or(a,b):
    if a is True or b is True:return True
    return None if a is None or b is None else False

def interpret(tree,env):
    op=tree[0]
    if op=='attr':return env[tree[1]]
    left=interpret(tree[1],env)
    if op=='Not':return None if left is None else not left
    if op=='IsNull':return left is None
    if op=='IsNotNull':return left is not None
    right=interpret(tree[2],env)
    if op=='And':return sql_and(left,right)
    if op=='Or':return sql_or(left,right)
    if op=='EqualTo':return None if left is None or right is None else left==right
    if op=='EqualNullSafe':return left==right
    raise OutOfScope('UNKNOWN_OP:'+op)

def distinguished(out_a,out_b,demand):
    require(out_a in TRUTH and out_b in TRUTH,'NON_BOOLEAN_RESULT')
    if demand=='TRUE_ONLY':return (out_a is True)!=(out_b is True)
    if demand=='FALSE_ONLY':return (out_a is False)!=(out_b is False)
    if demand=='NULL_ONLY':return (out_a is None)!=(out_b is None)
    if demand=='FULL_3VL':return out_a!=out_b
    if demand=='ANY':return False
    raise OutOfScope('UNKNOWN_OBSERVER')

def changed_site(before,after):
    """Return the single changed-expression boundary and contextual ancestors."""
    if before==after:return None
    if before[0]=='attr' or after[0]=='attr':
        return [],before,after
    if before[0]!=after[0] or len(before)!=len(after):
        return [],before,after
    indices=[i for i in range(1,len(before)) if before[i]!=after[i]]
    if len(indices)!=1:return [],before,after
    child=indices[0]
    nested=changed_site(before[child],after[child])
    if nested is None:return [],before,after
    path,old,new=nested
    return [(before[0],child-1),*path],old,new

def demanded_partition(path):
    """Exact weakest local obligation via the independently tested 3VL lattice.

    Sibling Boolean inputs are universally quantified when calculating child
    equivalence. Unsupported contexts return UNKNOWN, never an unsafe guess.
    """
    operators={'Not':'NOT','And':'AND','Or':'OR',
               'IsNull':'IS_NULL','IsNotNull':'IS_NOT_NULL'}
    needed=LABELS['TRUE_ONLY']
    for op,child in path:
        require(op in operators,'OBSERVER_TRANSFER_UNSUPPORTED:'+op)
        needed=requirement(needed,operators[op],child)
    labels=[key for key,value in LABELS.items() if value==needed]
    require(len(labels)==1,'UNKNOWN_OBSERVATION_PARTITION')
    return labels[0]

def same_unmodified_plan(old,new,join_index):
    if len(old)!=len(new):return False
    for i,(a,b) in enumerate(zip(old,new)):
        if i==join_index:
            if not isinstance(a,dict) or not isinstance(b,dict):return False
            a={k:v for k,v in a.items() if k!='condition'}
            b={k:v for k,v in b.items() if k!='condition'}
        if a!=b:return False
    return True

def witness_valuations(a,b):
    identities=sorted(attrs(a)|attrs(b))
    require(len(identities)==2,'REQUIRES_EXACTLY_TWO_DISTINCT_ATTRIBUTES')
    require(attrs(a)==attrs(b),'BEFORE_AFTER_ATTRIBUTE_BINDINGS_DIFFER')
    # Equivalent classes for two integral equality operands: equal non-null,
    # unequal non-null, and all possible NULL placements.
    tested=[]
    for vals in product(DOMAIN,repeat=2):
        env=dict(zip(identities,vals))
        tested.append({'bindings':env,'before':interpret(a,env),'after':interpret(b,env)})
    return sorted(tested,key=lambda item:(
        sum(v is None for v in item['bindings'].values()),
        sum(abs(v) for v in item['bindings'].values() if v is not None),
        tuple(-1 if v is None else v for v in item['bindings'].values())
    ))

def certificate_for_transition(event):
    """Fail-closed evaluation of an actual serialized RuleExecutor event."""
    try:
        old,new=event['old'],event['new']
        require(isinstance(old,list) and isinstance(new,list),'BAD_RULE_EVENT')
        require(len(old)==len(new),'PLAN_SHAPE_CHANGED')
        changed=[i for i,(a,b) in enumerate(zip(old,new)) if a!=b]
        require(len(changed)==1,'MULTIPLE_PLAN_NODES_CHANGED')
        i=changed[0]
        a,b=old[i],new[i]
        require(kind(a)=='Join' and kind(b)=='Join','CHANGED_NODE_NOT_JOIN')
        require(same_unmodified_plan(old,new,i),'NON_CONDITION_PLAN_CHANGE')
        jt=json.dumps(a.get('joinType',{}))
        require('Inner' in jt and 'Outer' not in jt,'JOIN_TYPE_NOT_INNER')
        orig,rewritten=parse_expression(a.get('condition')),parse_expression(b.get('condition'))
        change=changed_site(orig,rewritten)
        require(change is not None,'NO_EXPRESSION_CHANGE')
        path,subold,subnew=change
        demand=demanded_partition(path)
        valuations=witness_valuations(orig,rewritten)
        differences=[x for x in valuations if distinguished(x['before'],x['after'],'TRUE_ONLY')]
        local_vals=witness_valuations(subold,subnew)
        local_diffs=[x for x in local_vals if distinguished(x['before'],x['after'],demand)]
        # Exhaustively synthesize the weakest permissible observation contract
        # for this particular real transformation, rather than guessing a
        # rule-name guard. No SQL result reference is involved.
        admissible=[part for part in TRUTH_PARTITIONS
                    if not any(distinguished(x['before'],x['after'],part)
                               for x in local_vals)]
        guard_allows=demand in admissible
        require(guard_allows==(not local_diffs),'INCONSISTENT_CONTEXT_GUARD')
        # A local distinguishing tuple must also induce a root-level observable
        # divergence to label the whole transformation COUNTEREXAMPLE.
        verdict='COUNTEREXAMPLE' if differences else 'EQUIVALENT_IN_FRAGMENT'
        return {'status':verdict,'join_node_index':i,
                'rule':event.get('rule'),'batch':event.get('batch'),
                'context_path':path,'required_partition':demand,
                'before_expression':orig,'after_expression':rewritten,
                'changed_before':subold,'changed_after':subnew,
                'bindings_checked':len(valuations),
                'root_value_divergence':sum(x['before']!=x['after'] for x in valuations),
                'root_true_set_divergence':len(differences),
                'minimal_counterexample':differences[0] if differences else None,
                'local_observable_divergences':len(local_diffs),
                'admissible_observation_partitions':admissible,
                'synthesized_guard_decision':'ALLOW' if guard_allows else 'BLOCK',
                'guard_basis':'exact finite counterexample search over all five Boolean observer partitions',
                'assumptions':['unchanged INNER JOIN operands and plan outside condition',
                    'exactly two unchanged nullable integral attribute identities',
                    'supported deterministic equality/nullness Boolean grammar',
                    'finite domain {NULL,0,1} complete for this restricted grammar'],
                'raw_expression_nodes_before':a.get('condition'),
                'raw_expression_nodes_after':b.get('condition')}
    except (OutOfScope,KeyError,TypeError,ValueError) as exc:
        return {'status':'UNKNOWN','reason':str(exc)[:300] or type(exc).__name__,
                'rule':event.get('rule')}

