"""Spark-independent unit tests for conservative plan scope and expression IDs."""
import unittest
from plan_extractor import extract


class JSeq:
    def __init__(self, values): self.values=values
    def size(self): return len(self.values)
    def apply(self,i): return self.values[i]


class JClass:
    def __init__(self, s):self.s=s
    def getSimpleName(self):return self.s


class Node:
    def __init__(self,kind,**kwargs): self.kind=kind;self.kwargs=kwargs
    def getClass(self):return JClass(self.kind)
    def __getattr__(self,k):
        if k in self.kwargs:
            return lambda:self.kwargs[k]
        raise AttributeError(k)
    def children(self):return JSeq(self.kwargs.get('children',[]))


def attr(eid, name, kind='integer'):
    return Node('AttributeReference',exprId=Node('ExprId',id=eid),
                name=name,dataType=Node('DataType',typeName=kind))


def order(a,d='ASC',n='NULLS FIRST'):
    return Node('SortOrder', child=a,direction=Node('Direction',sql=d),
                nullOrdering=Node('Nulls',sql=n))


def qe(root):
    return Node('QueryExecution',analyzed=root)


def plan(refs,sorts,global_sort=True,wrappers=()):
    p=Node('Sort',order=JSeq(sorts),output=JSeq(refs),children=[],
           **{'global':global_sort})
    for wrapper in wrappers:
        p=Node(wrapper,**{'plan' if wrapper=='WithCTE' else 'child':p,
                          'output':JSeq(refs),'children':[p]})
    return qe(p)


class ExtractionTests(unittest.TestCase):
    def test_uses_identity_not_name(self):
        a,b=attr(1,'id'),attr(2,'id')
        result=extract(plan([a,b],[order(b)]))
        self.assertEqual(result['keys'],[{'index':1,'direction':'ASC','nulls':'FIRST'}])

    def test_root_unwrap_with_order_directions(self):
        a,b=attr(1,'first'),attr(2,'second')
        result=extract(plan([a,b],[order(b,'DESC','NULLS LAST'),order(a)],
                            wrappers=['GlobalLimit','WithCTE']))
        self.assertEqual(result['verdict'],'EXPOSED_ORDER_CONTRACT')
        self.assertEqual(result['keys'],[{'index':1,'direction':'DESC','nulls':'LAST'},
                                         {'index':0,'direction':'ASC','nulls':'FIRST'}])

    def test_hidden_attribute_never_guessed(self):
        self.assertEqual(extract(plan([attr(1,'x')],[order(attr(2,'x'))]))['reason'],
                         'SORT_KEY_NOT_EXPOSED')

    def test_non_global_sort_is_unknown(self):
        self.assertEqual(extract(plan([attr(1,'x')],[order(attr(1,'x'))],False))['reason'],
                         'PARTITION_LOCAL_SORT_ONLY')

    def test_unsupported_or_derived_sort_is_unknown(self):
        a=attr(1,'a',kind='double')
        self.assertEqual(extract(plan([a],[order(a)]))['reason'],'UNSUPPORTED_KEY_TYPE:double')
        b=attr(1,'x')
        self.assertEqual(extract(plan([b],[order(Node('Add'))]))['reason'],
                         'DERIVED_OR_NON_ATTRIBUTE_SORT_KEY')

    def test_project_above_sort_is_unknown(self):
        a=attr(1,'a')
        result=extract(qe(Node('Project',output=JSeq([a]),
                                  children=[Node('Sort',order=JSeq([order(a)]),
                                                 output=JSeq([a]),children=[],
                                                 **{'global':True})])))
        self.assertEqual(result['reason'],'NO_ROOT_GLOBAL_SORT')


if __name__=='__main__':unittest.main()
