"""Independent source/shape controls for contextual proof certificates."""
import unittest
from context_proof import certificate_for_transition,parse_expression,OutOfScope

def attr(name,i):
    return {'class':'org.apache.spark.sql.catalyst.expressions.AttributeReference',
            'num-children':0,'name':name,'dataType':'integer',
            'exprId':{'id':i,'jvmId':'synthetic'}}

def exp(kind,n,**extras):
    return {'class':'org.apache.spark.sql.catalyst.expressions.'+kind,
            'num-children':n,**extras}

def before(negate):
    expr=[exp('Or',2),exp('EqualTo',2),attr('a',1),attr('x',2),
          exp('And',2),exp('IsNull',1),attr('a',1),exp('IsNull',1),attr('x',2)]
    return [exp('Not',1),*expr] if negate else expr

def after(negate):
    expr=[exp('EqualNullSafe',2),attr('a',1),attr('x',2)]
    return [exp('Not',1),*expr] if negate else expr

def event(negative):
    old={'class':'org.apache.spark.sql.catalyst.plans.logical.Join','num-children':2,
         'joinType':{'object':'org.apache.spark.sql.catalyst.plans.Inner$'},
         'condition':before(negative)}
    new={**old,'condition':after(negative)}
    tail=[{'class':'Table','num-children':0},{'class':'Table','num-children':0}]
    return {'old':[old,*tail],'new':[new,*tail],'rule':'candidate-rule'}

class ContextProofTests(unittest.TestCase):
    def test_bad_nested_rewrite_distinguished_with_certificate(self):
        c=certificate_for_transition(event(True))
        self.assertEqual(c['status'],'COUNTEREXAMPLE')
        self.assertEqual(c['required_partition'],'FALSE_ONLY')
        self.assertEqual(c['bindings_checked'],9)
        self.assertGreater(c['root_true_set_divergence'],0)
        w=c['minimal_counterexample']
        self.assertIsNone(w['before'])
        self.assertIs(w['after'],True)
        self.assertEqual(sum(v is None for v in w['bindings'].values()),1)

    def test_same_rewrite_positive_filter_is_sound_in_fragment(self):
        c=certificate_for_transition(event(False))
        self.assertEqual(c['status'],'EQUIVALENT_IN_FRAGMENT')
        self.assertEqual(c['required_partition'],'TRUE_ONLY')
        self.assertEqual(c['root_true_set_divergence'],0)
        self.assertGreater(c['root_value_divergence'],0)

    def test_attribute_rename_does_not_change_id(self):
        v=event(True)
        v['new'][0]['condition'][2]['name']='renamed_a'
        self.assertEqual(certificate_for_transition(v)['status'],'COUNTEREXAMPLE')

    def test_mismatched_expression_identity_is_unknown(self):
        v=event(True)
        v['new'][0]['condition'][2]['exprId']['id']=3
        self.assertEqual(certificate_for_transition(v)['status'],'UNKNOWN')

    def test_plan_changes_outside_join_are_unknown(self):
        v=event(True)
        v['new'][1]={'class':'DifferentTable','num-children':0}
        self.assertEqual(certificate_for_transition(v)['status'],'UNKNOWN')

    def test_non_inner_join_unknown(self):
        v=event(True)
        v['new'][0]['joinType']=v['old'][0]['joinType']={'object':'LeftOuter'}
        self.assertEqual(certificate_for_transition(v)['status'],'UNKNOWN')

    def test_unsupported_expression_fails_closed(self):
        v=event(True)
        v['new'][0]['condition'][1]['class']='org.example.UnsafeExpression'
        self.assertEqual(certificate_for_transition(v)['status'],'UNKNOWN')

    def test_unresolved_attributes_fail_closed(self):
        v=event(True)
        v['old'][0]['condition'][3]['class']='org.foo.UnresolvedAttribute'
        self.assertEqual(certificate_for_transition(v)['status'],'UNKNOWN')


if __name__=='__main__':
    unittest.main()
