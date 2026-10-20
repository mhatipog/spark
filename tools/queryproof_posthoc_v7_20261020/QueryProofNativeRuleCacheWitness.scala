/* QueryProof native Spark 4.2.0 private RuleId memoization witness.
 *
 * Source-level basis: TreeNode._ineffectiveRules is a private mutable BitSet
 * and transformDownWithPruning consults it. This is not the TreeNodeTag map.
 * It is deliberately not visible to the QueryProof STATE or FRAME observers.
 *
 * The experiment does not claim any Spark optimizer bug: Spark's documented
 * RuleId contract assumes purely functional rules without varying external
 * state. QueryProof must make that premise explicit and source-checkable.
 */
package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation

object QueryProofNativeRuleCacheWitness {
  def main(args: Array[String]): Unit = {
    val node = OneRowRelation()
    val before = node.toJSON
    assert(node.isTagsEmpty)
    var ruleInvocations = 0
    val witnessRuleId = RuleId(0)

    val once = node.transformDownWithPruning(_ => true, witnessRuleId) {
      case n: OneRowRelation =>
        ruleInvocations += 1
        n
    }
    assert(once.asInstanceOf[AnyRef] eq node.asInstanceOf[AnyRef])
    assert(ruleInvocations == 1, "First rule invocation did not happen")
    assert(node.isTagsEmpty && node.toJSON == before)

    val twice = node.transformDownWithPruning(_ => true, witnessRuleId) {
      case n: OneRowRelation =>
        ruleInvocations += 1
        n
    }
    assert(twice.asInstanceOf[AnyRef] eq node.asInstanceOf[AnyRef])
    assert(ruleInvocations == 1,
      "The private ineffective-rule cache did not suppress the second call")
    assert(node.isTagsEmpty && node.toJSON == before)
    println("QUERYPROOF_NATIVE_UNOBSERVED_PRIVATE_RULE_CACHE_SUPPRESSES_TRAVERSAL")

    val fresh = OneRowRelation()
    fresh.transformDownWithPruning(_ => true, witnessRuleId) {
      case n: OneRowRelation =>
        ruleInvocations += 1
        n
    }
    assert(ruleInvocations == 2,
      "Fresh-node positive control should execute the same rule")
    assert(fresh.isTagsEmpty && fresh.toJSON == before)
    println("QUERYPROOF_NATIVE_RULE_CACHE_FRESH_NODE_POSITIVE_CONTROL")
  }
}
