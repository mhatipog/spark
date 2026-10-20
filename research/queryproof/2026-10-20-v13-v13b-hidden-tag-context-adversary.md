# QueryProof V13 and V13B: real ConstantFolding hidden-tag control-flow adversary

Date: 2026-10-20. Branch: research/queryproof-phase-local-independent-20261020.
Research owner: Metin Hatipogullari. This is POSTHOC and does not alter
W32/X frozen baselines or reclassify their strict hidden-state failures.

## Motivation and authoritative source

Apache Spark v4.2.0 official source:
- [ConstantFolding in expressions.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala).
  Its failed-to-evaluate tag is created in a conditional branch upon
  failed evaluation, and the match arm checking the tag appears BEFORE
  the foldable-expression tryFold arm. tryFold catches NonFatal only
  when isConditionalBranch=true.
- [TreeNode.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala).
  Nodes also store _ineffectiveRules, a distinct mutable BitSet used
  by transformWithPruning to skip previously ineffective rules.
  Its source comment assumes immutable structure and rules without
  varying external state. V11/V12 does not observe this BitSet.

## V13 — preserve the *failed* first run without editing it away

Commit 52a7611be5ada79c3ab50b017a1db506742366e6,
[CI 39476441071](https://github.com/mhatipog/spark/actions/runs/39476441071),
job 118813494366: **failed** as an important control.

- The native witness obtained a real FAILED_TO_EVALUATE tag from
  production ConstantFolding on a conditional ANSI bad Cast.
- Reused the exact Cast in a test-constructed unconditional Project.
  With the tag, this execution successfully returned the plan.
- The witness then unset the tag on that same already-optimized plan
  and expected an exception in its second optimization.
- **Contrary to the hypothesis, no exception occurred.** Assertion
  at V13 source line 89 failed. The full CI log preserves this.
- Likely confound, supported by Spark TreeNode source: the *same plan*
  may have cached a rule-ineffective bit on its first optimization,
  preventing traversal after the tag was cleared. This is an inference
  from the source and experiment, NOT direct measurement of the bit.

The failed run is NOT a result supporting the advertised V13
counterexample. The inference required a fresh, independent test.

## V13B — corrected counterexample with cache confound controlled

Initial V13B implementation SHA
0006129d333d9ca50f8109bb23c987544e2c8a19.
**First native CI run [39476960456](https://github.com/mhatipog/spark/actions/runs/39476960456),
job 118815297561: SUCCESS**, source-pinned official Spark 4.2.0,
Scala 2.13.18, Java 17. Both source patches compile in disposable CI.

- Run production ConstantFolding on a conditional bad ANSI Cast,
  observing its real tag write on the original Cast.
- Construct a new unconditional Project containing that same tagged
  Cast, and a second new unconditional Project containing a fresh
  untagged Cast of identical constructor parameters.
- Give both Alias nodes the same ExprId and verify their exact
  Spark TreeNode.toJSON strings match. Neither unconditional Project
  has been optimized before its individual test.
- Run a new RuleExecutor instance for the tagged and another fresh
  instance for the untagged Project, respectively.
- Tagged plan optimizer returned normally. Untagged plan optimizer
  threw a NonFatal exception (from an unconditional ANSI invalid
  constant Cast). JSON still matched.
- Output markers:
  QUERYPROOF_V13B_TAG_CREATED_BY_REAL_CONSTANTFOLDING
  QUERYPROOF_V13B_FRESH_ISOMORPHIC_SERIALIZED_AST
  QUERYPROOF_V13B_TAGGED_FRESH_AST_OPTIMIZED_SUCCESSFULLY
  QUERYPROOF_V13B_UNTAGGED_FRESH_AST_THREW_DURING_OPTIMIZATION
  QUERYPROOF_V13B_SAME_AST_DIFFERENT_RULE_OUTCOME_AFTER_CACHE_CONTROL
  QUERYPROOF_V13B_TEST_ONLY_RELOCATION_NOT_SPARK_WRONG_ANSWER

## Interpretation and limitations

This is a **genuine source-level counterexample** to the statement
that identical structural Spark AST serializations imply identical
optimizer execution behavior. Relevant tag state changes whether
ConstantFolding attempts evaluation. The legal tag origin is real
Spark code; **moving its expression between contexts is explicitly
test-constructed**, not evidence a production Spark optimizer pass
actually does this. This does NOT establish different SQL row bags,
a production wrong-answer bug, or a soundness failure for the actual
W32 certified 16. Exceptions at planning and execution are distinct
from bag-of-rows observational semantics.

Independent fresh plans and executors eliminated the specific
reused-tree cache confound from V13. To prove the exact reason V13
failed, future native work should observe _ineffectiveRules mutation
and traversal skip on the reused tree, under pinned source hooks.

Combined with V12, this strengthens the requirement that any
structural certificate must either verify context-sensitive
hidden-state noninterference or fail closed. Structural JSON and
checksum integrity cannot discharge this obligation.

## Additive code

- tools/queryproof_posthoc_v13_20261020/QueryProofNativeV13ContextShiftWitness.scala
- .github/workflows/queryproof-native-v13-hidden-tag-context.yml
- tools/queryproof_posthoc_v13b_20261020/QueryProofNativeV13BFreshContextWitness.scala
- .github/workflows/queryproof-native-v13b-fresh-cache-control.yml

## Highest priority next

1. Add source-pinned **rule-ineffective BitSet** instrumentation to the
   same evidence model as tag state, or conservatively tag all such
   effects UNKNOWN pending a justified noninterference theorem.
2. Mechanize context-indexed rule effect laws (conditional vs ordinary
   expression, tag presence, ANSI mode, failure propagation),
   ideally as state-indexed relational/bag semantics in Coq.
3. Test whether any existing production Spark transformations can
   relocate previously tagged expressions into a context where this
   distinction affects observable results. No such case is known.
4. Compare against original X strict 0/16; no retrospective success
   redefinition. Benchmark matched query classes against SQLSolver
   and VeriEQL and publish all UNKNOWN outcomes.

The original W32 prospective first run was CI 39435756545: 16/32
structural suffix candidates, all 16 specified targets and 32/32
Spark/SQLite finite row-bag agreements; original strict X replay
39436715938 failed 16/16. Both remain unchanged.
