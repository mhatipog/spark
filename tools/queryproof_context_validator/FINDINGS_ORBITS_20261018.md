# QueryProof: exact equality-orbit contextual certificates

Date: 18 October 2026. Exploratory mathematics and implementation, **not** a blind holdout experiment or a new optimizer-bug report.

## Scope and experiment

The earlier automatic Catalyst bridge on `research/queryproof-contextual-translation-validation-20261018` recovered actual RuleExecutor before/after `INNER JOIN` conditions and checked a two-operand 3VL null-equality rewrite with independent SQLite and Z3. Its limitation was the small fragment and two fixed attributes.

This branch adds `equality_orbits.py`, which accepts the same real serialized Catalyst event shape, with no rule-name or expected-answer whitelist, but can certify predicates involving up to seven distinct integral attributes, conditional on unchanged INNER JOIN operands and strictly checked expression types. Unsupported transformations return `UNKNOWN`.

### Exact finite completeness theorem (fragment only)

Consider formulas generated from n independent, nullable integral variables using `=`, `<=>`, `IS NULL`, `IS NOT NULL`, `NOT`, `AND`, and `OR`. There are no literals, arithmetic, casts, collation, or effectful operations. Each formula is invariant under any permutation of the non-NULL integer domain because every primitive inspects only equality or nullness. This is preserved by structural induction through the three-valued Boolean connectives.

Two assignments are therefore indistinguishable to every such formula if they induce the same partition of the n variables **together with one distinguished NULL element**. Every partition has a canonical realization using the values `NULL, 0, ..., n-1`. The number of distinct representatives is exactly the Bell number `B_(n+1)`.

Consequently, observing one before/after difference on any such representative is a sound local counterexample, while observing no differences over *all* representatives establishes equivalence for **all integral values within this grammar**. This result is an application of standard equality symmetry reduction, **not** a claim of a newly invented theorem.

| Distinct attributes n | Canonical equality/NULL orbits B(n+1) | Cartesian assignments (n+1)^n |
| ---: | ---: | ---: |
| 2 | 5 | 9 |
| 3 | 15 | 64 |
| 4 | 52 | 625 |
| 5 | 203 | 7,776 |
| 6 | 877 | 117,649 |
| 7 | 4,140 | 2,097,152 |

### Exploratory results

Five standard-library test methods passed locally, covering exact orbit counts, observation transfers, positive and negated null-safe equality rewrites at n=2/3/4/6, fail-closed unsupported AST mutations, and a three-variable algebraic control. Every supported test was replayed independently as parameterized SQLite expressions over all canonical representatives. No PySpark JVM/RuleExecutor rerun or CI run was performed for **this** new branch, and no claimed actual new Catalyst transition is reported here.

The three-variable control is especially interesting:

```
P = (a=b) AND (b=c)
Q = (a=b) AND (a=c)
```

For a positive INNER JOIN predicate, P and Q have identical TRUE-sets for every nullable integral assignment. Both require all three operands to be non-NULL and equal to produce TRUE. However, the **local** replacement `b=c → a=c` is not truth-set equivalent without conditioning on the sibling `a=b`. A conservative context-insensitive observation transfer therefore says BLOCK, while the full-context proof establishes equivalence. This is marked `semantic_context_masking=true`.

By contrast `NOT P` and `NOT Q` are distinguishable; one canonical witness is `a=NULL, b=0, c=1`, yielding `NOT P=TRUE` and `NOT Q=NULL`. The independent SQLite replay corroborated the verdict. This family was synthesized as a mathematical control, **not** extracted from a previously unseen Spark rule.

### Key methodological correction

The five SQL Boolean observation partitions express what a consumer distinguishes about the *value*. A sibling predicate can also constrain the **inputs** on which that value matters. Pulling back only a value partition while universally quantifying siblings is therefore a useful sufficient guard but not a necessary condition for whole-context equivalence. This program retains both the conservative local guard and the exact root condition verdict, rather than treating a conservative BLOCK as evidence of an optimizer defect.

### What remains unproved

- A local expression witness may violate invariants of the real input relations. It is not automatically a reachable SQL database witness or a Spark wrong-result defect.
- An INNER JOIN transformation's result must be checked under the precise unchanged-child assumption; altered joins, projections, aggregates, physical ordering, runtime errors, implicit coercions and effectful functions are outside scope.
- No broad observed performance numbers were measured, only exact enumeration counts and fast local synthetic test completion.
- This is an exploratory synthetic evaluation, not a preregistered prospective unseen-rule benchmark or a comparison against independent SQLancer/DQP/JoinEquiv systems.

### Next falsifiable test

1. Freeze a corpus of real, untouched RuleExecutor transitions from rule families beyond the historical `OptimizeJoinCondition` and `RewritePredicateSubquery` cases, selected without outcomes.
2. Classify all transitions with the unchanged parser; publish UNKNOWN denominators and why each abstained.
3. For root counterexamples, realize their canonical valuations as physical child tables and run before/after semantics in Spark with independent source contracts; label unreachable witnesses separately.
4. Extend to contextual path constraints using congruence closure or Z3 for correlated sibling equalities. Measure extra legitimate transformations admitted beyond value-partition-only guards and false-accusation rate.
5. Compare under equal budgets with appropriate SQL and translation-validation baselines before claiming scientific novelty or publishability.

Artifacts: `FROZEN_ORBIT_PROTOCOL.json`, `equality_orbits.py`, `test_equality_orbits.py`. The base branch, Spark master, and upstream PR #59445 remain untouched.
