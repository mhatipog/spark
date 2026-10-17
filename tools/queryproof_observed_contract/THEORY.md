# QueryProof: two composable semantic contracts for adaptive SQL optimizers

## 1. Problem: row bags and row sequences are different

An optimizer is allowed to change the physical order of an intermediate relation unless an order-sensitive operator depends on it. For a result ordered by key `K`, *observational correctness* requires more than equivalence of multisets.

Let `R=(r_1,...,r_n)`, with an explicit SQL total preorder `<=_K`. For the subset of queries where `K` can be independently and unambiguously read from output rows, the necessary observable-order contract is:

`C_order(R,K) := AND_{i=1}^{n-1} [K(r_i) <=_K K(r_(i+1))]`.

By transitivity, these adjacent checks are equivalent to monotonicity across *all* pairs in the observed sequence. One violating pair is a sound, concrete contract counterexample; successful checks say **nothing** about whether the correct rows were selected, their values, or non-exposed ORDER BY expressions. Our initial implementation needs O(n) comparisons and O(1) extra checker state, not counting collection of query results.

## 2. Selection needs a different invariant

For a fully specified total key `K` including tie-breaking identities, write `T_q(R)` for the first `q` elements of the globally sorted *multiset* `R`. The key bounded-summary identity is:

`T_q(A multiset_union B) = T_q(T_q(A) multiset_union T_q(B))`.

*Proof sketch.* If an element does not survive the local top-`q` of its own partition, there are already `q` local elements ranked ahead of it. It therefore cannot belong to the top-`q` of the global union. Consequently, keeping `q` candidates per partition is sufficient for the global top-`q`. The same argument proves associativity of the bounded merge `A ⊗_q B := T_q(A multiset_union B)`. (It is **not** idempotent on multisets with duplicates.)

For `OFFSET m LIMIT k`, the source-stage witness is `T_(m+k)(R)[m:m+k]`. If output rows include uniquely identifiable original row identities, a mismatch with the independently obtained witness proves incorrect LIMIT/OFFSET *membership*, even if the returned rows are internally sorted.

Scope restrictions matter: with unresolved ties at the selection boundary, lack of unique row identities, or no access to the pre-transformation source relation, the witness must return UNKNOWN, not invent a canonical SQL tie order. Our experimental PR #59445 fixture has six distinct ages and unique names, so this ambiguity does not arise.

## 3. Why Catalyst/AQE errors are fundamentally about non-commutation

Write `S_k` for an order-sensitive prefix-selection operator and `F` for an optimizer transformation that may materialize a join or change row emission order. Even if `F` preserves a bag of outer row identities, it need not preserve the sequence or its first `k` elements:

`bag(F(S_k(R))) != bag(S_k(F(R)))` in general.

In functional-operator language, `F` and `S_k` do **not commute**. Pushing a harmless deterministic, position-preserving projection through a limit can be safe. Pushing a correlated subquery projection that is decorrelated into a join through a limit can be unsafe because the join reorders rows *before* the prefix is chosen. This is the essential distinction in PR #59445.

The SPARK-59126 adaptive Sort loss exposes the other failure mode: the selected row bag may stay correct, but the final plan no longer enforces the declared result sequence. The contracts are complementary, not interchangeable.

## 4. What this research does and does not add

The mathematics of sortedness and compositional top-k is standard. **Do not claim novelty for these identities.** The potentially useful research question is whether one can compile *trusted semantic obligations* from the analyzed plan into small witnesses that survive Catalyst rewrites and adaptive physical-plan replacements:

- obligation `ORDER_BY(K)`: physical output-order metadata plus an optional observed adjacent-row witness;
- obligation `SLICE(K,m,k,identity)`: a bounded source-stage `Top_(m+k)` certificate plus an output membership witness;
- otherwise: UNKNOWN, with a reason.

A future translation validator would attach provenance for where each obligation was introduced, transformed, discharged, or lost, and would separate unsafe plan metadata from observed incorrect output. It must be evaluated against unseen bug families, overhead, false accusations, other database testing oracles and preserved negative controls before any scientific publication claim.

## 5. Experimental provenance

- Frozen cross-family observed-order benchmark: [manifest at fc8508d](https://github.com/mhatipog/spark/commit/fc8508de575986defbd2c83d1ddd4a23d669fcbc).
- Both official Spark 4.0.1 and 4.2.0 observed-order checks: [Actions 39059963109](https://github.com/mhatipog/spark/actions/runs/39059963109).
- Source-stage `Top_q` prototype tested using independent Spark input `p` versus the known correlated-projection LIMIT/OFFSET case in PR #59445: [Actions 39060511480](https://github.com/mhatipog/spark/actions/runs/39060511480).
- Algebra validation: four stdlib tests, including 75 deterministic randomized partition layouts × six top-k sizes; no claim that a traditional TopK heap or this property is original research.
