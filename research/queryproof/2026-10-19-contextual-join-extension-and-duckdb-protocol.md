# QueryProof Round 6: contextual INNER JOIN guards and cross-vendor prospective test

**Date:** 2026-10-19. **Research branch:** `research/queryproof-certificate-boundary-hardening-20261019`.

## Theorem (bag semantics)

Let `L` and `R` be unchanged resolved row-multisets with disjoint attribute
identities, `C` an INNER JOIN condition and `G_L`, `G_R` deterministic
side-scoped predicates in the integer equality/NULL three-valued fragment.
When the exact truth-orbit checker proves

`C=TRUE => G_L=TRUE and G_R=TRUE`

for every NULL/equality orbit, then

`JoinInner(C,L,R) ≡ JoinInner(C, Filter(G_L,L), Filter(G_R,R))`

as row multisets, including multiplicities. For every matching row pair,
an inserted guard can remove neither input row; for every nonmatching pair,
nothing is produced either way. Conversely, the engine cannot add pairs
by prefiltering. True-set implication is certified over complete Bell(n+1)
nullable-integral orbits and independently checked with SQLite.

If a frame is an **identical fixed Project** whose output expressions are
only direct `AttributeReference`, direct `Alias(AttributeReference)`, or
`Alias(Literal)`, that frame is a deterministic, total, row-local function.
Pushing two equal multisets through the same function preserves
multiplicities. An unchanged `Sort` frame likewise preserves *bag* equality
but is **not** an order-preservation certificate.

The implementation requires exact unchanged join metadata, INNER join type,
leaf output ExprId scoping, side-local guard references, and no modifications
outside inserted single Filter wrappers. It refuses arbitrary `Coalesce`,
casts, nondeterministic projections, outer joins, unknown leaves, stale IDs,
and changed frames. Neither sampling nor optimizer rule names decide acceptance.

Code: `tools/queryproof_context_validator/contextual_join_guard_certificate.py`.

## Retrospective discovery (cannot count as prospective!)

On the previously frozen PostgreSQL-derived 18-query sample, the original
**five** checkers certified **0/91** events (0/27 named-rule events).
Exploratory application of this new theorem **after seeing that sample**
certifies **5 events**, all `InferFiltersFromConstraints` in cases
`X06-X10`. Each proof checks five Bell(3) NULL/equality orbits and has an
independent SQLite valuation confirmation.

This is a meaningful mathematical extension, **not a prospective validation**.
The published original 0/91 figure is preserved.

## New prospectively frozen, independent-vendor sample

The new source is <https://github.com/duckdb/duckdb/tree/0b272c23b19eb7747313fffa0a1097f720905b29/test/sql/join/inner>:

- `test_join.test`: query ordinals 1,2,4–9 (8 cases).
- `test_using_join.test`: query ordinals 1–8 (8 cases).
- `test_inner_join_filter_pushdown.test`: query ordinals 1–2 (2 cases).

Total **18 original DuckDB-authored SELECT statements**, copied without
rewriting. The exact CREATE/INSERT fixture data from these files is recreated
as integer-typed `test`, `test2`, `t1`, `t2`, `left_table`, and
`right_table` in both official released Spark 4.2.0 and SQLite.

Before execution we exclude one DuckDB correlated scalar expression without
a FROM clause and two VALUES relation column-list alias queries for
cross-dialect portability. The exclusion rule and exact SQL are pinned in
`tools/queryproof_external_duckdb_join_20261019/CASES.json`.

Primary outcome: number of *actual optimizer rewrite events* certified by the
new contextual guard theorem, including zero. Also report complete captured
event count, 5-checker original union coverage, stage-labelled versus
named-rule event denominators, independent Spark/SQLite result-bag agreement,
error cases, trace failures, and abstentions. **Never tune SQL or checker after
the first execution and still call this cohort prospective.**

Limitations: source test cases are independently authored but join-focused,
not representative production queries; test fixtures are finite and lack
NULLs, so the mathematical NULL proof remains from exact orbits and
adversarial unit controls. No upstream Spark PR is proposed here.
