# QueryProof — independent ColumnPruning liveness confirmation (2026-10-20)

## Result

**Prospective confirmation succeeded:** the previously frozen 15-query DuckDB
aggregate/grouping cohort executed completely on official PySpark 4.2.0 and
SQLite, with **5 exact liveness certificates among 8 actual named
`ColumnPruning` transitions**. There were 116 captured changed Catalyst
events overall, zero Spark-versus-SQLite row-bag disagreements, zero query
errors, and zero AST decoding/instrumentation failures.

This is a narrow, independently sourced proof-family confirmation. It is **not**
a claim of 62.5% optimizer-wide coverage, proof that the entire Spark query was
optimized correctly, a newly discovered Spark defect, or a general-purpose
aggregate-equivalence theorem.

## Provenance and chronological status

- Source: `duckdb/duckdb` at
  `0b272c23b19eb7747313fffa0a1097f720905b29`, 15 previously unused
  original SELECT blocks with exact source rows and schemas.
- Frozen input: `tools/queryproof_external_duckdb_column_liveness_20261019/CASES.json`
  and `FROZEN_PROTOCOL.json`; no SQL text or fixture edited after freezing.
- Verifier freeze commit: `aca0ff232c4ce735f7e0d5ff0c23cea5d1625e32`.
- Frozen and executed checker Git blob:
  `55deae0fcf52b366ce50821f3cf7bf48b97424a7`; **exact match**,
  as checked by `git hash-object` in the runner.
- First attempted workflow at commit `d27db45f64e303bfc9fbef78a80bd1ca1ee2676b`,
  [run 39318794786](https://github.com/mhatipog/spark/actions/runs/39318794786),
  failed *before any cohort SQL ran*. Its sparse checkout omitted the entire
  new column-liveness runner directory; it therefore yielded no scientific
  holdout result.
- One-line infrastructure repair at
  [commit f936ebafee7cce5178207f683a6c33dd22ffbeac](https://github.com/mhatipog/spark/commit/f936ebafee7cce5178207f683a6c33dd22ffbeac)
  added that directory only to the external job's sparse-checkout list.
  Neither checker nor frozen protocol/manifest changed.
- First executed prospective cohort:
  [run 39373028730](https://github.com/mhatipog/spark/actions/runs/39373028730),
  all three jobs **success** (exact unit tests, frozen replay, external
  cross-vendor replay); published logs and sanitized artifacts.

## Frozen primary and secondary outcomes

| Observation | Result |
| --- | ---: |
| Independent SQL statements executed | 15/15 |
| Total changed Catalyst trace events | 116 |
| Named `ColumnPruning` events | 8 |
| `EQUIVALENT_IN_FRAGMENT` ColumnPruning liveness certificates | **5** |
| ColumnPruning UNKNOWN / abstentions | 3 |
| Spark–SQLite query-result row-bag mismatches | 0 |
| Query execution errors | 0 |
| Trace decode and instrumentation failures | 0 |
| Frozen verifier blob identity | Exact match |

Accepted certificate events:

| SQL case | Projected columns kept | Dropped | Empty projection | Unchanged Sort frames |
| --- | ---: | ---: | --- | ---: |
| A02 | 0 | 1 | Yes | 0 |
| A04 | 0 | 1 | Yes | 0 |
| A07 | 0 | 1 | Yes | 0 |
| A08 | 1 | 1 | No | 1 |
| A09 | 1 | 1 | No | 1 |

The three named-rule abstentions were reported as:

- `UNKNOWN_EXPRESSION_CLASS_OR_ARITY`: **2**
- `NOT_EXACT_INSERTED_PRUNING_PROJECT`: **1**

The historical five-checker union remains `UNKNOWN` on all 116 events;
the new ColumnPruning liveness theorem is evaluated in a **separate** field
rather than silently rewriting historical union results.

## Why the proof is sound inside its fragment

The checker compares actual resolved Catalyst preorder ASTs and admits only
an unchanged Aggregate, optionally under identical Sort ancestors, when
the optimizer inserts one direct subset Project below that Aggregate over an
unchanged permitted input subtree. Every attribute consumed by the unchanged
aggregate/grouping expressions must remain available in the projected subset,
and expression classes, metadata, identities, and tree positions must pass
strict checks. The original input bag and its projected version have the
same row multiplicity, even when the projection contains *zero* columns.
Thus the unchanged aggregate receives exactly the same multiset of
values for all expressions it actually reads, establishing equal output row
bags under the explicitly restricted pure logical semantics.

No claim is made for arbitrary Catalyst expressions, arbitrary aggregate
functions, reordered results, runtime exceptions, physical performance,
or all other optimizer stages.

## Interpretation and next falsifiable step

This 5/8 targeted score is considerably more persuasive than the earlier
**retrospective** 5/6 score on the already-inspected 18-query DuckDB grouping
cohort. It confirms genuine extrapolation to previously unused, independently
authored SQL. Nevertheless, eight rule events selected from one particular
grouping/aggregate-oriented vendor corpus are far too few and nonrandom to
estimate production Spark proof coverage.

The highest-value next round is **abstention-driven development with fresh
confirmation**:

1. Diagnose the two unsupported expression classes and the one nonmatching
   Project shape **post hoc**, with sanitized structural diagnostics. Do not
   reclassify the 15-query cohort as prospective after modifying the theorem.
2. Formulate a reusable, small liveness/congruence lemma grounded in exact
   input/output schemas, expression-ID scope, purity and bag multiplicities.
   Test failure cases such as dropped live keys, changed group functions,
   alias collisions, volatile expressions, outer joins, and Sort/order claims.
3. Freeze a **different untouched external SQL cohort** before executing
   any changed checker. Include normal, NULL, duplicate, zero-column and
   negative-control cases. Report all events and UNKNOWNs.
4. Separately pursue certification of complete optimization chains and
   comparison with existing SQL-equivalence tools (SQLSolver, Cosette,
   Rulescript), because isolated local proof coverage is not itself a
   sufficiently distinct publication claim.

The 15-case result may be used for research diagnosis, never counted again
as a fresh holdout for a checker changed in response to it.

## Subsequent post-hoc abstention anatomy (not additional holdout credit)

After preserving the first successful prospective score, a **workflow-only**
diagnostic replay at
[run 39373501726](https://github.com/mhatipog/spark/actions/runs/39373501726)
printed pre-existing sanitized shape diagnostics. All three jobs passed.
It did not modify the checker, input SQL, frozen protocol or the already
recorded 5/8 prospective result.

The three UNKNOWN events are now identified:

| Case | Captured structural difference | Why the frozen checker abstained |
| --- | --- | --- |
| A05 | `Aggregate` with `Alias(Upper(Cast(Literal)))`, new zero-column Project under Aggregate | `Upper` not in the strict expression grammar |
| A06 | `Aggregate` with `Alias(Lower(Cast(Literal)))`, new zero-column Project under Aggregate | `Lower` not in the strict expression grammar |
| A10 | `Sort(Aggregate(Project(... Remainder ...)))` becomes `Sort(Aggregate(Project(..., Project(... Remainder ...))))` | Actual Project-stack restructuring, not one inserted direct Project |

The first two are narrow syntax limitations, not evidence of unsafe
transformations. The third demands a different structural theorem; it is
not safe to declare equivalence merely because `ColumnPruning` was the rule
name or because the visible aggregate was unchanged.

### Generalizable next lemma: observational projection liveness

Let `R` be any finite bag; let `F(R)` and `G(R)` be deterministic,
total, one-output-row-per-input projections of the **same** unchanged base
relation, with explicit resolved output identities. Let `K` be the exact
set of downstream live attributes read by a whitelisted, unchanged,
deterministic relation context `C` (including its grouping/aggregate
expressions, but only bag-observed `Sort` frames).

A sufficient condition for

`C(F(R)) ≡bag C(G(R))`

is that, for **every possible input row**, the values exposed by `F`
and `G` at all identities in `K` agree, and both preserve one row for
each input occurrence. Columns outside `K` may be computed, retained,
removed or rearranged but cannot influence `C`. The proof follows by
pairing each input occurrence through the two projections; the resulting
bags agree after restriction to `K`, and the unchanged pure context
cannot distinguish them.

This subsumes the simple inserted Project case and could cover nested
project stacks such as A10. However, implementing it soundly requires:

1. Exact compositional output-identity / expression normalization across
   Project stacks, with explicit avoidance of alias collisions, duplicate
   IDs and unresolved scopes.
2. A conservative data-dependency/purity grammar; effects, side queries,
   runtime errors, volatile expressions and unrecognized references abstain.
3. A whitelist of contexts with a written bag-congruence lemma, and an
   explicit statement that Sort does not certify ordered result sequences.
4. Counterexample-oriented mutations and a **new untouched independent
   SQL cohort** frozen before any new checker receives prospective credit.

This is a proof-design hypothesis, **not a newly implemented or validated
certificate**. It should be evaluated for coverage and complexity against
smaller, case-specific extensions before code is written.
