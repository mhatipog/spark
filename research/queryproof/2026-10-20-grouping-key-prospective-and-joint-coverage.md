# QueryProof research checkpoint: a second prospective Catalyst certificate

Date: 2026-10-20 UTC
Repository: mhatipog/spark (personal public fork; research branch only)
Branch: research/queryproof-generalized-liveness-20261020
Status: local research, **not** an Apache Spark upstream PR or a claimed Spark bug.

## Verified result

The previously frozen projection-liveness programme established 60/60 named
ColumnPruning certificates on 60 targeted generated cases, 516 actual changed
Spark Catalyst transitions, and zero Spark-versus-SQLite row-bag mismatches.
That previous result remains unchanged.

This round established a **second, independent local proof family** for
RemoveRepetitionFromGroupExpressions: delete repeated pure grouping keys without
changing the input bag, the aggregate outputs, or any optional enclosing Sort.

The decisive prospective first execution of untouched H01-H48 was successful:

| Metric | Observed |
| --- | ---: |
| Previously unexecuted cases | 48/48 executed |
| Real changed Catalyst events (all phases) | 438 |
| Named RemoveRepetitionFromGroupExpressions events | 42 |
| New local certificates | 42/42 |
| UNKNOWN within this named family | 0 |
| Spark–SQLite mismatching result bags | 0 |
| SQL errors / trace decode / instrumentation failures | 0 |
| Checker Git blob | 092bb0ecdf8e459a0c1cd3c712959a9d243a191f |
| Scoring driver Git blob | 06428852dc1cc0e3e8c546a537b2775dd47db66b |
| Both pinned source blobs matched | Yes |

The GitHub Action succeeded:
https://github.com/mhatipog/spark/actions/runs/39381307021

The manifest, FROZEN_PROTOCOL, VERIFIER_PIN, and first-run entrypoint are in
tools/queryproof_grouping_repetition_round2_20261020/. The validator is in
tools/queryproof_context_validator/grouping_repetition_certificate.py.
The Python proof tests passed 33 adversarial and positive cases, plus 36
existing generalized projection-liveness tests:
https://github.com/mhatipog/spark/actions/runs/39381047911

The new H cohort is *design-informed and correlated*, generated from eight
SQL families by six new modulus/fixture settings, seeded separately from
earlier SQL. It is prospective with respect to **unexecuted instances and
fixtures**, not independent authorship, random real-world SQL, or unseen
optimization concepts.

## Mathematical claim and its exact boundary

For a fixed finite input row bag R, let G be a nonempty list of deterministic
total grouping functions. The grouping partition depends on the equivalence
relation r ~_G s iff every g in G returns equal SQL grouping-key values on
r and s. Duplicating a function adds the same equality test twice:

    (g(r)=g(s)) AND (g(r)=g(s))  iff  g(r)=g(s).

Thus deleting repeated functions while retaining all distinct functions leaves
the equivalence classes and their row-bag multiplicities unchanged. Identical
pure aggregate outputs over identical groups therefore give the same output
row bag. The reasoning includes duplicate rows, nullable input values,
NULL grouping keys, empty input, and optional identical Sort frames when
observation is unordered result bags.

**Nonempty grouping is essential.** A constant GROUP BY over empty input
returns no groups. Replacing its *last* grouping expression with an empty list
can instead produce a global aggregate row, e.g. COUNT(*)=0. Our repetition
checker does not accept that deletion. This is also why Spark's
RemoveLiteralFromGroupExpressions retains a dummy literal key when all original
grouping expressions are foldable (SPARK-17114 comment in Optimizer.scala).

The checker does not merely trust a rule name. It parses entire changed-plan
preorders, requires identical aggregate output metadata and input subtree,
checks attributes against resolved ExprId/type/name scopes, validates strict
de-duplication of exact canonical grouping ASTs, and whitelists deterministic
total grouping expressions (direct resolved attributes, typed literals, and
bounded integer remainder with nonzero literal divisor). Aggregate output
operators include a small validated grammar with COUNT/SUM/AVG and narrowly
admitted MAX/MIN of atomic inputs. Volatile or unrecognized expressions,
invalid schemas, changed contexts, an empty resulting grouping list, and
untrusted metadata abstain.

This is a theorem in a restricted, fixed-input **logical bag semantics**.
It is not a general theorem about all Catalyst operators, external functions,
physical ordering, resource usage, memory errors, or Spark's whole execution
chain.

## Transparent development and failed first freeze

Do not erase this history:

1. On the older already-inspected 60-case dataset, the new certificate
   eventually covered 12/12 repetition events; retrospective only.
2. We preregistered a first new G01-G48 cohort. Its first execution found
   42 named events and 18 accepted certificates; 48/48 result bags matched,
   but a verification workflow check failed due a miswired provenance field.
   **That run is not claimed as fully validated prospective success.**
3. Subsequent *development-only* replays of G cases identified Spark's
   qualified AttributeReference serialization and the aggregate MAX class.
   The corrected checker accepted 42/42 G events, but this is **retrospective**.
4. We left the original G CASES and FROZEN_PROTOCOL intact and froze a
   separate H01-H48 suite *before* evaluating it against the corrected pinned
   checker and driver. H then yielded the 42/42 green prospective result.

G first execution (validation check failure):
https://github.com/mhatipog/spark/actions/runs/39380486577
G posthoc 42/42 diagnostic:
https://github.com/mhatipog/spark/actions/runs/39381094148

## Retrospective coverage union on the same H cases

We replayed the already-seen H corpus using all three existing local families.
Each is counted only on its respective real named-rule events.

| Rule | Certified | Actual named events |
| --- | ---: | ---: |
| RemoveRepetitionFromGroupExpressions | 42 | 42 |
| ColumnPruning (generalized projection liveness) | 36 | 42 |
| CollapseProject (aggregate identity certificate) | 12 | 30 |
| InferFiltersFromConstraints | 0 | 6 |
| **Named individual-rule union** | **90** | **120** |

These named individual-rule events exclude 270 Analyzer wrappers and 48
Optimizer wrappers; total trace events were 438. The historic original
five-checker frozen union (which does not include the new families) remains
UNKNOWN on this cohort. The 90/120 figure is **posthoc** and conditioned on
purposefully enriched SQL, not a whole-chain or optimizer-wide success rate.

Combined run:
https://github.com/mhatipog/spark/actions/runs/39381559530

Co-occurrence in the same query does **not** demonstrate end-to-end semantic
composition. Adjacency and plan-state continuity still need separate proof.

## Scientific significance, calibrated

This is a reproducible, useful validation pipeline for real Spark Catalyst
transitions. It has moved from hypothetical algebra and unit examples to
multiple named-rule certificates, actual instrumented Spark optimizer events,
independent row-bag execution oracles, positive/adversarial tests, explicit
UNKNOWN counts, and immutable first-run scorer identities. That engineering
combination is useful for optimizer assurance and contributor regression
research.

The individual grouping-idempotence identity is elementary and should not be
presented as a novel mathematical discovery. There is substantial prior work
on query-equivalence proving, including U-semiring/UDP, SQLSolver, and
bounded VeriEQL. The research distinction worth developing is *compositional,
locally auditable translation validation integrated with Spark Catalyst*.

Primary related literature:
- Chu et al., U-semiring, PVLDB 2018:
  https://www.vldb.org/pvldb/vol11/p1482-chu.pdf
- Ding et al., SQLSolver, SIGMOD 2024:
  https://www.cs.yale.edu/homes/piskac/papers/2024SIGMOD.pdf
- VeriEQL, bounded SQL equivalence:
  https://doi.org/10.1145/3649849

## Priority next research

1. **Chain composition** (highest impact): within a private raw-trace
   temporary directory, identify exact consecutive transitions where
   next.old equals previous.new. Require independently accepted local
   certificates on every edge and preserve execution/context metadata.
   Report maximal proof-connected segments, their lengths and fully
   connected query counts. Never concatenate isolated successful events
   across unobserved Analyzer/Optimizer jumps or changed plan states.
2. **RemoveLiteralFromGroupExpressions**: prove the constant-key removal
   rule, explicitly retaining one dummy literal in all-literal groups.
   Stress empty inputs, singleton versus multiple keys, NULL, and changed
   aggregate results. Collect real AST shapes retrospectively first;
   freeze a fresh external or seeded cohort only after checker stabilization.
3. **ColumnPruning + MAX**: the unchanged frozen generalized liveness
   checker abstained on six H-case MAX contexts. If broadening, create a
   versioned successor checker and new untouched cases; do not edit the
   previously frozen certificate and silently repurpose its holdout.
4. **Independent coverage**: acquire authored third-party SQL cases or
   realistic workload fragments with predeclared selection rules. Avoid
   claiming design-targeted 100% success generalizes to production.
5. **Independent reviewer / formalization**: expose a small proof obligation
   format for external verification and compare against SQLSolver/VeriEQL
   on a supported overlapping fragment; assess false-accept risks and
   operator/dialect semantic assumptions.

Do not mutate or merge unrelated upstream Apache Spark PRs based on these
research results. The current work remains on Metin's personal research fork.

## Later round: actual same-executor proof-chain composition

We went beyond mere co-occurrence. A new fail-closed connectivity checker
at tools/queryproof_composition_20261020/chain_connectivity.py requires:

- Two or more *consecutive captured* changed-rule events in the same case.
- Each event independently accepted by its existing immutable local checker.
- Complete structural equality of the preceding new AST and the following
  old AST (including ExprIds and node metadata).
- The same actual Catalyst RuleExecutor on both edges.
- No intervening UNKNOWN event in that executor-local observed sequence.
- It admits transitions between different *sequential batches* of the same
  executor but does not jump between executors or assume an unrecorded rewrite.

For a chain P0 -> P1 -> ... -> Pk satisfying those obligations, each local
checker establishes observational equivalence Pi ~ Pi+1 as row bags. Exact
AST identity makes the intermediate relation the same object of discourse,
so transitivity yields P0 ~ Pk for that recorded subchain. This is a sound
compositional theorem in the same restricted pure logical fragment.

The actual H01-H48 retrospective instrumented run found:

| Measure | Result |
| --- | ---: |
| Recorded changed events | 438 |
| Independently locally certified | 90 |
| Exact consecutive certified pairs, same executor | 42 |
| Within one optimizer batch | 12 |
| Between two sequential batches of the same executor | 30 |
| Across distinct executors | 0 |
| Cases containing a verified >=2-edge chain | 36/48 |
| Cases containing a verified 3-edge chain | 6/48 |
| Maximum verified chain length | 3 |
| Fully certified optimizer/query traces | **0** |
| Adversarial chain-connectivity unit tests | 15 passed |

The six three-edge chains (H13-H18) follow the exact sequence:
RemoveRepetitionFromGroupExpressions -> ColumnPruning -> CollapseProject.
The two-step links across batches run from the same executor's Aggregate
batch to its Operator Optimization before Inferring Filters batch.
The same-batch links connect ColumnPruning to CollapseProject.

We explicitly investigated a stricter same-executor/**same-batch** rule:
it retained 12 two-edge chains and discarded the 30 cross-batch links.
A separate sanitized provenance audit showed that *all* 30 excluded links
were between sequential batches of the **same executor**, and none crossed
executors; allowing those sequential boundaries is therefore justified
for exact contiguous snapshots. No raw Catalyst ASTs are published.

Green strict-scope-chain run:
https://github.com/mhatipog/spark/actions/runs/39382685038

**Do not call this full-query validation.** All 48 query traces still contain
unproved Analyzer/Optimizer wrappers and potentially unobserved hierarchical
changes, so the first and last plans of a whole query are not connected by
one fully proved path. The key next advance is a hierarchical translation
validator that can show an outer Executor/Optimizer summary edge is exactly
the composition of all nested, explicitly accounted-for rule transitions,
with gaps never silently omitted. After that, address the remaining literal,
filter-inference, and aggregate-class certificates.

This experiment is retrospective on H. The separate 42/42 H grouping
certificate remains prospectively pinned and unchanged. No modifications
to its checker, scorer, manifest, or protocol were made for chain diagnostics.
