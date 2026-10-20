# QueryProof: complete Catalyst batch proofs and post-normalization optimizer suffixes

20 October 2026 | personal fork only | Spark 4.2.0 | research checkpoint

## Decisive change in proof granularity

Previous proof objects certified some isolated changed rules and up to three
contiguous named-rule transformations. They were not enough to certify a batch
or a complete optimizer invocation.

This round adds **ephemeral BEGIN/END plan snapshots** for the actual
`RuleExecutor.execute` and its serial batches, compiled only in disposable CI.
The pinned source tracer, frozen grouping checker, previously executed H SQL,
and frozen SQL scorer have not been changed.

Every scope marker records a unique nested scope ID, executor identity, batch
identity, case, and exact Catalyst JSON plan, preserved only while the scoring
run executes. The scoreboard contains no raw AST or embedded literal.

The ledger rejects missing/end-mismatched markers, interleaved cases, changed
rules outside a matching batch, noncontiguous plans, uncertified changes,
and changed endpoints without a complete path. Cross-executor composition is
never inferred from equal-looking snapshots. All unrecognized rules abstain.

A batch with entry P, exit Q is accepted only if (a) P and Q are exactly
identical, or (b) every changed rule inside the batch has an independently
accepted pure row-bag certificate, the first old AST equals P, every adjacent
new/old AST matches exactly, and the last new AST equals Q. Unchanged rules
are inert under the captured RuleExecutor's `fastEquals` semantics, and a
potentially uncaptured actual difference would fail the endpoint check.
The final result is a translation-validation claim about the *recorded*
execution, not about every possible Spark plan.

An executor is accepted only if the exact entry/exit plans are identical or
every enclosed batch is accepted with exact boundary continuity. Nested
executors are not quietly treated as independent proofs of an enclosing
rule's return value. Failure closes the current proof scope.

## H01-H48 retrospective findings

Auditable GitHub Actions:
- Complete boundary ledger: https://github.com/mhatipog/spark/actions/runs/39387705863
- Named-rule and AST shape diagnosis: https://github.com/mhatipog/spark/actions/runs/39388056597
- Post-normalization suffix experiment: https://github.com/mhatipog/spark/actions/runs/39388308491

| Measure | Result |
| --- | ---: |
| SQL cases executed with Spark and SQLite reference | 48/48 |
| Spark/SQLite rowbag mismatches and trace errors | 0 |
| Recorded changed named-rule transitions | 438 |
| Explicit scope markers (begin + end) | 5,856 |
| Complete batch scopes, including unchanged | 2,832 |
| Full executor scopes | 96 |
| Changed batch scopes | 186 |
| **Certified complete changed batch scopes** | **66** |
| Changed complete executors certified | **0 of 96** |
| **Certified optimizer suffixes after Finish Analysis** | **30 of 48** |
| Uncertified optimizer suffixes | 18 of 48 |

Batch-level positive proofs:
- 42/42 changed `Aggregate` batches, enclosing certified repetition
  eliminations.
- 24/42 changed `Operator Optimization before Inferring Filters` batches
  certified, using complete local liveness/identity proof chains.
- 2,646 unchanged batches recognized as **structural identities**. Do not
  inflate the nontrivial 66 by including these.

All 48 Analyzer `Resolution` batches are UNKNOWN (270 changed Analyzer
rule records). All 48 `Finish Analysis` optimizer batches are UNKNOWN (48
changed composite Optimizer rule records). Six `Infer Filters` batches are
UNKNOWN (six `InferFiltersFromConstraints` records). The remaining
operator-optimization gaps comprise 18 `CollapseProject` and six
`ColumnPruning` records. There were **no counterexamples** to the
restricted proof rules; UNKNOWN is an abstention.

AST-class-only diagnostics identified what Finish Analysis changes in this
cohort: 48 `View` and 48 `SubqueryAlias` nodes removed, with 30 `Project`
nodes introduced. Spark source shows FinishAnalysis is a composite of many
normalization operations, including time and expression rewrites; simply
declaring that wrapper an equational identity would be unsound.

## Why the 30 certified optimizer suffixes matter

Spark's Analyzer resolves initially undefined names and relations; these
intermediate plans are not necessarily meaningful executable relations.
FinishAnalysis performs correctness-relevant normalization beyond pure
equational optimization. We therefore establish a **normalization cut** at
the exact output of the Finish Analysis batch, and ask whether every
subsequent optimizer batch reaches the exact returned `Optimizer.execute`
plan through certified changes or exact identities.

On previously executed H SQL, **30/48 suffixes** satisfy that complete
condition. These cases are H01-H06, H13-H30, and H43-H48. H07-H12 and
H31-H42 abstain because later batches still contain unproved rules.
This is the first whole post-normalization optimizer *suffix* result in the
programme, not a whole-query, Analyzer, FinishAnalysis, or physical-plan
proof. It is retrospective: the cut/ledger was designed after inspecting H.

The semantic relation is unordered row-bag equivalence for the narrow
pure deterministic total expressions admitted by the existing checkers.
No result implies order equivalence, handling of arbitrary volatile SQL,
general aggregation, unsafe casts, timezone semantics, side effects, or
Catalyst versions other than the tested Spark 4.2.0 snapshot.

## Reproducibility and prospective validation

Scope tooling:
- `tools/queryproof_scopes_20261020/instrument_scopes.py`: pinned-copy
  instrumentation (the repository's original RuleExecutor remains untouched).
- `tools/queryproof_scopes_20261020/scope_ledger.py`: fail-closed
  hierarchical validator.
- `tools/queryproof_scopes_20261020/test_scope_ledger.py`: 20 adversarial
  scope/endpoint controls.
- `tools/queryproof_scopes_20261020/run_scopes.py`: H retrospective run.

The new K01-K48 data and SQL are frozen separately under
`tools/queryproof_scopes_fresh_k_20261020/`; the K workflow validates git
blob pins for the scope validator, instrumentation generator, adversarial
tests, frozen legacy checker, frozen scorer, and two auxiliary certificates
*before* executing Spark. SQL modifies several H-derived families: COUNT of
nullable attributes, MIN, extra syntactically duplicate grouping keys, new
moduli, and new seeded NULL/duplicate datasets. K is **not** third-party
SQL and remains strongly grammar-correlated with H.

Prospective K first run:
**PASS on the first K execution**:
https://github.com/mhatipog/spark/actions/runs/39388855109

The frozen new K01-K48 cohort completed 48/48 cases with no Spark/SQLite
rowbag mismatches, worker errors, malformed scopes or trace gaps; all pinned
module git blob hashes matched before execution. It recorded 432 actual
changed-rule events and 2,832 complete batch scope boundaries, with 180
changed batch scopes, **60 complete certified changed batches**, and again
**30 of 48 complete certified post-FinishAnalysis optimizer suffixes**.
The remaining 18 optimizer suffixes stayed UNKNOWN; zero whole Analyzer or
Optimizer executor executions were certified.

This repeats the H suffix count but does not supply a statistically
independent SQL distribution: K reused eight target-enriched grammar families
with new nullable aggregate arguments, MIN, repeated grouping keys and seeded
data. Report this as **prospective verifier-frozen replication on
family-correlated generated tests**, not as a 62.5% general-purpose coverage
estimate. Do not modify pinned H or K checkers or retroactively promote any
repaired outcome to prospective status.

## Highest-leverage next research

1. Preserve the first-run K replication, its 18 UNKNOWN suffixes and all
   pin hashes. The fact that H and K share grammar families limits the
   generalization despite matched 30/48 suffix results.
2. Repair the actual 18 suffix blockers only on a new **versioned** checker,
   beginning with strict pure projection composition/identity for the
   `CollapseProject` cases. Six `ColumnPruning` MAX-type cases and
   six filter-inference events require independent treatment. Create a fresh
   L corpus after any checker changes; never rescore K as new prospective.
3. Formalize normalization cuts and the relational guarantee, then test
   third-party SQL workloads selected independently of the generated grammar.
   Consider mutation/adversarial falsification (wrong ExprIds, changed NULL
   semantics, nondeterministic terms, missing scope IDs) before publication.
4. Keep this research on the personal fork. Neither these observations nor
   rowbag agreements establish an upstream Spark defect or justify changing
   a separate Spark pull request.
