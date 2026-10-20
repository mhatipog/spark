# QueryProof Round N — development of COUNT/MAX/MIN identity and liveness certificates

**Date:** 2026-10-20  
**Owner:** Metin Hatipogullari  
**Branch:** `research/queryproof-adversarial-tag-frame-20261020`  
**Status:** POSTHOC DEVELOPMENT; no new independent holdout credit and no upstream PR.

## Non-negotiable provenance

The 48-case prospective M cohort and its checker baseline remain **frozen and unchanged**. Its first-run result is still **20/48 complete conditional optimizer suffixes**, with target-enriched 16/24, generalization-challenge 4/24, and zero finite cross-engine row-bag mismatches. See `2026-10-20-prospective-m-first-run.md` and first M CI run 39417193656.

Round N copied *four previously observed failures* from M into a distinct, explicitly labeled development fixture:

| Original M case | Query family | Original changed optimizer suffix | Round N dev v2 |
|---|---|---|---|
| M17 | MOD_REPEAT_MAX | UNKNOWN | COMPLETE_CERTIFIED_CHAIN |
| M21 | NESTED_PROJECT_REPEAT | UNKNOWN | COMPLETE_CERTIFIED_CHAIN |
| M29 | INNER_JOIN_REPEAT | UNKNOWN | UNKNOWN |
| M33 | LEFT_JOIN_REPEAT | UNKNOWN | UNKNOWN |

The subset is stored in `tools/queryproof_round_n_dev_20261020/CASES.json` and a separate `PROTOCOL.json`. It is *not* a new prospective sample. It contains SQL and synthetic integer/NULL/duplicate data already visible to M.

## Falsification-oriented root-cause triage

First correct replay: GitHub Actions **39418935928**, Spark **4.2.0**, Java 17. The four cases executed, with **39 changed Catalyst rule events**, no missing/errors, and **zero finite Spark/SQLite result-bag differences**. The first attempt 39418625935 completed execution but a literal-backslash/tab bug meant its *new diagnostics* were empty; fix was made before using them as evidence.

Specific *recorded JVM rule transitions*:

- **M17**: `ColumnPruning` takes `Sort(Aggregate(Project(LogicalRDD)))` to `Sort(Aggregate(Project(Project(LogicalRDD))))`; original generalized liveness checker gives `UNKNOWN_AGGREGATE_EXPRESSION` for direct integer `MAX`. Subsequent `CollapseProject` removes the extra identity project, but original aggregate-framed identity checker only supports `SUM` and returns `UNTRUSTED_AGGREGATE_FUNCTION`.
- **M21**: `ColumnPruning` is **already certified** for the project/filter stack. The subsequent identity `CollapseProject` on `COUNT` is the sole blocked transition and likewise returns `UNTRUSTED_AGGREGATE_FUNCTION`.
- **M29 / M33**: `ColumnPruning` inserts projects above and on *both inputs of a join*. The fixed-base invariant fails with `PROJECT_BASE_CHANGED`. Their suffixes also contain unsupported predicate/filter-rewrite events. Do not infer join-proof coverage from the two aggregate successes.

The Scala source-pinned optimizers are the released [Apache Spark 4.2.0 Optimizer.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala): ColumnPruning explicitly has multiple cases for nested Project, Aggregate, Filter and child-attribute pruning. The existing verifier covered only a subset of those cases.

## Local theorem and new checker boundaries

Round N versions were **copied to separate development files**; frozen M/H/K/L checkers were not edited:

- `tools/queryproof_round_n_dev_20261020/aggregate_identity_v2.py` extends exact removal of `Project(identity output, T)` under **unchanged** Aggregate and optional Sort contexts. The aggregate interpreter conservatively recognizes direct **integral SUM, COUNT, MIN, MAX** arguments and Count-of-literal. Unknown aggregate function classes, changed frame nodes, filtered aggregates, unequal child trees, missing/misordered identity columns, mismatched resolved attribute IDs or output types abstain.
- `tools/queryproof_round_n_dev_20261020/generalized_liveness_v2.py` reuses the already-written strict direct integer extrema scanner in the frozen grouping-repetition certificate, so a project's *symbolic live outputs* can be compared for an otherwise unchanged Aggregate with MAX/MIN.
- `tools/queryproof_round_n_dev_20261020/scope_ledger_v2.py` is the separate chain scorer wired only to these development checkers.

The identity law, under the stated **defined** logical bag semantics, is: for a resolved row sequence T and a proven exact identity Project, `C(Project(identity,T))` and `C(T)` give the same result row bag when C is the checked unchanged pure Aggregate/Sort context. The projection-liveness law similarly requires every free aggregate argument to have identical symbolic value across the compared project stacks, and unchanged input relation identity.

**What this does not prove:** arbitrary Spark programs, physical ordering, execution plans, SQL ANSI runtime error equivalence, nondeterministic UDFs, future versions, or global tag noninterference. The v2 checkers are Python proof *validators with a reasoned local lemma*, not Rocq/Lean kernel-checked certificates. The real RDD identity check and frame/tag checks are separate *necessary* gates, not an axiomatic substitution for a theorem.

## Completed Round N replay and adversarial tests

- **39420058060**: exact full changed optimizer suffix ledger for 4 old M cases: **0/4** under the untouched M grammar, **2/4** under the new development grammar. M17 and M21 each have exactly two non-identity changed post-FinishAnalysis batches and a **COMPLETE_CERTIFIED_CHAIN**; both join cases correctly abstain.
- **39420267533**: 16 adversarially mutated versions of *real recorded Spark events* were rejected, **8/8** by identity v2 and **8/8** by liveness v2. Negative classes: changed unchanged Aggregate frame; dropped identity/live projected output; changed backing-input subtree; changed resolved attribute output identity; unsupported hypothetical volatile aggregate. These are mutation tests of actual JVM-recorded ASTs, **not** independent JVM executions of unsafe rewrites.
- **39420402265**: both dev candidates passed **all independent original audit layers**: state witness `COMPLETE_JVM_WITNESSES`; frame `COMPLETE_FRAME_COMPARISON`; backing-RDD input `COMPLETE_INPUT_RETROSPECTIVE`; all rule calls `COMPLETE_ALL_RULE_AUDIT`; pinned tag taxonomy `COMPLETE_HASHED_TAG_CENSUS`. Candidate counts are exactly **2/2 for each** frame, input, all-rule, and tag necessary gate. This equality allows the intersection of these measured candidate sets to be reported as 2 *only for this replay*.

The amended CI workflow `.github/workflows/queryproof-round-n-posthoc-diagnostic.yml` explicitly asserts the old 0 vs dev 2, full audit statuses and candidate counts, and all 16 negative rejections. Raw SQL-plan traces are held in disposable GitHub worker temp directories; only redacted summaries are uploaded. All work used free CI.

## Mandatory scientific caution and related work

The original published M result **does not change**. We have neither generalized from two hand-selected formerly failed examples nor established efficacy on unseen Spark plans. It would be misleading to announce 22/48 or 28/48 as a new prospective result; even retrospective extrapolation to all 48 requires replay and complete guard intersections.

Most importantly, formal SQL equivalence is well developed: [SQLSolver](https://github.com/SJTU-IPADS/SQLSolver) reasons about unbounded queries, while the August 2026 [Logos preprint](https://arxiv.org/abs/2608.15709) describes Rocq-kernel-checked, typed and order/error-aware SQL rewrite proofs (claimed 86.9% on 389 benchmark pairs). Our distinctive hypothesis is **certifying live, source-pinned Spark Catalyst rule traces and hidden JVM/physical input evidence**, not inventing relational rewrite verification. An explicit peer comparison on identical cases is essential before novelty or superiority claims.

## Next decisive programme

1. First **falsify the v2 scope**: external fresh SQL instances with COUNT/MAX/MIN, NULL and duplicate variation, plus genuine Spark expressions that produce ANSI errors and non-deterministic outputs to ensure **abstention**. Structural AST mutation tests alone are insufficient.
2. Freeze checker bytes, witnesses, thresholds, version pin and a **new, genuinely unseen N cohort** before its first execution. Stratify targets (nested COUNT/MAX/MIN), adversarial joins, and outside-fragment controls; report full chains, individual guard intersections, wall time, unsupported reasons and zero-certificate strata.
3. Formally specify relational bag/partial-error semantics for identity and project liveness, and consider translating one proof into Rocq or independent SMT before publication. Compare one pair with SQLSolver or Logos on matching observable semantics.
4. Treat join pruning as a **new theorem** with explicit join-type/null-extension conditions and predicate free variables; do not simply relax `PROJECT_BASE_CHANGED`. Analyze M29/M33 joined projections plus separate PushDownPredicates/InferFiltersFromConstraints abstentions.
5. Preserve H/K/L/M frozen artifacts and this development line as distinct evidence streams; never silently blend posthoc fixes into prior prospective denominators.

No upstream Spark PR was opened.
