# QueryProof — W32 first untouched prospective full-suffix checkpoint
2026-10-20 | Branch: `research/queryproof-adversarial-tag-frame-20261020`

## Outcome — primary prospectively frozen study

**16/32 complete changed optimizer suffixes structurally certified under the
defined Catalyst row-bag fragment.** All 32 cases executed without SQL
errors or Spark–SQLite unordered rowbag mismatches; all 32 had changed
post-`Finish Analysis` optimizer suffixes.

| Predeclared stratum | Cases | Full suffix v5 certificates |
| --- | ---: | ---: |
| INNER_COUNT (two SQL variants) | 4 | 4 |
| LEFT_COUNT (two SQL variants) | 4 | 4 |
| INNER_SUM (two SQL variants) | 4 | 4 |
| LEFT_SUM (two SQL variants) | 4 | 4 |
| INNER_LT | 4 | 0 |
| LEFT_OR | 4 | 0 |
| DISTINCT_PAIR | 4 | 0 |
| HAVING_COUNT | 4 | 0 |
| **Total** | **32** | **16** |

The four broad challenge families abstain `UNKNOWN_RULE`; this is
correct conservative behavior, **not** evidence of an unsound optimization,
nor 16 counterexamples. The 16 target results are optimizer-*suffix*
proof-chain results, **not** merely the local join-step count from R32.

## Frozen-first-run reproducibility
- Evidence: https://github.com/mhatipog/spark/actions/runs/39435756545
- First-run commit: `1d56bb97e2327ce9f8dd0e5aebccb243140c3ee8`
- Archived report: https://github.com/mhatipog/spark/actions/runs/39435756545/artifacts/12273160063
- Archive checksum SHA-256:
  `024da3af1ea90616b1364cbc5f2ba0d75e10202e38003ba4837a0373bdf3ac01`
- `tools/queryproof_prospective_w_20261020/CASES.json` contains 32 new
  frozen instances, seed 2026102057, 35 left/29 right rows per case,
  duplicate/NULL cases, two predetermined SQL variants/family.
- `tools/queryproof_prospective_w_20261020/PROTOCOL.json` frozen
  **15 exact Git blob hashes**, *including CASES.json* and each checker,
  required before first SQL execution. CI explicitly passed this preflight.
- `DESIGN.md` was written before the first execution. Neither the
  checked code nor the W data were edited based on W output.

## Independent necessary witness checks
- Complete source-tagged state-witness instrumented stream.
- Frame comparison: COMPLETE_FRAME_COMPARISON, 16 candidate survivors.
- Source RDD input identity: COMPLETE_INPUT_RETROSPECTIVE, 16.
- All optimizer-rule audit: COMPLETE_ALL_RULE_AUDIT, 16.
- Tag-shape taxonomy: COMPLETE_HASHED_TAG_CENSUS, 16.
- State stream reported COMPLETE_JVM_WITNESSES, **but the W summary does
  not expose its separate per-case state-guarded suffix count**; do not
  silently equate marker completeness with zero blocked state cases.
- The four reported candidate-count gates are subsets of the
  structurally certified candidate set in their source code; matching
  cardinalities are strong evidence of no exclusions in those categories.
  This is still observational bounded instrumentation, not a proof of
  absence of every hidden Catalyst/JVM effect.

## How W differs from the previously frozen milestones
- M48: original 48/48 executed; 20/48 earlier conditional certificates.
- O32: 32/32 executed; 16/32 complete changed suffixes for its *aggregate*
  target fragment under frozen **v2**.
- R32: 32/32 executed; 16/32 **local ColumnPruning join steps** under
  prereg Q, 0/32 complete full suffixes under prereg v2. Do not revise it.
- Posthoc M29/M33 join development: Q + two local filter laws + v5
  grouping extension; 4/4 M cases complete suffixes with independent
  necessary counters four. These were development cases.
- **W32 first run: 16/32 independently prospective v5 full-suffix
  certificates across INNER and LEFT equijoins and count/sum.**

## Mathematical scope / why not a universal proof
The local laws are identity projection preservation under duplicate-aware
inner/left equijoin, SQL three-valued non-NULL key inference (only right
side under LEFT OUTER), commuting pure IsNotNull filters through identity
projection, and idempotence of duplicate pure total GROUP BY keys over
an unchanged joined bag. These are known algebraic facts, with exact AST
side conditions. The audit composes every recorded changed optimizer rule,
rechecks frame, state and input provenance, and abstains outside grammar.

The new finite model independently checked 2,916 join-guard comparisons
and 531,441 multiset/projection cases; it demonstrated unsound mutants for
filtering LEFT's preserved side and for treating NULL-safe equality as
NULL-intolerant. That is bounded evidence, **not** mechanized proof.
The 20 posthoc filter and 12 posthoc grouping AST mutants were rejected.

No guarantee is asserted for physical query execution, output order,
runtime errors, ANSI corner cases outside the grammar, side effects,
UDFs, arbitrary Catalyst operators, all JVM mutable state or SQL broadly.
The 32 purposely clustered synthetics are not a random sample of Spark
production query distribution; the SQL templates partially reuse R32
shapes, albeit with new bags and withheld variants.

## Adjacent research: honest differentiation
- SQLSolver (SIGMOD 2024) already proves SQL query equivalence and
  evaluates Spark SQL benchmark pairs: https://github.com/SJTU-IPADS/SQLSolver
- VeriEQL (OOPSLA 2024) verifies a richer SQL fragment with bounded
  symbolic bag/NULL semantics:
  https://github.com/VeriEQL/VeriEQL
- Ricciotti & Cheney (2022) mechanize SQL with NULLs in Coq:
  https://doi.org/10.1007/s10817-022-09632-4
- QueryProof's **candidate differentiator**, not yet an established
  novelty claim, is witnessing *actually executed Spark Catalyst rewrite
  chains* with independently replayed rule, source-identity, hidden-state,
  frame and tag evidence. It is presently weaker than general
  machine-checked SQL semantics and must be benchmarked against the above.

## Next falsifiable round
1. In a separate **posthoc** W replay, expose the state-guarded case-ID set,
   not only the witness-completeness status, and verify all five guard
   intersections per case; **do not rewrite the W first-run result**.
2. Independently mechanize the left-join non-NULL filter and duplicate-key
   grouping sublemmas in a proof assistant or a defensible solver encoding;
   compare against SQLSolver/VeriEQL, retain unsupported/null-safe mutants.
3. New *structurally diverse* held-out join+projection conditions:
   multi-key joins, reordered keys, aliases, more nested frames; prereg
   new checker/grammar and hidden case families, report negative results.
4. Reduce reliance on observer completeness assumptions by checking
   native Spark source instrumentation and formalizing each unobserved
   hidden-state category as an explicit unsupported boundary.
