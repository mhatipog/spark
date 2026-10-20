# QueryProof — compositional Project liveness checkpoint (20 October 2026)

## Evidence summary

The generalized certificate now accepts genuine nested Catalyst Project-stack
restructuring under an unchanged Aggregate context. Evidence is intentionally
stratified:

| Cohort | Cases | Changed events | ColumnPruning events | Exact liveness proofs | Row-bag mismatches | Evidence status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Original independently authored unused DuckDB cohort A01–A15 | 15 | 116 | 8 | **5** | 0 | Prospectively frozen *narrow* verifier |
| Same A01–A15 with extended verifier, redesigned after abstentions | 15 | 116 | 8 | **8** | 0 | **Retrospective**, no holdout credit |
| 16 previously unused DuckDB GROUP/HAVING queries, corrected fixture | 16 | 125 | **0** | 0 | 0 | Diagnostic; no relevant events, fixture correction invalidated freeze |
| Fixed 10-family x six-settings synthetic SQL stress | **60** | **516** | **60** | **60** | **0** | Prospective to unseen *design-informed generated* instances; not independent SQL authorship |

The original historical five-checker union remains UNKNOWN on all 516 events
of the generated corpus. The new family proves 60 local transitions, leaving
the other 456 optimizer transitions uncertified; this is *not* end-to-end
optimizer correctness or representative optimizer-wide coverage.

## Proof theorem and scope

Let R be a finite input bag and F and G finite stacks of deterministic, total,
exactly-one-output-row-per-input-row Project operators on the *same* unchanged
base relation. Let K be the resolved input expression identities read by an
unchanged, whitelisted, pure Aggregate context C, optionally beneath identical
Sort ancestors observed only as bags. Suppose F and G expose the same symbolic
typed value at every attribute in K for all possible input rows, with checked
scope and output identity. Then C(F(R)) and C(G(R)) have identical row bags.
Each base row occurrence survives each projection exactly once and agrees on
every observed input variable, including NULLs and duplicated occurrences.
Sort does not establish ordered output equivalence.

The implementation accepts strict Spark 4.2.0 captured Catalyst preorder ASTs,
resolved ExprIds and type/name scopes, at most eight layers of Project over
identical whitelisted inputs. Project expressions are limited to direct
attributes, typed literals and integral Remainder with a nonzero type-bounded
literal divisor; Remainder evalContext is preserved in symbolic identity.
Alias qualifier and nonInheritableMetadataKeys are accepted only when empty.
Unknown/volatile operators, computed casts, dynamic or zero divisors,
malformed metadata, duplicate ExprIds, unresolved scopes and changed live
expressions abstain. A wider but whitelisted grammar is permitted only inside
the *unchanged* Aggregate context. All proof claims are for defined pure
logical bag semantics, not Spark runtime errors, physical order or performance.

[Verifier source](https://github.com/mhatipog/spark/blob/research/queryproof-generalized-liveness-20261020/tools/queryproof_context_validator/generalized_projection_liveness_certificate.py)
and [adversarial tests](https://github.com/mhatipog/spark/blob/research/queryproof-generalized-liveness-20261020/tools/queryproof_context_validator/test_generalized_projection_liveness.py).

## A10 real-AST development

Original DuckDB A10: SELECT i % 2 AS k, SUM(i) FROM integers GROUP BY k, k ORDER BY 1;

Spark's actual ColumnPruning changed a depth-one Project exposing three
columns into a depth-two Project stack exposing two columns beneath unchanged
Aggregate and Sort. Two live attributes retained identical normalized
values, so the generalized checker certifies the resulting row-bag law.
The narrow frozen checker abstained because the change was not a simple
single inserted Project.

Chronology: the initial generalized checker accepted 7/8 old cases and A10
abstained at Alias metadata. Safe support for empty qualifier and
nonInheritableMetadataKeys exposed Remainder evalContext; incorporating that
immutable node property exposed Spark's decimal-string encoding of integral
literals. Strict signed-range and nonzero checks finally accepted A10.
This **8/8** score is retrospective because A10 drove the changes.

[Passing final A01–A15 posthoc A10 replay](https://github.com/mhatipog/spark/actions/runs/39376750519).
The independent original 5/8 result remains unmodified.

## Unsuccessful independent-author sample

A separate 16-statement unused DuckDB cohort first terminated after 12
executed queries when the manifest creator accidentally rounded
-9223372036854775808 into an out-of-range JavaScript number. The literal was
corrected to a lossless signed-64-bit decimal representation, SQL and verifier
remained identical, but the protocol revoked prospective status because a
partial first replay had already happened. Its completed diagnostic replay
covered 16/16 queries, 125 optimizer events and no row mismatches, but
**zero named ColumnPruning events**. It is not evidence against or for the new
theorem. [Diagnostic run](https://github.com/mhatipog/spark/actions/runs/39375628967).

## First fresh prospective generated stress result

The generated cohort files were frozen together:
- tools/queryproof_metamorphic_liveness_20261020/CASES.json
- tools/queryproof_metamorphic_liveness_20261020/FROZEN_PROTOCOL.json
- tools/queryproof_metamorphic_liveness_20261020/VERIFIER_PIN.json
- tools/queryproof_metamorphic_liveness_20261020/run_metamorphic.py

Checker Git blob: c1a8c81fa10d94d191e7c930254d78e9de7c3258,
**exact match at execution**.

Ten a priori SQL families by six modulus/literal settings (2, 3, 5, 7, 11,
13) and deterministic PRNG seed 20261020 produced 60 never-before-executed
SQL instances. Each has eight rows with explicit duplicates, NULL grouping
keys, NULL measures and signed integers, plus an unused input column.
Templates were intentionally inspired by previous A02/A05/A08/A09/A10
observations; they are *not* independently authored SQL and the cases are
highly correlated. No cases were dropped or fixed after observing outputs.

- 60/60 SQL cases executed.
- 516 changed Spark Catalyst rule events captured.
- 60 named ColumnPruning events, **60 exact liveness certificates**.
- 18 of 60 certificates validated depth-one to depth-two projection-stack
  restructuring (not merely inserted single-Project attribute pruning).
- Zero Spark–SQLite row-bag mismatches; zero SQL errors, trace decoder failures
  or instrumentation failures.
- Zero modifications to the checker after pinning.

[First frozen 60-case run, all checks green](https://github.com/mhatipog/spark/actions/runs/39377035675).

## Interpretation and next decisive experiment

This is an actual generalization of the proof family: resolved symbolic value
observations allow certificates across non-identical Project trees. The
60/60 targeted result is strong executable regression evidence, but cannot
estimate real-world Spark optimizer coverage because the sample was designed
to produce this exact family. Nor does 60 local proofs mean a complete Spark
optimization trace is certified when 456 other transformations remain UNKNOWN.

Next priorities, in order:

1. Find and freeze a **new independently authored external SQL** sample that
   creates nested Project ColumnPruning transitions; do not prescreen with
   the final checker, and report zero-hit sources candidly.
2. Add counterexample-oriented tests and independent finite semantics for
   every widened expression/operator class, especially partial arithmetic,
   null types and alias scopes.
3. Certify an **entire multi-rule optimizer chain** across changed plans:
   without proof of every step, local certificates do not prove the final plan.
4. Compare formal novelty to Cosette, SQLSolver and Rulescript. Focus any
   scientific contribution on live Spark Catalyst event provenance and
   trace-local, compositional proof certificates, not novelty of SQL
   equivalence in general.
5. Seek external peer review and potential upstream integration only after
   independent-author and chain-level evidence; do not conflate this branch
   with the unrelated upstream correctness PR #59445.


## Post-checkpoint sanitized rule inventory and successor target

A workflow-only diagnostic replay of the same frozen 60-case corpus (with the
unchanged pinned verifier) logged the complete named-rule denominator:
[rule-inventory replay](https://github.com/mhatipog/spark/actions/runs/39377411390).

| Named transition source | Count | Current evidence |
| --- | ---: | --- |
| Analyzer phase/batch boundary | 288 | UNKNOWN; not an individually certified rewrite |
| Optimizer phase/batch boundary | 60 | UNKNOWN; not an individually certified rewrite |
| ColumnPruning | 60 | 60 prospective local proof certificates |
| RemoveLiteralFromGroupExpressions | 24 | UNKNOWN |
| CollapseProject | 18 | 12 retrospective aggregate-framed identity certificates; 6 abstentions |
| AnsiTypeCoercion | 18 | UNKNOWN |
| NullPropagation | 18 | UNKNOWN |
| ResolveTimeZone | 18 | UNKNOWN |
| RemoveRepetitionFromGroupExpressions | 12 | UNKNOWN |

Across the 168 events not labeled as Analyzer/Optimizer boundaries, the new
family covers 60 (35.7%) in this deliberately enriched generated corpus. This
is descriptive *within the sample*, never a production coverage estimate.

We inspected the exact Spark 4.2 optimizer implementation in
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala,
around lines 3561 and 3639. The strongest next standalone lemma is duplicate
grouping-key idempotence: under an otherwise identical Aggregate, deleting a
repeat of an identical, deterministic grouping expression preserves the
equivalence classes of input rows and hence aggregate bag output. The
RemoveRepetitionFromGroupExpressions rule produced 12 observed events and
could become a second provable local family if strict resolved-expression
and unchanged-child checks succeed on frozen actual ASTs.

**Important trap for the adjacent RemoveLiteralFromGroupExpressions rule:**
Spark deliberately retains one literal grouping expression when *all* group
keys are constant. Removing the final constant would convert GROUP BY constant
into a scalar aggregate and change behavior on **empty input** (historical
SPARK-17114). Any future proof must distinguish mixed literal/nonliteral
keys from the all-literal case, preserve the empty-bag semantics, and reject
a naive last-grouping-key deletion. This is an especially valuable adversarial
test and illustrates why prospective proof construction matters.

The more consequential long-term result would be composing these isolated
certificates with trustworthy intermediate plan boundary evidence into a
complete optimizer-chain proof; the trace wrappers are not independently
certified merely because some contained steps pass.
