# QueryProof — V13 repair, V13 vacuity audit, V14 stored identity (21 October 2026)

Successor round 1. Retrospective W32 development data only; no frozen score changes.

## 1. V13 integration repair

Run 39584281197 (head `7a1d7358`) failed: `QueryProofV13LivePairs.v` imports
`QueryProofGuardPayload` and `QueryProofGuardPairPayload`, which the job never
compiled. Commit `ebbb59eb11` compiles them in import order
(Certificate → Pair → Payload → PairPayload → V10LiveInstances → V13LivePairs).
Run 39594442892 **passed**: 32/32 executed, 0 row-oracle mismatches, 4 generated
pairs, strict native object attestations 0, ExprId value-field attestations 4,
Coq marker `QUERYPROOF_V13_LIVE_PLAN_PAIR_CERTIFICATES_COQ_KERNEL_CHECKED`.
The failed run stays on record.

## 2. Audit: the V13 live instances are kernel-checked but carry no information

The universal theorems (`checked_certificate_sound`,
`checked_plan_pair_preserves_bags`, `checked_pair_preserves_entire_row_bags`)
are correct, contain no `Admitted`/`Axiom`, and state the intended law:
adding non-NULL guards on the inputs of a strict (NULL-rejecting) join, and
on the LEFT OUTER join's NULL-supplying side only, preserves every output row
bag, including arbitrary payload rows and duplicates.

The *per-event* instances, however, are vacuous as evidence about Spark:

- `emit_pair_instances.py` writes the OLD term with constant
  `filter_left := false; filter_right := false` regardless of the real plan.
- OLD and NEW copy `comparison` and `join_kind` from the same Python field.
- The only way `transition_ok` could fail is a LEFT OUTER left guard, which
  the emitter already asserts away in Python.

So every instance is a corollary of the universal theorem applied to constants
chosen by Python. The kernel decides nothing about the actual Catalyst event.
The whole correspondence (guarded attribute = compared attribute, guard
placement, unchanged frame, OLD had no guard) lives in the V10/V11 Python
reflection. "Coq checks the live plan pair" must not be claimed in a
stronger sense than "Python's four booleans satisfy a proven-sound predicate".

Mathematical remark: the comparator detail is irrelevant to the law. All it
needs is that the join condition can be TRUE only when the guarded key is
non-NULL (Spark's `NullIntolerant` property), together with
`filter_implied_guard`. A deep checker should prove *implication of the guard
by the join condition* over an expression syntax, not enumerate comparators.

## 3. V12 strict identity failure: diagnosed as an observer artifact

V12 strict JVM-identity attestation rejected all four of W17–W20 with the same
signature: 2 missing prior objects, 0 changed, 0 new variants, source variant
sets equal. Hypothesis: `LogicalPlan.output` is *derived* on every call, and
`Alias.toAttribute` allocates a fresh `AttributeReference` (Project/Aggregate),
so an `IdentityHashMap` keyed on output occurrences fails even for an
unchanged plan.

Local probe (official pyspark 4.2.0): calling `output` twice on the same
optimized plan object gives a new object for the aliased Project output
(1/2) and Aggregate output (1/2). Join/Filter/LogicalRDD outputs are stable.

V14 (`tools/queryproof_v14_stored_identity_20261021/`, predictions frozen in
`PROTOCOL.json` before the W32 replay) splits *stored* NamedExpressions
(reachable through `QueryPlan.expressions`) from *derived* output occurrences
and adds a SELF control (the same pre-rule plan object observed twice).

Controls (run 39596229910, repeated in 39596584123):

| Control | V12 strict | V14 SELF derived missing | V14 stored diff (missing,changed,fresh,unseen) |
| --- | --- | --- | --- |
| Real `InferFiltersFromConstraints`, synthetic aliased strict inner join | reject | 1 (= V12 missing) | 0,0,0,0 |
| V12 metadata-only adversary | reject | 0 | 1,0,1,1 (exposed) |
| V12 tag-only adversary | — | 0 | 0,0,0,0 (blind, as predicted) |
| V12 transient tag adversary | — | 0 | 0,0,0,0 (blind, as predicted) |

W32 replay, run 39596584123 (`61bf5069c8`): 32/32 executed, 0 row-oracle
mismatches, V9 count still 24.

| Case | V12 missing | SELF stored | SELF derived missing | pre→post stored | H1 | H2 |
| --- | --- | --- | --- | --- | --- | --- |
| W17–W20 (each) | 2 | 0,0,0,0 | 2 | 0,0,0,0 | yes | yes |

H1 (failure fully explained by derived re-allocation): **4/4**.
H2 (stored NamedExpression identity preserved, no fresh stored objects): **4/4**.
The inserted `IsNotNull` guards reference the *same JVM attribute objects*
already stored in the pre-rule plan (fresh = 0).
Run 39596229910 failed only in the W32 step, because the sparse checkout
omitted `research/queryproof/proofs`. It stays on record.

**Interpretation.** For these four events the strict-identity rejection is
caused by the observer, not by any rule effect. The stricter, well-defined
notion, identity of *stored* NamedExpressions, holds exactly, and it is
strictly stronger than the V12 ExprId value-field attestation used by V13.
This is evidence about *source binding*, not semantics, and not about tags:
the tag and transient adversaries remain invisible by construction. V12's
strict identity tier should be retired or redefined as stored identity; it
measures the observer.

## 4. Evidence hierarchy after this round

1. Theorem: universal guard/pair/payload laws, kernel-checked (unchanged).
2. Source correspondence: stored identity attested 4/4 (retrospective);
   per-event Coq instances carry no extra information (section 2).
3. Trace completeness / hidden state: unchanged. X 0/16 frozen; tags and
   transient effects unobserved by V12/V14.
4. Composition across the suffix: unchanged, not established.
5. Prospective: W32 16/32 and X 0/16 frozen. No new prospective data.

## 5. Next step (highest information)

Replace the boolean certificate with a **deep-embedded Catalyst fragment in
Coq** (resolved attribute ids, 3VL expressions with `IsNotNull`, `And` and
null-intolerant comparisons, Filter/Join/Project/opaque leaves) and an
executable checker `iffc_ok old new` that *itself* decides: NEW equals OLD
except for guard filters inserted above join inputs; each guarded attribute is
in that child's output and is null-rejected by the join condition; no
preserved-side outer guards; and the frame is syntactically identical.
Prove `iffc_ok old new = true -> forall db, eval old db = eval new db`.
Python then shrinks to a total, near-syntactic TreeNode-JSON → term printer,
and opaque subplans are compared as uninterpreted relations. Negative
controls: wrong ExprId guard, null-safe join, preserved-side guard, frame
edits, and guards on attributes absent from the child.
