# QueryProof V7 — identity-cycle falsification protocol (20 October 2026)

Status: DEVELOPMENT; NOT a prospective W32 rescore.

## Source-level finding
In both frozen v5 and posthoc v6 `scope_ledger`, `_chain_proof(start,end,steps)` returns `EXACT_IDENTITY` immediately when `start == end`, before testing `steps[*].certified`. The executor path has the same shortcut. Thus the code can report a complete suffix while an identity-returning batch contains unsupported effective steps. This is a counterexample to the claim that *every executed effective rule* has a certificate; it is not necessarily a counterexample to extensional endpoint equivalence under pure and faithful semantics.

## Independent counterexample
Let P and Q be distinct serialized plans and let the recorded effective rules be P→Q (UNKNOWN) and Q→P (UNKNOWN). The old checker returns EXACT_IDENTITY on the batch P→P. A proof-carrying-event checker must reject both unknown steps. Similarly an executor with identical entry/exit and an unsupported changed child batch must not be called a fully certified executed trace.

## Required diagnostic
1. Replay unchanged W32 privately with the original V6 ledger and a separately versioned strict ledger.
2. Count accepted case IDs and the exact intersection; report identity-returning batches with nonzero changed events and unsupported steps by case, without exporting raw SQL/ASTs.
3. Require strict continuity and per-step certificate on every recorded effective event. An identity batch with zero effective events may be accepted as a no-op; identity after events requires a certified cycle.
4. Preserve original W32=16/32, X=0/16 and V6 posthoc=20/32. Never relabel first-run results.
5. Add mutants: P→Q→P with UNKNOWN edges must reject; both edges certified may pass; an event missing between scope endpoints must reject; nested unsupported batch under identity executor must reject.
6. Source-level completeness concern: `QueryProofAstTrace.capture` uses a global 2500-event cap and only records events where `!result.fastEquals(plan)`; no trace completeness theorem follows from seeing scope markers. Require overflow accounting and explicit completeness evidence before unconditional certification.
7. Separately audit semantics-preserving identity of TreeNode.toJSON, ExprId resolution, mutable tags, and side effects. These are unproved assumptions.

## Scientific interpretation
The stricter audit measures *trace-wide proof coverage*, not endpoint equivalence. An identical endpoint can be mathematically sufficient under a faithful pure-plan semantics, but the central QueryProof promise is evidence for the actual optimizer transformations. Keep the two claims separate.

## Next research
Create a fail-closed proof-event protocol with monotone sequence IDs, attempted/effective event counts per batch, overflow markers, explicit entry/exit hashes, and a source-pinned Catalyst→typed semantics bridge. Only then run a fresh prospective V7 benchmark with preregistered new SQL and mutations.