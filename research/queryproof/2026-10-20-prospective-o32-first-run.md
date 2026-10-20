# QueryProof — prospective O32 first-run holdout, limitations, and research checkpoint

**Date:** 2026-10-20  
**Owner:** Metin Hatipogullari  
**Repository / branch:** `mhatipog/spark` / `research/queryproof-adversarial-tag-frame-20261020`  
**Status:** INDEPENDENT PROSPECTIVE SYNTHETIC HOLDOUT COMPLETED; conditional local certificates, **not** an end-to-end or kernel-checked Spark equivalence theorem; no upstream PR.

## Scientific headline

**O32's first execution succeeded:** 32/32 independently generated finite Spark 4.2.0 SQL instances ran with no execution errors or unordered Spark/SQLite row-multiset disagreements. The pinned developmental v2 verifier returned **16/32 complete changed optimizer-suffix chains**. All 16 are in the targeted aggregate fragment; **0/16** challenge cases were certified. Four independent necessary gate families (JVM state, scope/frame, input-identity/all-rule and tag observations, enumerated below) all agreed on precisely the same 16 structural candidates.

This is genuine generalization to *previously unexecuted synthetic datasets* for the narrow SUM/COUNT/MIN/MAX projection/liveness grammar developed after M. It is **not** an unbiased estimate of real SQL proof coverage, nor evidence of safety on arbitrary joins, user-defined functions, errors, ordering, or native physical plans.

## Immutable design and provenance

1. Previous **prospective M48** first run remains intact: 48/48 executed, 412 changed rule events, **20/48** complete *original* conditional suffix certificates; 16/24 target-enriched and 4/24 generalization challenge. Run [39417193656](https://github.com/mhatipog/spark/actions/runs/39417193656). M's original scorer was never rewritten to credit the later v2 development.
2. **Posthoc development** reused known M17, M21, M29 and M33 failures, deliberately labeled as training: v2 passed M17/MAX and M21/nested COUNT (2/4), not the two joins. Sixteen AST-level adversarial negatives built from observed rule events were rejected; these are **mutations of actual observed plans**, *not* independently observed malicious JVM transitions. See `research/queryproof/2026-10-20-round-n-posthoc-count-extrema-identity.md`; successful v2 chain CI [39420574040](https://github.com/mhatipog/spark/actions/runs/39420574040).
3. An intended **N32** first run [39421965245](https://github.com/mhatipog/spark/actions/runs/39421965245) executed 32 SQL instances, but the post-execution report step crashed because N's protocol omitted `frozen_verifier_commit`. Private scores were not recovered; **N was not rerun or represented as a pristine holdout result**.
4. **O32** was frozen as a fresh, genuinely different deterministic cohort with seed **2026102025**, not N's data. Freeze files: `tools/queryproof_prospective_o_20261020/{CASES.json,PROTOCOL.json,DESIGN.md,run_o.py}`; workflow `.github/workflows/queryproof-prospective-o32.yml`. The protocol has 10 exact verifier/observer Git blob SHA pins, `frozen_verifier_commit=743bcf8c232373235438c7ba4b0939d6ce697646`, fixed 16 target/16 challenge stratum sizes, an UNKNOWN-is-not-proof policy, and no postrun tuning.
5. The O workflow's **pre-execution** step hashes the verifier blobs, compiles pinned Python, checks corpus IDs/strata, and inspects `run_external.py` for every directly indexed protocol field before it lets *any* O SQL run. Thus it incorporates the N administrative lesson.
6. **O32 first and only executed holdout:** [GitHub Actions run 39422632158](https://github.com/mhatipog/spark/actions/runs/39422632158), run HEAD `50732780` (check exact full SHA in run metadata); successful. Sanitized artifact `queryproof-prospective-o32-first-run`, [artifact 12264551514](https://github.com/mhatipog/spark/actions/runs/39422632158/artifacts/12264551514), 30-day retention, GitHub reported artifact SHA-256 `3da81c843a2bc005756761960d92dc6de2a484ec07da13d8358bc917a5926032`. Freeze the artifact externally before expiry if long-term reproducibility is needed.

## Exact first-run measurements

| Measurement | O32 |
|---|---:|
| Executed synthetic SQL cases | **32/32** |
| Execution errors | **0** |
| Finite Spark/SQLite row-multiset mismatches | **0** |
| Captured changed Catalyst rule events | **288** |
| Changed post-FinishAnalysis optimizer suffixes | **32** |
| Complete structurally certified changed suffixes | **16** |
| Frame witnesses passing the exact candidate set | **16** |
| Live backing-RDD input-identity witnesses passing | **16** |
| All-rule silent-drift audit witnesses passing | **16** |
| Tag census / shape gate passing | **16** |
| Conservative same-case all-necessary-gates intersection | **16** |
| Aggregate target stratum certificates | **16/16** |
| Broadening/generalization challenge certificates | **0/16** |

Source: the `QUERYPROOF_O32_FIRST_RUN` JSON line in successful first-run job logs; independently derived from the preserved run output.

| SQL family (four fresh data cases each) | Complete conditional suffix chains |
|---|---:|
| MAX_MOD | **4/4** |
| MIN_MOD | **4/4** |
| NESTED_COUNT | **4/4** |
| NESTED_MAX | **4/4** |
| INNER_JOIN | **0/4** |
| LEFT_JOIN | **0/4** |
| DISTINCT | **0/4** |
| HAVING | **0/4** |

The all-gates result is `COMPLETE_ALL_GATES`, with scope `RETROSPECTIVE_EXACT_CATALYST_SCOPE_LEDGER`, JVM state `COMPLETE_JVM_WITNESSES`, frame `COMPLETE_FRAME_COMPARISON`, input `COMPLETE_INPUT_RETROSPECTIVE`, all-rule `COMPLETE_ALL_RULE_AUDIT`, and tag `COMPLETE_HASHED_TAG_CENSUS`. The four independently computed gate sets are reported as exactly equal to the structural candidate set, not merely equal-sized, with `ALL_GATE_SETS_EQUAL_BASELINE`.

**Unsupported observed transitions remain explicitly UNKNOWN.** Examples from this cohort: optimizer `ColumnPruning` (12 occurrences outside checker grammar), `PushDownPredicates` (8), `InferFiltersFromConstraints` (8), `ReplaceDistinctWithAggregate` (4), `ConstantFolding` (4), `RemoveRepetitionFromGroupExpressions` (12 in analyzer) and other analyzer/optimizer operations. A structural proof of a *suffix* is not a full SQL query proof. The finite external rowbag comparison tests outputs on these examples; it is not a universal theorem.

## Runtime effects boundary: extra Spark experiment

Separate, explicitly developmental `tools/queryproof_round_n_dev_20261020/error_boundary.py` ran on PySpark **4.2.0** in successful [run 39422716398](https://github.com/mhatipog/spark/actions/runs/39422716398) (HEAD `42922960`). In these particular queries:

- Direct and apparently unused constant `1/0` and `raise_error` expressions **both raised**, rather than exhibiting the hoped-for dead-error-elision difference.
- Row-dependent division-by-zero and `raise_error` likewise raised, including in apparently unused projections.
- A safe unused arithmetic projection under `COUNT(*)` produced two rows counted (`[[2]]`).

**Do not claim a Spark optimizer defect or a witnessed error-elision counterexample from these data.** They show why the current theorem must retain its **defined logical row-bag-only** boundary until error-order semantics are modeled and separately tested.

## Why this matters, and why it is still limited

The meaningful result is the *combination* of real Spark optimizer rule observation, conservative local structural lemmas, exact source/blob provenance, and independently replayed checks for silent state, live RDD inputs, frame, all-rule drift and metadata tags. A novel methods argument would need to position this narrowly against existing SQL rewrite verification and mechanized semantics; it **does not** invent SQL equivalence checking or establish dominance over formal prior work such as SQLSolver or the Rocq-based Logos preprint.

Selection was deliberately target-heavy and clustered by four queries per family. Thus `16/32` is a descriptive design outcome, **not 50% general Spark proof coverage**. Nor should one pool `20/48` (original frozen M checker) with `16/32` (revised v2 on new O) as an apparent one-verifier score. Zero finite mismatches does not establish correctness on all databases; guard agreement alone is necessary but not sufficient.

## Next decisive falsification work (separate future frozen round)

1. **Machine-check the smallest two core lemmas:** source-preserving exact identity projection collapse in an aggregate frame; bound-column/liveness projection insertion with duplicate grouping and NULL semantics. Keep typed attributes/exprIds, bags, 3-valued predicates, grouping and aggregate degeneracies explicit. Independently generated models and mutation controls should try to break the proofs.
2. **Tackle a *genuine join* transition, not posthoc score rescue.** Reconstruct actual two-child `ColumnPruning` with join condition references, output ordering, NULL extension for left joins, and row multiplicities, and prove/falsify minimal lossless projection conditions. Most likely first hard step: separate equijoin inner preservation from left-outer null-extension and ensure condition input attributes remain live.
3. Freeze another untouched *join-focused* corpus **before any run**, with changed source/checker/observer blobs; explicitly permit zero proofs. Avoid editing O or N outcomes retroactively.
4. Introduce an **observable outcome algebra** (`Rows(bag)` / `Error(class)` / nondeterministic / unmodeled) before claiming any ANSI-error preservation, exceptions, order or volatile-function correctness.
5. Compare accurately with SQLSolver and mechanized Logos under the same declared grammar, theorem and assumptions. Separate *instrumentation novelty* from theorem novelty; obtain independent Scala/JVM witnesses if possible.

**Decision:** Continue. The narrow aggregate theorem earned evidence on fresh O data; joining it into a general-purpose Spark proof would be premature. Better to make the small theorem machine-checkable and make joins fail honestly than to widen certificate counts by unverified heuristics.
