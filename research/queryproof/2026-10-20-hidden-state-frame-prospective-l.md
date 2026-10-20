# QueryProof: hidden JVM state and first prospective frame-witness cohort L

Date: 2026-10-20  
Branch: research/queryproof-generalized-liveness-20261020  
Owner: mhatipog/spark personal fork; no upstream PR or Spark product patch.

## Main result

The previous H/K scope ledger connected serialized Catalyst snapshots, but
Spark's TreeNode.toJSON is **not injective** over actual JVM plan state:

- TreeNode._tags is a mutable auxiliary map outside constructor JSON fields.
- TreeNode.parseToJson deliberately serializes Metadata as Metadata.empty.
- Unsupported Seq/Map branches may serialize as JNull.
- A compiled Scala control independently reproduced real tag and metadata
  JSON collisions (workflow 39390796794).

Therefore the original JSON-equality optimizer proof **cannot** be called
unconditional. The initial version of the JVM guard correctly abstained on
all 48 H and 48 K cases because the hidden state was nonempty. This was
scientific evidence, not an optimizer counterexample.

## Runtime state dissection

Ephemeral Catalyst overlays inspect actual JVM state at each instrumented
scope and changed-rule boundary, without storing raw values:

- Nonempty Metadata, mutable TreeNode tags, and JSON-opaque collection counts.
- Class-only fingerprints identify where the hidden fields occur.
- For the post-FinishAnalysis suffix, no opaque collections were observed.
- Remaining state was localized to AttributeReference.metadata, LogicalRDD
  tags and Sort tags (H and K, consistently).
- A stronger observer privately SHA-256 hashes actual tag maps (using
  value string forms), attribute metadata keyed by ExprId, and complete
  output field identities/metadata. It compares successive scope and
  rule-event states; no tag or output drift, surviving-attribute metadata
  drift, or opaque collection in a measured optimizer suffix is accepted.
- Raw Catalyst ASTs and the unhashed data remain in the temporary worker
  stderr file and are deleted after scoring.

Adversarial tests verify missing/forged witness rejection, hash-frame
mutation rejection, output metadata divergence, and normalization-cut
locality. None of the original frozen H/K scorers, manifests or certificates
were edited.

### Retrospective results

| Cohort | H | K |
| --- | ---: | ---: |
| Spark/SQLite bag matches | 48/48 | 48/48 |
| Changed-rule trace events | 438 | 432 |
| Post-FinishAnalysis batch intervals | 1,872 | 1,872 |
| Frame violations in these intervals | 0 | 0 |
| Changed optimizer suffixes certified by old local checker | 30 | 30 |
| Those also preserving the additional JVM frame | 30 | 30 |

Independent retrospective frame run:  
https://github.com/mhatipog/spark/actions/runs/39392543769

**Qualification:** A cryptographic fingerprint of a string representation
is an observational identity witness, not a mathematical proof that
metadata or tags are semantically irrelevant. These are **conditional**
rowbag certificates for a narrow grammar, not whole-engine correctness.

### First genuinely prospective frame test: L01-L48

Before **any L SQL execution**, the full source/checker/scope/frame modules
and the L manifest/protocol were frozen using Git blob hashes in
tools/queryproof_prospective_l_20261020/VERIFIER_PIN.json.

This L corpus is seeded with xorshift32 and uses sixteen-row data tables.
Unlike the mostly related H/K grammars, L covers eight distinct families:
FILTERED_REPEAT, COALESCE_ALIAS, CASE_ALIAS, PROJECTED_SUBQUERY,
SELF_JOIN, HAVING_REPEAT, TWO_KEY_MIXED and ARITH_ALIAS. Six independently
generated tables are tested per family. It is still synthetic and not
third-party SQL.

The first two CI attempts stopped **before any SQL execution**:
1. The disposable instrumented Scala source failed its original-source pin.
   Restoring the pristine checkout source *after JAR compilation* resolved
   the test-harness ordering issue without changing a proof module.
2. The frozen worker eagerly evaluated manifest["tables"] even when every
   case had its own tables. An atomic pre-SQL fix added a redundant top-level
   table fallback and updated its Git blob pin. All 48 SQL statements and
   their per-case data remained byte-for-byte identical; zero SQL had run.

The first valid L execution succeeded on the unchanged frozen verifier:

| Prospective L observation | Count |
| --- | ---: |
| SQL cases executed | 48/48 |
| Finite Spark/SQLite rowbag mismatches | 0 |
| Actual changed-rule trace events | 474 |
| Post-FinishAnalysis batch intervals fully covered | 1,872 |
| Hidden-frame drift in those batch intervals | 0 |
| Whole changed optimizer suffixes conditionally certified | **12/48** |
| Changed suffixes UNKNOWN | **36/48** |
| Changed batch certificates | 24 |

Original **first-run result** and provenance:  
https://github.com/mhatipog/spark/actions/runs/39393542980

Posthoc, artifact-only classification, without a new Spark execution:  
https://github.com/mhatipog/spark/actions/runs/39393862947

### L proof coverage by family

| Family (six cases each) | Certified changed suffixes |
| --- | ---: |
| FILTERED_REPEAT | 6 |
| TWO_KEY_MIXED | 6 |
| COALESCE_ALIAS | 0 |
| CASE_ALIAS | 0 |
| PROJECTED_SUBQUERY | 0 |
| SELF_JOIN | 0 |
| HAVING_REPEAT | 0 |
| ARITH_ALIAS | 0 |

Certified L case IDs: L01-L06 and L37-L42. All other cases are UNKNOWN,
**not** demonstrated inequivalences or Spark bugs.

The 30 unsupported RemoveRepetitionFromGroupExpressions transitions had
these specific checker rejection reasons:

- 18: UNSAFE_OR_UNKNOWN_PROJECT_EXPRESSION
- 6: UNSUPPORTED_INPUT_SCHEMA
- 6: AGGREGATE_INPUT_CHANGED

Other changed optimizer events requiring additional proof include
ColumnPruning (30), CollapseProject (18), InferFiltersFromConstraints
(12), PushDownPredicates and ConstantFolding. Analyzer and FinishAnalysis
normalization events are intentionally excluded from the optimizer
equational suffix.

An important negative observation: the **hidden JVM state remained stable**
through all 1,872 L post-normalization batch intervals. Thus the 36 UNKNOWN
suffixes stemmed from narrow local mathematical rule certificates, not from
observed hidden-frame drift.

## Next gate: widen pure grouping expressions without circular trust

The next candidate is a separately versioned checker for removing repeated
deterministic, total grouping keys. Its proof is simple but powerful:

    partition(R, [g, g, h, g]) = partition(R, [g, h])

provided each g is deterministic and has no observable side effects or
exceptional evaluation differences in the claimed domain.

Candidate grammar extensions: nullable COALESCE, CASE WHEN, safe arithmetic,
and nested projections with explicitly scoped attributes. Test malformed
ASTs, nondeterministic functions, divide-by-zero or overflow, alias
capture, differing ExprIds, metadata drift and changed child plans.
Do not infer determinism from plan JSON alone and do not add broad wildcard
acceptance. Rules whose input subtree genuinely changes need independent
compositional proof or must remain UNKNOWN.

Crucially: modifying the checker now **cannot upgrade** L's already
reported 12/48 prospective result. Any application to L is retrospective.
Freeze the successor verifier before a fresh M cohort first run.

## Remaining scientific limits

- Schema metadata and mutable optimizer tags may matter outside these
  measured rowbag-scope fragments; hashed stability supports preservation
  along a trace, not general noninterference.
- SHA-256 fingerprints depend on runtime serialization and are not formal
  injectivity proofs for arbitrary JVM objects.
- SQL rowbag reference checks are finite tests, not universal theorems.
- SQL evaluation errors, side effects, user-defined functions, concurrency,
  ordering guarantees, distributed execution, and physical plans have not
  been certified.
- The eight L families are purposive synthetic test designs, not a
  representative workload sample, and are related to earlier aggregation
  research.
- No claim is made that any whole Analyzer or Optimizer RuleExecutor was
  proven correct in the general case.

The value of this round is now more precise: direct JVM counterexamples
exposed a measurement flaw; the corrected, fail-closed observer retained
conditional local certificates; and the first broader, prospectively frozen
cohort measured the rule-grammar coverage gap honestly.
