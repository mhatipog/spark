# QueryProof: automatic analyzed-plan contracts (first prototype)

**Purpose:** replace manually supplied output-column ordering specifications with a conservative extraction from Spark's *analyzed* logical plan, using unmodified PySpark 4.0.1 and 4.2.0 wheels.

**This is not an independent SQL correctness proof.** It is a small, intentionally incomplete contract compiler. It cannot infer a trusted pre-LIMIT input relation for the top-k membership witness yet.

## Semantics
1. Start at the analyzed plan root and traverse only known order-preserving wrapper nodes: WithCTE, LocalLimit, GlobalLimit, Offset, SubqueryAlias and Project. Record encountered slice nodes but do not combine them into a claimed source-stage selection certificate.
2. Require a result-wide global Sort in the permitted path. Resolve every sort key through direct integral AttributeReference lineage to an *exposed output column* (Project permits direct attribute pass-through and aliasing only).
3. Convert Spark's sort direction and null policy to the comparator used by our existing pure-Python adjacent-order witness. *Never* invent key indices based on SQL text, attribute names, or positional guesses.
4. If any key is hidden, computed, unsupported, or the plan has an unsupported root operator, emit UNKNOWN with a reason. Numeric floating-point keys are deliberately excluded to avoid NaN semantics assumptions.
5. Keep the handwritten frozen manifest as a **separate evaluator** of key-index correctness: it is not read by the extractor.

## Development benchmark (not fresh holdout)
Reuse the previously frozen 17 SQL families and the known AQE global-order bug calibration, plus seven deliberately adversarial additional development controls (hidden keys, non-global Sort, string order, aliases, filters and OFFSET).
Run with AQE on/off, official Spark 4.0.1/4.2.0, producing full JSON with extraction metadata and witnessed results. A wrong automatic exposed key assignment or unsafe claim of a global sort is an immediate failure; inability to extract a safe contract is reported as UNKNOWN.

These SQL families were previously inspected during design. High coverage would be **engineering progress**, not evidence of generality. A later genuinely unseen benchmark is mandatory for publication.

## Experimental risk
Spark's analyzed-plan API and expression shapes may differ across releases; probe failures must be reported and addressed explicitly, not silently interpreted as correctness. Even an exact extracted observable order only covers a necessary condition. The existing TopK witness remains manually sourced and adds an independent scan; no automatic pre-transformation source certification is claimed.

## Reproduce
The scoped GitHub Actions workflow uses untouched official wheels, checks the published frozen test suite without rebuilding Spark, and publishes JSON logs/artifacts. It does not change the existing upstream Spark PR #59445.
