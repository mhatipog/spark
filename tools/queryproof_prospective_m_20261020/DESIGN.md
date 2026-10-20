# Prospective adversarial M cohort (first-run freeze)

This 48-case corpus and its scoring protocol are committed **before any M execution** on the Spark 4.2.0 JVM. The local proof algorithms, core score functions, H/K/L cohorts, and known controls remain frozen.

- **24 target-enriched** cases: repeated grouping expressions, modulo aliases, reordering, filtering, and nested projections.
- **24 generalization challenges**: non-repeat controls, joins, left joins, UNION ALL, DISTINCT and HAVING. These are intentionally less likely to fall inside the narrow proven fragment; abstention is legitimate.
- Exactly four deterministic data instances per query family, with duplicates, NULLs, 17 left rows and 13 right rows, per-case separate tables.
- The LCG rule and seed are frozen in `CASES.json`; all values are synthetic. No raw personal data or tenant content is used.
- The primary output must **separate** structurally certified changed suffixes, tag-shape eligibility, real JVM input-identity eligibility, and all-rule completeness; it must not report their minimum as an intersection unless explicitly proven.
- A 0/48 proof count or mismatch is *scientifically valid*. Do not adjust the corpus, parser, or verifier after viewing its first outcome and still call the revision prospective.
- Spark/SQLite oracle equality tests only finite row bags. A positive finite result does not establish arbitrary input equivalence.
- All H/K/L results and their prior biases remain reported separately. Generalization beyond these 12 families cannot be claimed.

The frozen verifier baseline before these cases is commit `40bf2fbab64c717c72ec630566bb5c8cf705c2a4`. Any later M changes must start a separate cohort designation.
