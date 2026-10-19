# QueryProof independent adversarial SQL cohort — preregistered 2026-10-19

Purpose: probe false-positive risk and abstention in the *unchanged*
literal-aware/Sort-aware contextual INNER JOIN guard theorem after nine
prospective certifications in the separate DuckDB sample.

Source: original DuckDB SQL tests at pinned commit
[`0b272c23b19e`](https://github.com/duckdb/duckdb/commit/0b272c23b19eb7747313fffa0a1097f720905b29).

- `test/sql/join/inner/test_join_is_not_distinct.test`, query blocks
  **3, 4, 14, 15, 16, 17**. First two use original four-row duplicate
  `tbl` state after second INSERT, next four the exact seven-row
  `tbl_null` state including multiple NULLs.
- `test/sql/join/left_outer/test_left_outer.test`, query blocks
  **1–9 and 11–13**. Skip 10 because it repeats query 5 exactly.
  Use original `integers` and `integers2` data unchanged.

The 18 SELECT texts are copied verbatim, without rewritten predicates or
injected optimizer-friendly expressions. The file-selection rationale is
adversarial coverage of duplicates, NULL-safe equality, ordinary equality,
LEFT and RIGHT OUTER joins, disjunctions and nullable ON clauses. It was
locked before any run on this specific cohort.

## Expected and forbidden conclusions

- Valid INNER JOIN transitions *may* be certified if their exact 3VL
  implication is proven, including a nullable input. Zero certificates is
  legitimate.
- LEFT/RIGHT OUTER **Catalyst AST** joins must never be treated as INNER
  directly; an SQL left join can legitimately be transformed into an INNER
  join *by an earlier rule* if later conditions reject NULL-extended rows.
- A recognized and identical Sort frame preserves only bag semantics, not
  a promise about ordering/ties.
- Full result-row multiset hashes are compared to SQLite, with NULL and
  duplicate multiplicities retained. No finite oracle is claimed to prove
  semantic equivalence for every database.
- Old five-checker union remains unchanged. This separate cohort evaluates
  new contextual theorem prospectively; no edits to the certificate itself
  after its earlier independent DuckDB validation.

All results, including any SQL-dialect errors, must be reported. Revisions
after the first run convert this cohort into retrospective development.
