# QueryProof — V16: kernel-decided IFFC filter-merge and PushDownPredicates (21 October 2026)

Successor round 3. W32 is historical development data. The 8 V16 probes are
*development* queries written while building V16. Neither is a held-out cohort.
Frozen W32 16/32 and X 0/16 are unchanged.

## Question

Does the V15 approach (Coq decides real before/after Catalyst plan terms,
Python only prints) extend beyond guard insertion: to IFFC's Filter-merge
form and to a second rule, PushDownPredicates?

## Mathematical core

In SQL 3VL a WHERE/ON keeps a row iff **every conjunct is TRUE**
(`and3_is_true`, `conj_true`). Hence:

- `filter_equiv c c'`: conditions with the same conjunct set, plus any
  `IsNotNull(x)` where x is null-rejected by `c`, select the same rows on
  every row (`filter_equiv_sound`). This one lemma covers IFFC Filter-merge
  (`Filter(c)` → `Filter(g AND c)`), CombineFilters (with deduplication)
  and conjunct reordering.
- Pushdown is conjunct redistribution, valid when each moved conjunct only
  reads attributes of the input it moves to (`eval_agree`, `lookup_app_in` /
  `lookup_app_notin`, `lookup_normalize`).

## What was built

`research/queryproof/proofs/QueryProofDeepRewrite.v` (imports V15 unchanged)
adds `check2` and `check2_sound`. `Print Assumptions` reports **Closed under the
global context**. The proof is by induction on plan size, because the rules
recurse into grandchildren. Certified rewrite forms:

| Form | Side conditions decided by the kernel |
| --- | --- |
| IFFC join-input guards (V15) | guard attribute in the input's output and null-rejected by the unchanged ON; never on the LEFT OUTER preserved side |
| IFFC Filter-merge | `filter_equiv` |
| CombineFilters | `filter_equiv (c2 AND c) c'` |
| Push through attribute-only Project, optionally combined with the filter below | refs(c) ⊆ ids ⊆ child output |
| Push through INNER join (WHERE ∪ ON redistributed to left/right/ON) | conjunct-set equality; left refs ⊆ left output; right refs ⊆ right output and disjoint from left output |
| Push into LEFT OUTER preserved side, remainder kept above | conjunct-set equality; pushed refs ⊆ left output |

There are 14 kernel-evaluated controls. They include rejection of a right-side
push into LEFT OUTER, of a dropped conjunct, of a wrong-side push, and of a
merged unimplied guard.

Tooling: `run_v15.py` gained `--deps/--checker/--sound` (its defaults keep
V15) and push mutant families (`drop_pushed_filter`, `unimplied_conjunct`,
`wrong_filter_attribute`). `tools/queryproof_v16_rewrite_dev_20261021/DEV_CASES.json`
holds the 8 probes. The workflow is `.github/workflows/queryproof-v16-deep-rewrite.yml`.

## Negative control caught a printer fail-open

On the probes, `frame_edit` mutants were first **accepted**: the printer
translated attribute-only Project nodes while ignoring unknown JSON
fields, so the injected field vanished. The kernel was right about the terms
it received. The trusted printer had dropped information. Fix: an interpreted
Project now fails closed on any unexpected field (Filter and Join already did).
Afterwards those mutants are UNKNOWN (not accepted). This is exactly the
printer-trust risk named in V15, and it shows the mutants exercise the printer
as well as the kernel.

## Results

CI run 39608992463 (commit `b11fca17ad`, Coq 8.18.0, pyspark 4.2.0), identical
to the local run. Run 39608913502 (`cd0ad1cbef`) ran with V15 defaults because
of a workflow-generation slip. It is on record and superseded. The V15 workflow
re-ran green with the hardened printer (39608913368).

| | W32 (32 queries) | V16 development probes (8 queries) |
| --- | --- | --- |
| Effective rule events / printed | 308 / 104 | 60 / 29 |
| Kernel-accepted IFFC | 28 / 28 | 8 / 8 (incl. Filter-merge, no-join case) |
| Kernel-accepted PushDownPredicates | **28 / 28** (V15: 0) | **13 / 13** |
| Printed events of other rules | 48, all rejected | 8, all rejected (ColumnPruning, EliminateOuterJoin, RemoveNoopOperators) |
| Adversarial mutants | **264 / 264 rejected** | **45 / 45 rejected**, plus 21 frame edits UNKNOWN at the printer |

The probes exercise each new form on real Spark output: CombineFilters (D06);
push through a Project fused with CombineFilters (D01, D02, D07); WHERE to
left/right/ON across an INNER join (D02); OR kept in ON (D04); cross join
WHERE into ON (D05); LEFT OUTER preserved-side push (D03); IFFC
Filter-merge without a join (D06, D08).

## Proof status

1. **Kernel-proved:** `check2_sound`, axiom-free, for all databases and all
   opaque-operator interpretations.
2. **Kernel-decided:** every verdict above (309 mutants, 133 printed events).
3. **Trusted:** the printer (now fail-closed on Filter/Join/Project fields);
   evaluator agreement on integral comparisons, IsNotNull, And and Or;
   opaque identity; JSON fidelity.
4. **Not covered:** tags, caches, control flow, event omission, and composition
   across the suffix. Also uncovered forms: push through aliased Project,
   Aggregate or Union; non-IsNotNull inferred constraints (e.g. `a = 1 AND a = b ⇒ b = 1`);
   LEFT OUTER push of ON-clause conjuncts into the right side.
5. **Prospective:** none yet.

## Next step

The held-out cohort, now covering two rules. Freeze seeds, query
templates (operators, join kinds, WHERE shapes with NULL-rich data), the
acceptance definitions and the commit SHA before running. Report coverage and
mutant rejection separately per rule, and treat every UNKNOWN as not accepted.
