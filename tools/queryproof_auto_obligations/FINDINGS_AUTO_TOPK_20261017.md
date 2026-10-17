# QueryProof — automated Catalyst TopK obligation and source provenance

**Date:** 2026-10-17. **Status:** Research prototype; successfully reproduced in public GitHub Actions. No upstream patch or new bug claimed.

## Reproducibility and chronology

- Initial read-only Catalyst shape inspection: [GitHub Actions 39062763977](https://github.com/mhatipog/spark/actions/runs/39062763977).
- Strict extraction scope and negative controls frozen in [FROZEN_STAGE_PROTOCOL.json](FROZEN_STAGE_PROTOCOL.json), commit [13c4292](https://github.com/mhatipog/spark/commit/13c4292aab1c66ffb2005a2ac2857bb0ebd7e7a1), before the source-stage extractor was implemented.
- First run [39063141465](https://github.com/mhatipog/spark/actions/runs/39063141465) established the positive pattern, but **failed** because the multiple-CTE negative test was malformed: it selected `id` from the `k` view, whose fields are `a` and `b`. The outcome was preserved and *must not* be counted as a successful negative test.
- The negative fixture was corrected from `SELECT id FROM k` to `SELECT a FROM k` in [6e14efb](https://github.com/mhatipog/spark/commit/6e14efb9b0f87060f0ccae30d81b0c61fbee7724), **without changing the extractor algorithm or any positive SQL case**.
- Final both-version successful run: [GitHub Actions 39063288040](https://github.com/mhatipog/spark/actions/runs/39063288040).

## What changed technically

The earlier top-k checker required the human to supply the input table `p`, output identity `name`, sorting key `age`, and the intended slice. The new `auto_topk.py` derives these **from the analyzed Catalyst logical tree**, without SQL regex, manual field names or an expected-answer table passed to the verifier.

It first requires exactly one resolvable `WithCTE` / `CTERelationDef` / `CTERelationRef`, a bare projected output identity traced through identical Catalyst `exprId` values, and a `GlobalLimit`/`LocalLimit` or `Offset` above a global `Sort` and direct source `View`. It accepts one integral, descending, non-null and **unique** sort key on a simple `LogicalRDD`, a unique source identity and integral source ID. It independently rereads the recovered named source view, verifies a **64-row cap**, and composes per-partition top-q summaries rather than evaluating an alternative full SQL query.

For an offset-only query without a limit, this prototype must retain the **entire small source** before slicing. It is not a bounded-memory solution for unlimited large inputs. A mutable view could also change between scans, so the current witness assumes a replayable immutable test source.

## Final outcome

Each version tested **48 positive executions**: 6 query projection classes × 2 slice types × AQE on/off × 2 repeats. All positive executions were successfully mapped to an automatic source-stage certificate, including all safe controls.

| Query projection type | Spark 4.0.1 violations / executions | Spark 4.2.0 violations / executions |
|---|---:|---:|
| Plain, deterministic, uncorrelated scalar, uncorrelated EXISTS (combined) | 0/32 | 0/32 |
| Correlated COUNT | 4/8 | 8/8 |
| Correlated EXISTS | 4/8 | 8/8 |
| **All correlated cases combined** | **8/16** | **16/16** |

All **12 negative-control executions**, six per Spark version, returned `UNKNOWN` with the intended abstention behavior: Filter in source, computed output identity, derived sort expression, tied sort keys, too many source rows, or multiple CTEs. No negative test was erroneously given a positive proof status in the corrected run. No positive case unexpectedly returned UNKNOWN, and no safe case triggered a membership violation.

Spark 4.0.1 produces the correlated bug for `LIMIT 3`, while the tested `OFFSET 3` controls return the expected members. Spark 4.2.0 produces the correlated membership discrepancy for both. These are related test variations of already reported upstream PR #59445, **not newly discovered independent Spark bugs**.

### Complementary previously achieved result

`plan_extractor.py` independently recovers root exposed `ORDER BY` obligations. The [successful prior run](https://github.com/mhatipog/spark/actions/runs/39062616527) extracted exactly matching keys in all 60 in-scope SQL executions across both versions, correctly abstained in eight unsupported runs, and detected six out of six repeated observations of known SPARK-59126 (only when AQE enabled).

## Publication and novelty assessment

**Engineering contribution: strong relative to our previous iteration. Academic novelty: not yet demonstrated.** Output-order monotonicity and compositional top-k algebra are classical. Automated extraction of these obligations from Spark's analyzed plan and explicit tri-state witnesses is potentially useful, but our selection extractor currently handles a very narrow **single-CTE, direct in-memory, unique integer-sort** shape. The 15 query families are independent *correctness controls*, not distinct optimizer faults. The membership results are an in-sample reproduction of one known fault family.

Neither SQLancer's DQP/CODDTest nor the 2026 DistSQL system was run as a comparison. The CI jobs demonstrate reproducibility, not superiority or broad detection rates.

**Next hard go/no-go experiment:** freeze real unseen Spark correctness issues, evaluate the provenance-aware method blind, measure UNKNOWN rate, sensitivity on independently verified wrong results, false accusations, and source scan overhead. Evaluate stronger plan equivalence oracles rather than assuming all errors manifest as sortedness/membership violations. In particular, handling arbitrary joins, filters, hidden sort keys, multi-CTE and non-unique keys without false accusations is unsolved.

## Links

- [Automatic final-output sort extractor](plan_extractor.py)
- [Automatic TopK source and selection extractor](auto_topk.py)
- [Two-version test harness](run_auto_topk.py)
- [Frozen protocol](FROZEN_STAGE_PROTOCOL.json)
- [Lightweight GitHub workflow](../../.github/workflows/queryproof-auto-obligations.yml)

All experiments used synthetic data. No Apache Spark upstream code or existing PR #59445 changed.
