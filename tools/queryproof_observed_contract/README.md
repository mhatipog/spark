# QueryProof — observable ORDER BY contract, prospective test (2026-10-17)

## Relationship to previous work
This is a separate **research branch**. No Apache Spark upstream change and no change to PR #59445. Do not reinterpret the known SPARK-59126 reproducer as a newly discovered bug.

Prior QueryProof's Catalyst structural ranking flagged **0/3** independently reproduced bugs; its physical metadata inspector detected missing declared ordering on the known AQE bug but correctly returned **UNKNOWN**, not proof of wrong rows. The previous narrower Sort guard also passed the corpus, but an already existing upstream `LogicalQueryStage.maxRows` repair passed as well. We will not submit a competing patch on that evidence.

## New, mathematically simple necessary condition
Given explicitly exposed ORDER BY keys and the corresponding Spark integral-key/null-order comparator, any observable result sequence `r_1,...,r_n` must satisfy `K(r_i) <= K(r_(i+1))` for every adjacent pair. This is equivalent to lexicographic monotonicity of that *observed sequence*. A violating pair is a concrete result-order defect, independent of the optimizer's claimed `outputOrdering`.

The checker is **not** a SQL equivalence oracle: it does **not** verify row bags, LIMIT/OFFSET membership, semantics of subqueries, tie stability (which SQL does not demand), or computation correctness. It supports only integral keys exposed in result columns and explicitly specified ASC/DESC, NULLS FIRST/LAST policies. Hidden ordering keys, unsupported value types and queries without a global observable ORDER BY are **UNKNOWN**. It does not parse SQL.

Evaluation outcomes:
- **CONTRACT_VIOLATED:** an actual adjacent descending pair, which is a necessary-condition failure under the supplied key specification.
- **NO_VIOLATION_OBSERVED:** observed ordering necessary condition holds. This is **not** PASS and does not establish overall SQL correctness.
- **UNKNOWN:** unsupported/hidden/no global keys; cannot infer compliance.

The comparator uses O(n) time and O(1) extra memory on an iterable. The experiment itself uses Spark `DataFrame.collect()` to observe the client's actual returned sequence, which materializes O(n) rows; the O(1) statement is **only about the checker**, not the collection procedure.

## Precommitted design
- `manifest.json` frozen at commit [fc8508d](https://github.com/mhatipog/spark/commit/fc8508de575986defbd2c83d1ddd4a23d669fcbc), **before** the harness existed, 17 SQL families/controls.
- Distinct shapes: range multi-key sort; null policies, tied keys, LIMIT/OFFSET; UNION; GROUP BY; all four major join forms; window, DISTINCT, nested subquery; and two intentionally unsupported negative controls.
- Independently known SPARK-59126 reproduced **separately** as calibration, not counted as out-of-family discovery.
- Run the same manifest with AQE enabled and disabled on unmodified published Spark 4.0.1 and 4.2.0; preserve per-case JSON and any errors.
- Pure stdlib mutation tests verify that each adjacent swap of a strictly sorted integer sequence is detected. Duplicate/tied keys must not lead to a false violation.

## Predeclared interpretation
If a held-out SQL family yields CONTRACT_VIOLATED, reproduce independently without the verifier, shrink the query, investigate its plan, and check upstream issue history before claiming anything new. If all holdouts return NO_VIOLATION_OBSERVED or UNKNOWN as expected, the experiment demonstrates only that this narrow observable contract has few false alarms on these controls; it **does not** establish a general query verifier or provide a scientifically independent bug detection rate.

Comparator work such as SQLancer (PQS/NoREC/TLP/DQP) and the 2026 DistSQL paper is relevant but **has not** been run as an external baseline. The next research question is whether this observable semantic witness adds useful discrimination to the existing metadata inspector while maintaining zero false accusations and acceptable overhead on independently frozen *unseen* bug families.

## Running without upstream build
```bash
python -m unittest discover -s tools/queryproof_observed_contract -p 'test_contract.py' -v
python -m pip install 'pyspark==4.2.0'
python tools/queryproof_observed_contract/run.py
```

The dedicated GitHub Actions workflow runs both released wheel versions. On **this isolated branch only**, the inherited full-Spark `build_main.yml` workflow ignores changes to `tools/queryproof_observed_contract/**` and the dedicated workflow file. This avoids running the expensive upstream build on a standalone research harness; do not merge this override upstream.
