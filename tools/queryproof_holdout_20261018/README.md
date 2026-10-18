# QueryProof: frozen Filter / INNER JOIN Catalyst holdout

This is a **pre-execution** benchmark, not a claim of a newly verified Spark
optimizer bug. The cases and scoring rules were committed on 2026-10-18 before
running this benchmark through instrumented Catalyst.

## Frozen design

- [CASES.json](CASES.json): 18 fixed SQL statements over two nullable-integer
  fixtures; 12 Filter, five INNER JOIN, and one Project negative control.
- [FROZEN_PROTOCOL.json](FROZEN_PROTOCOL.json): frozen eligibility, denominators,
  exclusions, independent checks, and interpretation limits.
- [run_holdout.py](run_holdout.py): runs unmodified Spark SQL 4.2.0 (except the
  existing RuleExecutor trace instrumentation), compares results against SQLite,
  decodes actual before/after ASTs, applies the unchanged semantic checker, and
  writes a sanitized JSON scoreboard.
- [test_holdout.py](test_holdout.py): offline tests of case validity, multiset
  comparison, trace decoding, known-family separation, and fail-closed behavior.

The only rule identities excluded from the **primary unseen-rule metric** are
`OptimizeJoinCondition` and `RewritePredicateSubquery`. Their traces remain
in the all-event denominator. No rule name is input to the certificate checker.

## Reproduction on a disposable trusted hosted machine

From a checkout of `research/queryproof-filter-holdout-20261018`, with Python
3.11, Java 17+, `java`, `jar`, and network access for Maven compiler jars:

```bash
python3 -m venv /tmp/queryproof-holdout-venv
source /tmp/queryproof-holdout-venv/bin/activate
python3 -m pip install --disable-pip-version-check pyspark==4.2.0

python3 -m unittest discover -s tools/queryproof_context_validator \
  -p 'test_equality_orbits.py' -v
python3 -m unittest discover -s tools/queryproof_holdout_20261018 \
  -p 'test_holdout.py' -v

# Patches only the disposable PySpark wheel's Catalyst JAR.
bash tools/queryproof_instrumented/build_overlay.sh

python3 tools/queryproof_holdout_20261018/run_holdout.py
```

Expected summary path:
`artifacts/queryproof_holdout_20261018.json`.

The runner launches PySpark in a child process and captures its entire JVM
stderr in a private temporary file. That file, including base64-encoded
Catalyst plan JSON, is destroyed when scoring finishes. Only sanitized counts,
case IDs, proof witness values, and output multiset hashes are persisted. Never
upload or publicly paste a raw Catalyst trace; real plan JSON can contain
query literals or other sensitive data.

**Do not attach the public Spark fork to the Oracle VM's private repository
self-hosted runners.** That VM's policy forbids running untrusted public-fork
code. Execution there requires separate owner approval and an independently
reviewed, pinned, owner-controlled private harness. If hosted Actions quota is
unavailable, defer real Spark execution; this offline validation is not a
substitute for it.

## Scientific interpretation

The orbit checker proves equivalence for a narrow typed fragment: nullable
integral attributes, equality, null-safe equality, AND/OR/NOT, and null tests.
Its proof enumerates Bell(n+1) equality/nullness orbits and independently
replays them through SQLite. It does **not** prove arbitrary Spark optimizer
transformations. It fails closed if a rule changes the plan topology,
child plans, predicate operator types, metadata, or unsupported expressions.

For Filter and INNER JOIN conditions, preserving the TRUE set preserves
row selection within the unchanged consumer. The local subexpression guard
can be stricter than what the surrounding Boolean context actually needs.

A `COUNTEREXAMPLE` reports an expression valuation in this fragment. It is
not a Spark bug unless a reachable source-row fixture demonstrates a whole
query mismatch. Conversely, a Spark/SQLite match for the 18 fixed queries is
not an exhaustive correctness proof.

**Execution status at freeze:** offline verifier and transport tests pass.
No instrumented Spark holdout execution or out-of-sample generalization claim.
