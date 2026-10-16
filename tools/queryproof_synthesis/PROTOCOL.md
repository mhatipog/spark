# QueryProof: generating and shrinking real Spark ORDER BY counterexamples

The 61-case candidate manifest was committed *before* the runner: commit c065ab1c86a67558cf12f66fcb99c200d877a8f1.
There is 1 known calibration, 48 generated perturbations, and 12 alternative shape controls.
All are variations of ONE KNOWN SPARK-59126 issue family: this is not a new bug and is not a blind general SQL test.

The generator never reads a stored expected Spark output. It recognizes an ordering inversion using the final SQL ORDER BY top.id ASC, rhs.bid ASC; the keys (left id, right id) are unique in its grammar. A separately implemented pure-Python left/inner-join model verifies row bag equality. If the bag is different, it records that as a separate failure, not as an ordering failure.

The generated data includes different range sizes, offsets, limits, join types, modulus join keys, partition counts, input shifts, and WITH CTE vs nested subquery SQL syntax. LIMIT may exceed the number of rows remaining after OFFSET; this is valid SQL and the reference handles it.

Generated witness minimization uses a bounded greedy shrinker, reexecuting proposed smaller inputs twice before accepting them. The final witness is replayed three times, and three counterfactual configurations check AQE off, InlineCTE enabled, and both broadcast thresholds disabled.

Spark's outputOrdering inspector is provided by the out-of-tree external Scala probe. It only reports Spark-declared planning properties, not an independent proof of physical correctness.

We must report every invalid/error case and failures of minimization. No claim of general or new SQL-bug discovery is warranted without a prospectively frozen different bug family and a compiler-based obligation extractor. No customer data or existing upstream PR is touched.
