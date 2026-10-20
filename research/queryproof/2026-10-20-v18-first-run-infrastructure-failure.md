# QueryProof V18 first run — infrastructure failure (2026-10-20)

- Original first V18 CI run: https://github.com/mhatipog/spark/actions/runs/39483141760
- Job: 118836611307
- Workflow: .github/workflows/queryproof-native-v18-read-dependency-cache-pair.yml
- Source patch: tools/queryproof_posthoc_v18_20261020/instrument_tag_reads.py
- Witness file: tools/queryproof_posthoc_v18_20261020/QueryProofNativeV18RealReadDependency.scala
- Result: FAILED at the Java launch step. Source pinning / V18 patch and both Scala compilation steps passed.
- Fatal log: Error: Could not find or load main class org.apache.spark.sql.catalyst.optimizer.QueryProofNativeV18RealReadDependency; ClassNotFoundException.
- Cause: the Scala object in the committed file was QueryProofNativeV18StockOptimizerCacheJoin, while the workflow launch class was QueryProofNativeV18RealReadDependency. The experiment never executed; V18 first run has NO observational verdict (neither a proof nor a falsification of the tag-read hypothesis).
- Repair protocol: update the object name under an explicit new commit (V18B retry), reexecute CI, separately preserve actual native outcome. Do not rename first run or describe it as scientific failure.
