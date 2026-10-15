#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/queryproof_classes
JARS=$(python - <<'PY'
import os,pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
PY
)
LIB=$(find "$JARS" -maxdepth 1 -name "scala-library-2.13.*.jar" | head -n1)
test -n "$LIB"
VERSION=$(basename "$LIB" | sed -e 's/^scala-library-//' -e 's/\.jar$//')
for lib in scala-reflect scala-compiler; do
  if test ! -f "$JARS/$lib-$VERSION.jar"; then
    curl --retry 3 -fsSL \
      "https://repo.maven.apache.org/maven2/org/scala-lang/$lib/$VERSION/$lib-$VERSION.jar" \
      -o "$JARS/$lib-$VERSION.jar"
  fi
done
echo "QUERYPROOF_SCALA_VERSION=$VERSION"
java -Xmx3g -cp "$JARS/*" scala.tools.nsc.Main -usejavacp \
  -classpath "$JARS/*" -d artifacts/queryproof_classes \
  tools/queryproof_instrumented/RuleExecutor.scala
jar cf artifacts/queryproof-spark-overlay.jar -C artifacts/queryproof_classes .
jar tf artifacts/queryproof-spark-overlay.jar |
  grep -F 'org/apache/spark/sql/catalyst/rules/RuleExecutor.class'
jar tf artifacts/queryproof-spark-overlay.jar |
  grep -F 'org/apache/spark/sql/catalyst/rules/QueryProofAstTrace'
# CI-only: patch disposable PySpark wheel JAR (not user source/repository).
CATALYST=$(find "$JARS" -maxdepth 1 -name 'spark-catalyst_2.13-*.jar' | head -n 1)
if test -z "$CATALYST"; then echo "spark-catalyst JAR missing" >&2; exit 1; fi
cp "$CATALYST" "${CATALYST}.queryproof-original"
jar uf "$CATALYST" -C artifacts/queryproof_classes .
echo "QUERYPROOF_CATALYST_PATCHED=$(basename "$CATALYST")"
echo "QUERYPROOF_OVERLAY_COMPILED"
