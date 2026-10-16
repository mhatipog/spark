#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/queryproof_ordering_classes
JARS=$(python - <<'PY'
import os,pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
PY
)
LIB=$(find "$JARS" -maxdepth 1 -name 'scala-library-2.13.*.jar' | head -n 1)
test -n "$LIB" || { echo "Spark Scala 2.13 library missing"; exit 1; }
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
  -classpath "$JARS/*" -d artifacts/queryproof_ordering_classes \
  tools/queryproof_physical/PhysicalOrderingProbe.scala
jar cf artifacts/queryproof-ordering-probe.jar -C artifacts/queryproof_ordering_classes .
jar tf artifacts/queryproof-ordering-probe.jar | grep 'org/queryproof/PhysicalOrderingProbe'
echo QUERYPROOF_EXTERNAL_PROBE_COMPILED
