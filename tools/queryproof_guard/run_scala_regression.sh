#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/queryproof_scala_regression_classes
JARS=$(python - <<'PY'
import os,pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
PY
)
LIB=$(find "$JARS" -maxdepth 1 -name 'scala-library-2.13.*.jar' | head -n 1)
test -n "$LIB"
VERSION=$(basename "$LIB" | sed 's/^scala-library-//;s/\.jar$//')
for lib in scala-reflect scala-compiler; do
 test -f "$JARS/$lib-$VERSION.jar" || curl --retry 3 -fsSL \
  "https://repo.maven.apache.org/maven2/org/scala-lang/$lib/$VERSION/$lib-$VERSION.jar" \
  -o "$JARS/$lib-$VERSION.jar"
done
java -Xmx3g -cp "$JARS/*" scala.tools.nsc.Main -usejavacp \
 -classpath "$JARS/*" -d artifacts/queryproof_scala_regression_classes \
 tools/queryproof_guard/AqEOneRowOrderingRegression.scala
java -Xmx3g --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
 -cp "artifacts/queryproof_scala_regression_classes:$JARS/*" \
 org.queryproof.AqEOneRowOrderingRegression
