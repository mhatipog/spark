#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/queryproof_guard_classes
JARS=$(python - <<'PY'
import os,pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
PY
)
LIB=$(find "$JARS" -maxdepth 1 -name 'scala-library-2.13.*.jar' | head -n 1)
test -n "$LIB"
VERSION=$(basename "$LIB" | sed 's/^scala-library-//;s/\.jar$//')
for lib in scala-reflect scala-compiler; do
 if test ! -f "$JARS/$lib-$VERSION.jar"; then
   curl --retry 3 -fsSL \
     "https://repo.maven.apache.org/maven2/org/scala-lang/$lib/$VERSION/$lib-$VERSION.jar" \
     -o "$JARS/$lib-$VERSION.jar"
 fi
done
java -Xmx3g -cp "$JARS/*" scala.tools.nsc.Main -usejavacp \
 -classpath "$JARS/*" -d artifacts/queryproof_guard_classes \
 tools/queryproof_guard/OptimizeOneRowPlan.scala
CATALYST=$(find "$JARS" -maxdepth 1 -name 'spark-catalyst_2.13-*.jar' | head -n 1)
test -n "$CATALYST"
cp "$CATALYST" "$CATALYST.unmodified"
jar uf "$CATALYST" -C artifacts/queryproof_guard_classes .
jar tf "$CATALYST" | grep -F 'org/apache/spark/sql/catalyst/optimizer/OptimizeOneRowPlan'
echo QUERYPROOF_EXPERIMENTAL_GUARD_PATCH_LOADED
