#!/usr/bin/env bash
set -euo pipefail
MODE="$1"
mkdir -p artifacts
JARS=$(python - <<'PY'
import os,pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
PY
)
if [[ "$MODE" == "baseline" ]]; then
  echo "QUERYPROOF_INTERVENTION BASELINE untouched published wheel"
  exit 0
fi
if [[ "$MODE" == "sort_guard" ]]; then
  bash tools/queryproof_guard/build_patch.sh
  echo "QUERYPROOF_INTERVENTION SORT_GUARD"
  exit 0
fi
if [[ "$MODE" != "upstream_cardinality" ]]; then
  echo "Unknown intervention: $MODE" >&2
  exit 2
fi
SOURCE=sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/LogicalQueryStage.scala
grep -q 'isMaterialized && stats.isRuntime' "$SOURCE" || {
  echo 'Upstream SPARK-57956 source fix not found, aborting' >&2; exit 3;
}
LIB=$(find "$JARS" -maxdepth 1 -name 'scala-library-2.13.*.jar' | head -n 1)
VERSION=$(basename "$LIB" | sed 's/^scala-library-//;s/\.jar$//')
for lib in scala-reflect scala-compiler; do
  if [[ ! -f "$JARS/$lib-$VERSION.jar" ]]; then
    curl --retry 3 -fsSL "https://repo.maven.apache.org/maven2/org/scala-lang/$lib/$VERSION/$lib-$VERSION.jar" -o "$JARS/$lib-$VERSION.jar"
  fi
done
mkdir -p artifacts/upstream_cardinality_classes
java -Xmx3g -cp "$JARS/*" scala.tools.nsc.Main -usejavacp \
  -classpath "$JARS/*" -d artifacts/upstream_cardinality_classes "$SOURCE"
JAR=$(find "$JARS" -maxdepth 1 -name 'spark-sql_2.13-4.2.0.jar' | head -n 1)
test -n "$JAR"
BEFORE=$(unzip -p "$JAR" org/apache/spark/sql/execution/adaptive/LogicalQueryStage.class | sha256sum | awk '{print $1}')
cp "$JAR" artifacts/spark-sql-original.jar
jar uf "$JAR" -C artifacts/upstream_cardinality_classes org/apache/spark/sql/execution/adaptive
AFTER=$(unzip -p "$JAR" org/apache/spark/sql/execution/adaptive/LogicalQueryStage.class | sha256sum | awk '{print $1}')
if [[ "$BEFORE" == "$AFTER" ]]; then
  echo 'Expected compiled class hash to change but did not' >&2; exit 4
fi
printf 'QUERYPROOF_UPSTREAM_CLASS_PATCH before=%s after=%s\n' "$BEFORE" "$AFTER"
echo "QUERYPROOF_INTERVENTION UPSTREAM_CARDINALITY SPARK-57956 from current Apache master"
