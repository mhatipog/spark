#!/usr/bin/env bash
# Patch the installed (disposable, CI-only) PySpark spark-sql JAR with the SPARK-57956
# fix from apache/spark master (commit 5623395102): LogicalQueryStage.maxRows trusts
# rowCount only for materialized runtime statistics, else falls back to
# logicalPlan.maxRows. Used to test whether a wrong result is the SPARK-57956 root cause.
set -euo pipefail
JARS=$(python -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')
SQL=$(find "$JARS" -maxdepth 1 -name 'spark-sql_2.13-*.jar' | head -n1)
VER=$(basename "$SQL" | sed -e 's/^spark-sql_2.13-//' -e 's/\.jar$//')
LIB=$(find "$JARS" -maxdepth 1 -name "scala-library-2.13.*.jar" | head -n1)
SV=$(basename "$LIB" | sed -e 's/^scala-library-//' -e 's/\.jar$//')
for lib in scala-reflect scala-compiler; do
  test -f "$JARS/$lib-$SV.jar" || curl --retry 3 -fsSL \
    "https://repo.maven.apache.org/maven2/org/scala-lang/$lib/$SV/$lib-$SV.jar" -o "$JARS/$lib-$SV.jar"
done
mkdir -p lqs/src lqs/classes
curl --retry 3 -fsSL "https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql_2.13/$VER/spark-sql_2.13-$VER-sources.jar" -o lqs/sources.jar
(cd lqs/src && unzip -q -o ../sources.jar org/apache/spark/sql/execution/adaptive/LogicalQueryStage.scala)
F=lqs/src/org/apache/spark/sql/execution/adaptive/LogicalQueryStage.scala
python - "$F" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = "  override def maxRows: Option[Long] = stats.rowCount.map(_.min(Long.MaxValue).toLong)\n"
new = ("  override def maxRows: Option[Long] = {\n"
       "    if (isMaterialized && stats.isRuntime) {\n"
       "      stats.rowCount.map(_.min(Long.MaxValue).toLong)\n"
       "    } else {\n"
       "      logicalPlan.maxRows\n"
       "    }\n"
       "  }\n")
assert s.count(old) == 1, "maxRows definition not found (already patched?)"
open(p, "w").write(s.replace(old, new))
PY
java -Xmx3g -cp "$JARS/*" scala.tools.nsc.Main -usejavacp -classpath "$JARS/*" -d lqs/classes "$F"
jar uf "$SQL" -C lqs/classes .
echo "QUERYPROOF_LQS_PATCHED spark-sql $VER"
