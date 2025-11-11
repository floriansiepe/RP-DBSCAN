#!/bin/bash
set -euo pipefail

# Usage: ./run_local.sh [dataset] [dim] [eps] [minPts] [numPartitions] [expDir] [out] [rho]
# Example: ./run_local.sh data/example.csv 2 0.5 5 4 exp out 0.01

if [ -z "${SPARK_HOME:-}" ]; then
  echo "ERROR: SPARK_HOME is not set. Export your Spark installation path, e.g. export SPARK_HOME=/path/to/spark"
  exit 1
fi

DATASET=${1:-data/example.csv}
DIM=${2:-2}
EPS=${3:-0.5}
MINPTS=${4:-5}
NUM_PARTITIONS=${5:-4}
EXP_DIR=${6:-exp}
OUT=${7:-out}
RHO=${8:-0.01}

echo "[LOCAL] Building project (maven)..."
mvn -q package -DskipTests

JAR=$(ls target/*-SNAPSHOT.jar 2>/dev/null | head -n 1 || true)
if [ -z "$JAR" ]; then
  echo "ERROR: built jar not found in target/. Check mvn package output."
  exit 1
fi

echo "[LOCAL] Running spark-submit (local[*])"
"$SPARK_HOME/bin/spark-submit" \
  --master spark://MacBook-Pro.local:7077 \
  --deploy-mode client \
  --class dm.kaist.main.MainDriver \
  --conf spark.driver.memory=32g \
  --conf spark.executor.memory=8g \
  "$JAR" \
  -i "$DATASET" -o "$OUT" -rho "$RHO" -dim "$DIM" -eps "$EPS" -minPts "$MINPTS" -np "$NUM_PARTITIONS" -M "$EXP_DIR"

EXIT_RC=$?
echo "[LOCAL] spark-submit exited with $EXIT_RC"
exit $EXIT_RC
