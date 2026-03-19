#!/bin/bash
# Run Spark MOR operations (creates delete.parquet files in MinIO)
#
# Prerequisites:
#   - Docker: MinIO + Hive metastore (cd docker && docker-compose up -d)
#   - Spark 3.5.x with Iceberg runtime
#   - Java 17 (recommended; Java 21+ needs -Djava.security.manager=allow)
#
# Usage:
#   ./scripts/run_spark_mor.sh
#   JAVA_HOME=/path/to/jdk-17 ./scripts/run_spark_mor.sh
#   SPARK_HOME=/path/to/spark ./scripts/run_spark_mor.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIRN_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$FIRN_ROOT"

# Java 17 recommended for Spark/Hadoop (avoids PrefetchingStatistics + Subject.getSubject issues)
# Set before running if you have multiple Java versions, e.g.:
#   sdk use java 17.0.17-amzn  OR  export JAVA_HOME=/path/to/jdk-17
if [[ -n "$JAVA_HOME" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
fi

# Spark home (configurable)
SPARK_HOME="${SPARK_HOME:-/home/dikshantsharma/Desktop/oss/emr-advisor-tool-guide/spark-3.5.3-bin-hadoop3}"
SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"

if [[ ! -x "$SPARK_SUBMIT" ]]; then
    echo "Error: spark-submit not found at $SPARK_SUBMIT"
    echo "Set SPARK_HOME or install Spark 3.5.x"
    exit 1
fi

# Optional: OpenMetadata lineage agent (omit if not needed)
EXTRA_JARS=""
if [[ -f "$SCRIPT_DIR/jars/openmetadata-spark-agent-1.1.jar" ]]; then
    EXTRA_JARS="--jars $SCRIPT_DIR/jars/openmetadata-spark-agent-1.1.jar"
fi

# hadoop-aws 3.3.4 to match Spark 3.5.3's bundled Hadoop 3.3.4 (3.3.6 causes PrefetchingStatistics NoClassDefFoundError)
# Fix: Java 17+ breaks Hadoop's Subject.getSubject() - add security manager opt-in
export SPARK_SUBMIT_OPTS="-Djava.security.manager=allow"

"$SPARK_SUBMIT" \
    --driver-memory 8g \
    --conf spark.driver.maxResultSize=8g \
    --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
    --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
    $EXTRA_JARS \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.25.61,software.amazon.awssdk:url-connection-client:2.25.61 \
    --conf spark.driver.host=127.0.0.1 \
    --conf spark.driver.bindAddress=127.0.0.1 \
    "$SCRIPT_DIR/spark_mor_operations.py"
