#!/bin/bash
set -euo pipefail

# Default envs (override in docker-compose)
: "${KAFKA_BROKER:=kafka:29092}"
: "${CASSANDRA_HOST:=cassandra}"
# Use CHECKPOINT_PATH consistently (processor.py reads CHECKPOINT_PATH)
: "${CHECKPOINT_PATH:=/opt/spark_checkpoints/climate_processor}"
: "${SPARK_DRIVER_MEMORY:=1g}"
: "${SPARK_EXECUTOR_MEMORY:=1g}"
: "${SPARK_MASTER:=local[2]}"
# Packages (adjust versions if your Spark/Scala combination is different)
: "${SPARK_PACKAGES:=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1}"

echo "Starting Spark job with:"
echo " KAFKA_BROKER=$KAFKA_BROKER"
echo " CASSANDRA_HOST=$CASSANDRA_HOST"
echo " CHECKPOINT_PATH=$CHECKPOINT_PATH"
echo " SPARK_PACKAGES=$SPARK_PACKAGES"

# Wait for Cassandra to be ready (optional)
echo "Waiting for Cassandra to be reachable on ${CASSANDRA_HOST}:9042..."
try=0
until nc -z "$CASSANDRA_HOST" 9042; do
  try=$((try+1))
  echo "  Cassandra not reachable yet (attempt $try). Sleeping 5s..."
  sleep 5
  if [ $try -gt 36 ]; then
    echo "Timed out waiting for Cassandra (approx 3 minutes). Continuing anyway..."
    break
  fi
done

# Ensure checkpoint path exists and is writable
mkdir -p "$CHECKPOINT_PATH"
# adjust ownership if running as root, but we run as non-root user in image; chown may fail if not root - ignore errors
if [ "$(id -u)" -eq 0 ]; then
  chown -R 1001:0 "$CHECKPOINT_PATH" || true
fi
chmod -R 770 "$(dirname "$CHECKPOINT_PATH")" || true

# Run spark-submit
exec /opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST}" \
  --conf spark.sql.shuffle.partitions=2 \
  --packages "${SPARK_PACKAGES}" \
  --conf spark.kryoserializer.buffer.max=512m \
  /opt/spark/app/processor.py
