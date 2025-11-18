#!/bin/bash
set -euo pipefail

# Default envs (override in docker-compose)
: "${KAFKA_BROKER:=kafka:29092}"
: "${CASSANDRA_HOST:=cassandra}"
: "${SPARK_CHECKPOINT:=/tmp/spark_checkpoints/climate_processor}"
: "${SPARK_DRIVER_MEMORY:=1g}"
: "${SPARK_EXECUTOR_MEMORY:=1g}"
: "${SPARK_MASTER:=local[2]}"

echo "Starting Spark job with:"
echo " KAFKA_BROKER=$KAFKA_BROKER"
echo " CASSANDRA_HOST=$CASSANDRA_HOST"
echo " CHECKPOINT=$SPARK_CHECKPOINT"

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
until nc -z "$CASSANDRA_HOST" 9042; do
  echo "Cassandra is unavailable - sleeping"
  sleep 5
done
echo "Cassandra is up - waiting 10 more seconds for full initialization"
sleep 10

# create checkpoint dir
mkdir -p "$SPARK_CHECKPOINT"

# packages (Kafka + Cassandra connector) - compatible with Spark 3.5.x
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

# Run spark-submit
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST}" \
  --conf spark.sql.shuffle.partitions=2 \
  --packages "${PACKAGES}" \
  --conf spark.kryoserializer.buffer.max=512m \
  /opt/spark/app/processor.py
