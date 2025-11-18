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

# create checkpoint dir
mkdir -p "$SPARK_CHECKPOINT"
chown -R 1001:0 "$SPARK_CHECKPOINT" || true

# packages (Kafka + Cassandra connector)
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"

# Run spark-submit
exec spark-submit \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST}" \
  --conf spark.sql.shuffle.partitions=2 \
  --packages "${PACKAGES}" \
  --conf spark.kryoserializer.buffer.max=512m \
  /opt/spark/app/processor.py
