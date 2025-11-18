from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as _sum, count, to_timestamp,
    explode, map_keys, map_values, lit, current_timestamp, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType, IntegerType
import os
import time
from logging_utils import json_log

# ----------------------------
# Environment variables
# ----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
WEATHER_TOPIC = "weather_raw"
AIR_TOPIC = "air_raw"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = "climate"
RAW_WEATHER_TABLE = "weather_processed"
METRICS_TABLE = "climate_metrics"
METRICS_HOURLY_TABLE = "climate_metrics_hourly"
METRICS_DAILY_TABLE = "climate_metrics_daily"
CHECKPOINT_PATH_WEATHER = os.getenv("CHECKPOINT_PATH", "/opt/spark_checkpoints/weather_raw")
CHECKPOINT_PATH_METRICS = os.getenv("CHECKPOINT_PATH", "/opt/spark_checkpoints/climate_metrics")
CHECKPOINT_PATH_METRICS_HOURLY = os.getenv("CHECKPOINT_PATH_HOURLY", "/opt/spark_checkpoints/climate_metrics_hourly")
CHECKPOINT_PATH_METRICS_DAILY = os.getenv("CHECKPOINT_PATH_DAILY", "/opt/spark_checkpoints/climate_metrics_daily")
COMPONENT = "spark_processor"

# ----------------------------
# Spark session
# ----------------------------
spark = SparkSession.builder \
    .appName("ClimateProcessor") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
json_log(COMPONENT, "start", broker=KAFKA_BROKER, cassandra_host=CASSANDRA_HOST)

# ----------------------------
# Schemas
# ----------------------------
weather_schema = StructType([
    StructField("location", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", LongType(), True),
    StructField("wind_speed", DoubleType(), True)
])

air_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("location_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("measurements", MapType(StringType(), DoubleType()), True),
    StructField("sensor_count", IntegerType(), True)
])

# ----------------------------
# Read Weather Stream
# ----------------------------
weather_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", WEATHER_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

weather_df = weather_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp_utc")))

# ----------------------------
# Read Air Quality Stream
# ----------------------------
air_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", AIR_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

air_df = air_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), air_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp_utc"))) \
    .withColumn("pm_value", coalesce(
        col("measurements").getItem("sensor_688"),
        lit(0.0)
    ))

# ----------------------------
# Write raw weather data to Cassandra
# ----------------------------
def _write_batch_to_cassandra(df, table_name: str, batch_id: int):
    batch_start = time.time()
    json_log(COMPONENT, "cycle_begin", stream=table_name, batch_id=batch_id)
    rows = df.count()
    if rows > 0:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", table_name) \
            .mode("append") \
            .save()
        elapsed = round(time.time() - batch_start, 3)
        thr = round((rows / elapsed) if elapsed > 0 else 0.0, 3)
        json_log(COMPONENT, "send_success", stream=table_name, batch_id=batch_id, rows=rows, elapsed_sec=elapsed, throughput_rps=thr)
    else:
        elapsed = round(time.time() - batch_start, 3)
        json_log(COMPONENT, "send_success", stream=table_name, batch_id=batch_id, rows=0, elapsed_sec=elapsed, throughput_rps=0.0)
    json_log(COMPONENT, "cycle_complete", stream=table_name, batch_id=batch_id)


weather_raw_query = weather_df.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, RAW_WEATHER_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_WEATHER) \
    .start()

# ----------------------------
# Compute rolling aggregates
# For demo: using 1-minute windows to see results quickly
# For production: change to "7 days" with "1 day" slide
# ----------------------------
# Weather aggregations with watermark
weather_agg = weather_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        col("location"),
        window(col("event_time"), "1 minute", "30 seconds")
    ).agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("precipitation").alias("avg_precipitation"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("avg_humidity"),
        col("avg_wind_speed"),
        col("avg_precipitation"),
        lit(None).cast("double").alias("avg_pm_value"),  # Placeholder for air quality
        col("record_count")
    )

# Air quality aggregations with watermark
air_agg = air_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        col("location_name").alias("location"),
        window(col("event_time"), "1 minute", "30 seconds")
    ).agg(
        avg("pm_value").alias("avg_pm_value"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit(None).cast("double").alias("avg_temp"),  # Placeholder
        lit(None).cast("double").alias("avg_humidity"),  # Placeholder
        lit(None).cast("double").alias("avg_wind_speed"),  # Placeholder
        lit(None).cast("double").alias("avg_precipitation"),  # Placeholder
        col("avg_pm_value"),
        col("record_count")
    )

# ----------------------------
# Write weather metrics to Cassandra
# ----------------------------
weather_metrics_query = weather_agg.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS + "_weather") \
    .start()

# ----------------------------
# Write air quality metrics to Cassandra
# ----------------------------
air_metrics_query = air_agg.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS + "_air") \
    .start()

# ----------------------------
# Hourly aggregations (1 hour window, 5 minute slide)
# ----------------------------
weather_hourly = weather_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        col("location"),
        window(col("event_time"), "1 hour", "5 minutes")
    ).agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("precipitation").alias("avg_precipitation"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"), col("avg_humidity"), col("avg_wind_speed"), col("avg_precipitation"),
        lit(None).cast("double").alias("avg_pm_value"),
        col("record_count")
    )

air_hourly = air_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        col("location_name").alias("location"),
        window(col("event_time"), "1 hour", "5 minutes")
    ).agg(
        avg("pm_value").alias("avg_pm_value"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit(None).cast("double").alias("avg_temp"),
        lit(None).cast("double").alias("avg_humidity"),
        lit(None).cast("double").alias("avg_wind_speed"),
        lit(None).cast("double").alias("avg_precipitation"),
        col("avg_pm_value"),
        col("record_count")
    )

weather_hourly_query = weather_hourly.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_HOURLY_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS_HOURLY + "_weather") \
    .start()

air_hourly_query = air_hourly.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_HOURLY_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS_HOURLY + "_air") \
    .start()

# ----------------------------
# Daily aggregations (1 day window, 1 hour slide)
# ----------------------------
weather_daily = weather_df \
    .withWatermark("event_time", "1 hour") \
    .groupBy(
        col("location"),
        window(col("event_time"), "1 day", "1 hour")
    ).agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("precipitation").alias("avg_precipitation"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"), col("avg_humidity"), col("avg_wind_speed"), col("avg_precipitation"),
        lit(None).cast("double").alias("avg_pm_value"),
        col("record_count")
    )

air_daily = air_df \
    .withWatermark("event_time", "1 hour") \
    .groupBy(
        col("location_name").alias("location"),
        window(col("event_time"), "1 day", "1 hour")
    ).agg(
        avg("pm_value").alias("avg_pm_value"),
        count("*").alias("record_count")
    ).select(
        col("location"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit(None).cast("double").alias("avg_temp"),
        lit(None).cast("double").alias("avg_humidity"),
        lit(None).cast("double").alias("avg_wind_speed"),
        lit(None).cast("double").alias("avg_precipitation"),
        col("avg_pm_value"),
        col("record_count")
    )

weather_daily_query = weather_daily.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_DAILY_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS_DAILY + "_weather") \
    .start()

air_daily_query = air_daily.writeStream \
    .foreachBatch(lambda df, bid: _write_batch_to_cassandra(df, METRICS_DAILY_TABLE, bid)) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH_METRICS_DAILY + "_air") \
    .start()

json_log(COMPONENT, "producer_ready", sinks=[
    f"{CASSANDRA_KEYSPACE}.{RAW_WEATHER_TABLE}",
    f"{CASSANDRA_KEYSPACE}.{METRICS_TABLE}",
    f"{CASSANDRA_KEYSPACE}.{METRICS_HOURLY_TABLE}",
    f"{CASSANDRA_KEYSPACE}.{METRICS_DAILY_TABLE}"
])

# Keep queries running
weather_raw_query.awaitTermination()
