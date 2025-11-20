#!/usr/bin/env python3
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, to_timestamp, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import requests
# Spark session
spark = SparkSession.builder \
    .appName("ClimateProcessor") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

air_quality_schema = StructType([
    StructField("location", StringType(), True),
    StructField("parameter", StringType(), True),  # pm25, pm10, o3, no2, so2, co
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
])

# Minimal schema for avg_7d memory table (loc + aggregates)
avg_7d_schema = StructType([
    StructField("window", StructType([
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True)
    ]), True),
    StructField("loc", StringType(), True),
    StructField("temp_7d_avg", DoubleType(), True),
    StructField("humidity_7d_avg", DoubleType(), True)
])

post_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("upvotes", LongType(), True),
    StructField("downvotes", LongType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("timestamp", LongType(), True),
    StructField("permalink", StringType(), True),
])

comment_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("upvotes", LongType(), True),
    StructField("downvotes", LongType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("timestamp", LongType(), True),
    StructField("permalink", StringType(), True),
    StructField("post_id", StringType(), True),
])

# ----------------------------
# Read weather stream
# ----------------------------
kafka_reader = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit_posts,reddit_comments,youtube_raw,weather_raw,air_raw,sentiment_scored") \
    .option("startingOffsets", "earliest") \
    .load()

weather_df = weather_stream.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp_utc"))) \
    .withWatermark("event_time", "2 days")
# ----------------------------
# 7-day windowed aggregates (streaming -> memory table)
# ----------------------------
avg_7d_stream = weather_df.groupBy(
    window(col("event_time"), "7 days", "1 day"),
    col("location").alias("loc")
).agg(
    avg("temperature").alias("temp_7d_avg"),
    avg("humidity").alias("humidity_7d_avg")
)

# write aggregates to memory sink (complete mode)
avg_query = avg_7d_stream.writeStream \
    .format("memory") \
    .queryName("avg_7d_table") \
    .outputMode("complete") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_PATH, "avg_7d")) \
    .start()

# allow a short warm-up so the memory table is populated (non-blocking)
time.sleep(5)

# check if table exists
tables = [t.name for t in spark.catalog.listTables()]
if "avg_7d_table" in tables:
    avg_7d_static = spark.table("avg_7d_table")
else:
    # create empty DataFrame with the expected schema so the join doesn't fail
    avg_7d_static = spark.createDataFrame([], avg_7d_schema)

# normalize avg_7d_static columns (rename window.start/end if needed)
# depending on Spark's memory table layout, the window field may be a struct:
# keep loc, temp_7d_avg, humidity_7d_avg available for join
if "loc" not in avg_7d_static.columns:
    # attempt to flatten if necessary (defensive)
    cols = avg_7d_static.columns
    # This fallback will keep as-is; downstream join uses avg_7d_static.loc
    pass

# ----------------------------
# Stream-static join on location
# ----------------------------
joined = weather_df.join(
    avg_7d_static,
    weather_df.location == avg_7d_static.loc,
    how="left"
).select(
    weather_df.location.alias("location"),
    weather_df.event_time.alias("window_ts"),
    weather_df.temperature,
    weather_df.humidity,
    weather_df.precipitation,
    avg_7d_static.temp_7d_avg,
    avg_7d_static.humidity_7d_avg
).withColumn("ts_ingested", current_timestamp())

# ----------------------------
# Write function to Cassandra (foreachBatch)
# ----------------------------
def write_batch_to_cassandra(df, epoch_id):
    # ensure column types and names match Cassandra table
    # rename or cast if needed before write
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
      .save()

# final streaming query (append mode required for stream-static)
query = joined.writeStream \
    .foreachBatch(write_batch_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_PATH, "write")) \
    .start()

query.awaitTermination()
