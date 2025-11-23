"""
Kafka stream reading module
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, current_timestamp, lit, to_timestamp, 
    when, from_utc_timestamp
)
from config import KAFKA_BOOTSTRAP, KAFKA_TOPICS
from schemas import WEATHER_SCHEMA, AIR_QUALITY_SCHEMA, SOCIAL_POSTS_SCHEMA, SENTIMENT_SCHEMA


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read all Kafka topics into a single stream""" 
    try:
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("subscribe", ",".join(KAFKA_TOPICS)) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"Successfully connected to Kafka broker at {KAFKA_BOOTSTRAP}")
        print(f"Subscribed to topics: {', '.join(KAFKA_TOPICS)}")
        
        return kafka_stream
    except Exception as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        raise


def parse_weather_stream(kafka_stream: DataFrame) -> DataFrame:
    """Parse weather_raw topic"""
    weather_stream = kafka_stream \
        .filter(col("topic") == "weather_raw") \
        .select(
            from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("timestamp_utc"))) \
        .withColumn("source_topic", lit("weather_raw")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("is_valid", lit(True)) \
        .withWatermark("event_time", "2 days")
    
    print("✓ weather_raw parsed with schema, timestamps converted, watermark applied")
    return weather_stream


def parse_air_quality_stream(kafka_stream: DataFrame) -> DataFrame:
    """Parse air_raw topic"""
    air_quality_stream = kafka_stream \
        .filter(col("topic") == "air_raw") \
        .select(
            from_json(col("value").cast("string"), AIR_QUALITY_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("timestamp_utc"))) \
        .withColumn("source_topic", lit("air_raw")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("is_valid", lit(True)) \
        .withWatermark("event_time", "2 days")
    
    print("air_raw parsed")
    return air_quality_stream


def parse_social_stream(kafka_stream: DataFrame) -> DataFrame:
    """Parse youtube_raw topic"""
    social_stream = kafka_stream \
        .filter(col("topic") == "youtube_raw") \
        .select(
            from_json(col("value").cast("string"), SOCIAL_POSTS_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("timestamp") / 1000.0)) \
        .withColumn("source_topic", lit("youtube_raw")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("is_valid", lit(True)) \
        .withWatermark("event_time", "2 days")
    
    print("youtube_raw parsed")
    return social_stream


def parse_sentiment_stream(kafka_stream: DataFrame) -> DataFrame:
    """Parse sentiment_scored topic"""
    sentiment_stream = kafka_stream \
        .filter(col("topic") == "sentiment_scored") \
        .select(
            from_json(col("value").cast("string"), SENTIMENT_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("timestamp_utc"))) \
        .withColumn("source_topic", lit("sentiment_scored")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("is_valid", lit(True)) \
        .withWatermark("event_time", "2 days")
    
    print("sentiment_scored parsed")
    return sentiment_stream
