"""
Data schemas for Spark streaming processor
"""
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, BooleanType


WEATHER_SCHEMA = StructType([
    StructField("location", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", LongType(), True),
    StructField("wind_speed", DoubleType(), True),
])

AIR_QUALITY_SCHEMA = StructType([
    StructField("location", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
])

SOCIAL_POSTS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("source", StringType(), True),
    StructField("upvotes", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("url", StringType(), True),
])

SENTIMENT_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
])
