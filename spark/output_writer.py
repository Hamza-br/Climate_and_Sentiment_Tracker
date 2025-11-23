"""
Output/persistence module for streaming data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.streaming.query import StreamingQuery


def write_stream_to_console(dataframe: DataFrame, name: str) -> StreamingQuery:
    """Write stream to console for debugging"""
    query = dataframe \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .start()
    
    print(f"✓ {name} stream console output started")
    return query


def prepare_weather_output(weather_transformed: DataFrame) -> DataFrame:
    """Prepare weather data for output"""
    return weather_transformed.select(
        col("city"),
        col("event_time_utc"),
        col("temperature_celsius"),
        col("feels_like_temp"),
        col("humidity"),
        col("precipitation"),
        col("wind_speed"),
        col("wind_category"),
        col("weather_anomaly"),
        col("weather_code"),
        col("ingestion_time")
    ).withColumn("batch_timestamp", current_timestamp())


def prepare_air_quality_output(air_quality_transformed: DataFrame) -> DataFrame:
    """Prepare air quality data for output"""
    return air_quality_transformed.select(
        col("city"),
        col("event_time_utc"),
        col("parameter_normalized"),
        col("value_standard_unit").alias("value"),
        col("aqi_category"),
        col("health_risk_level"),
        col("pollution_spike"),
        col("ingestion_time")
    ).withColumn("batch_timestamp", current_timestamp())


def prepare_social_output(social_transformed: DataFrame) -> DataFrame:
    """Prepare social posts for output"""
    return social_transformed.select(
        col("id").alias("post_id"),
        col("title_normalized"),
        col("body_normalized"),
        col("author_normalized"),
        col("source_topic").alias("source"),
        col("event_time_utc"),
        col("upvotes"),
        col("url"),
        col("ingestion_time")
    ).withColumn("batch_timestamp", current_timestamp())
