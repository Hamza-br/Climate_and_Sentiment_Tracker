"""
Data cleaning and normalization module
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lower, trim, lit, udf
from pyspark.sql.types import StringType
from config import BOSTON_CITY, BOSTON_REGION, BOSTON_COUNTRY, BOSTON_LOCATION


def clean_weather_stream(weather_stream: DataFrame) -> DataFrame:
    """Clean and normalize weather stream"""
    weather_clean = weather_stream \
        .filter(col("is_valid")) \
        .filter(lower(col("location")).contains(BOSTON_LOCATION)) \
        .withColumn("event_time_utc", F.from_utc_timestamp(col("event_time"), "UTC")) \
        .withColumn("temperature_celsius", col("temperature")) \
        .withColumn("location_normalized", lower(trim(col("location")))) \
        .withColumn("city", lit(BOSTON_CITY)) \
        .withColumn("region", lit(BOSTON_REGION)) \
        .withColumn("country", lit(BOSTON_COUNTRY)) \
        .select(
            "location_normalized", "city", "region", "country",
            "event_time_utc", "temperature_celsius",
            "humidity", "precipitation", "weather_code", "wind_speed",
            "source_topic", "ingestion_time"
        )
    
    print(f"✓ Weather stream cleaned: Boston only, {BOSTON_CITY.upper()}, MA, USA")
    return weather_clean


def clean_air_quality_stream(air_quality_stream: DataFrame) -> DataFrame:
    """Clean and normalize air quality stream"""
    air_quality_clean = air_quality_stream \
        .filter(col("is_valid")) \
        .filter(
            (lower(col("city")).contains(BOSTON_CITY)) | 
            (lower(col("location")).contains(BOSTON_CITY))
        ) \
        .withColumn("event_time_utc", F.from_utc_timestamp(col("event_time"), "UTC")) \
        .withColumn("parameter_normalized", lower(trim(col("parameter")))) \
        .withColumn("location_normalized", lower(trim(col("location")))) \
        .withColumn("city_normalized", lower(trim(col("city")))) \
        .withColumn("country_normalized", lower(trim(col("country")))) \
        .withColumn("value_standard_unit", col("value")) \
        .withColumn("city", lit(BOSTON_CITY)) \
        .withColumn("region", lit(BOSTON_REGION)) \
        .withColumn("country", lit(BOSTON_COUNTRY)) \
        .select(
            "city", "region", "country",
            "parameter_normalized", "value_standard_unit", "unit",
            "latitude", "longitude", "event_time_utc",
            "source_topic", "ingestion_time"
        )
    
    print(f"✓ Air quality stream cleaned: Boston only, {BOSTON_CITY.upper()}, MA, USA")
    return air_quality_clean


def clean_social_stream(social_stream: DataFrame) -> DataFrame:
    """Clean and normalize social posts stream"""
    import re
    
    def normalize_text(text: str) -> str:
        """Normalize text by removing URLs, special characters"""
        if not text:
            return ""
        text = re.sub(r'http\S+|www\S+', '', text)  # Remove URLs
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special chars
        text = text.lower().strip()
        return text
    
    normalize_text_udf = udf(normalize_text, StringType())
    
    social_clean = social_stream \
        .filter(col("is_valid")) \
        .withColumn("event_time_utc", F.from_utc_timestamp(col("event_time"), "UTC")) \
        .withColumn("title_normalized", normalize_text_udf(col("title"))) \
        .withColumn("body_normalized", normalize_text_udf(col("body"))) \
        .withColumn("author_normalized", lower(trim(col("author")))) \
        .withColumn("city", lit(BOSTON_CITY)) \
        .withColumn("region", lit(BOSTON_REGION)) \
        .withColumn("country", lit(BOSTON_COUNTRY)) \
        .select(
            "id", "title_normalized", "body_normalized",
            "author_normalized", "source_topic", "upvotes",
            "city", "region", "country", "event_time_utc",
            "url", "ingestion_time"
        )
    
    print(f"✓ Social posts stream cleaned: Boston-focused, {BOSTON_CITY.upper()}, MA, USA")
    return social_clean


def clean_sentiment_stream(sentiment_stream: DataFrame) -> DataFrame:
    """Clean and normalize sentiment stream"""
    sentiment_clean = sentiment_stream \
        .filter(col("is_valid")) \
        .withColumn("event_time_utc", F.from_utc_timestamp(col("event_time"), "UTC")) \
        .withColumn("sentiment_label", lower(trim(col("sentiment_label")))) \
        .select(
            "post_id", "sentiment_label", "sentiment_score",
            "event_time_utc", "ingestion_time"
        )
    
    print(f"✓ Sentiment stream cleaned: UTC timestamps, normalized labels")
    return sentiment_clean
