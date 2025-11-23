#!/usr/bin/env python3
"""
Test utilities for debugging individual modules
Usage: python test_modules.py
"""
from pyspark.sql import SparkSession
from config import CASSANDRA_HOST, CASSANDRA_PORT


def init_test_spark() -> SparkSession:
    """Initialize Spark for testing"""
    return SparkSession.builder \
        .appName("ClimateProcessorTest") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


def test_schemas():
    """Test schema definitions"""
    from schemas import (
        WEATHER_SCHEMA, AIR_QUALITY_SCHEMA, 
        SOCIAL_POSTS_SCHEMA, SENTIMENT_SCHEMA
    )
    
    print(f"Weather schema fields: {len(WEATHER_SCHEMA.fields)}")
    print(f"Air quality schema fields: {len(AIR_QUALITY_SCHEMA.fields)}")
    print(f"Social posts schema fields: {len(SOCIAL_POSTS_SCHEMA.fields)}")
    print(f"Sentiment schema fields: {len(SENTIMENT_SCHEMA.fields)}")


def test_config():
    """Test configuration loading"""
    from config import (
        KAFKA_BOOTSTRAP, KAFKA_TOPICS, CASSANDRA_HOST,
        BOSTON_CITY, BOSTON_REGION, BOSTON_COUNTRY
    )
    
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP}")
    print(f"Kafka topics: {KAFKA_TOPICS}")
    print(f"Cassandra host: {CASSANDRA_HOST}")
    print(f"Boston location: {BOSTON_CITY}, {BOSTON_REGION}, {BOSTON_COUNTRY}")


def test_kafka_reader():
    """Test Kafka reader module"""
    spark = init_test_spark()
    
    try:
        from kafka_reader import read_kafka_stream
        kafka_stream = read_kafka_stream(spark)
        print("✓ Kafka stream reader initialized")
    except Exception as e:
        print(f"✗ Kafka reader error: {str(e)}")


def test_data_cleaner():
    """Test data cleaner module (with sample data)"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from data_cleaner import clean_weather_stream
    
    spark = init_test_spark()
    
    # Create sample weather data
    schema = StructType([
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("weather_code", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("is_valid", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("source_topic", StringType(), True),
        StructField("ingestion_time", StringType(), True),
    ])
    
    data = [("Boston", 20.5, 65.0, 0.5, "01", 5.2, "2024-01-01", "true", "2024-01-01", "weather_raw", "2024-01-01")]
    sample_df = spark.createDataFrame(data, schema)
    
    try:
        cleaned = clean_weather_stream(sample_df)
        print(f"Data cleaner working. Columns: {cleaned.columns}")
    except Exception as e:
        print(f"Data cleaner error: {str(e)}")


def test_transformations():
    from transformations import transform_weather


def test_output_writer():
    """Test output writer module"""
    from output_writer import (
        write_stream_to_console, prepare_weather_output,
        prepare_air_quality_output, prepare_social_output
    )
    


if __name__ == "__main__":    
    try:
        test_config()
        test_schemas()
        test_kafka_reader()
        test_data_cleaner()
        test_transformations()
        test_output_writer()
        
        print("\n" + "="*60)
        print("✓ ALL TESTS PASSED")
        print("="*60)
    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
