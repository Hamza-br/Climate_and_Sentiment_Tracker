#!/usr/bin/env python3
"""
Climate & Sentiment Tracker - Modular Spark Processor
Main orchestration script for the streaming pipeline
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import configuration
from config import CASSANDRA_HOST, CASSANDRA_PORT

# Import Kafka reader
from kafka_reader import (
    read_kafka_stream, parse_weather_stream, parse_air_quality_stream,
    parse_social_stream, parse_sentiment_stream
)

# Import data cleaning
from data_cleaner import (
    clean_weather_stream, clean_air_quality_stream, clean_social_stream,
    clean_sentiment_stream
)

# Import transformations
from transformations import (
    transform_weather, transform_air_quality, transform_social
)

# Import output writers
from output_writer import (
    write_stream_to_console, prepare_weather_output, prepare_air_quality_output,
    prepare_social_output
)


def initialize_spark() -> SparkSession:
    """Initialize Spark session"""
    
    spark = SparkSession.builder \
        .appName("ClimateProcessor") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session initialized")
    return spark


def main():
    """Main pipeline orchestration"""
    
    try:
        spark = initialize_spark()
        
        kafka_stream = read_kafka_stream(spark)

        weather_stream = parse_weather_stream(kafka_stream)
        air_quality_stream = parse_air_quality_stream(kafka_stream)
        social_stream = parse_social_stream(kafka_stream)
        sentiment_stream = parse_sentiment_stream(kafka_stream)
        
        weather_clean = clean_weather_stream(weather_stream)
        air_quality_clean = clean_air_quality_stream(air_quality_stream)
        social_clean = clean_social_stream(social_stream)
        sentiment_clean = clean_sentiment_stream(sentiment_stream)
        
        weather_transformed = transform_weather(weather_clean)
        air_quality_transformed = transform_air_quality(air_quality_clean)
        social_transformed = transform_social(social_clean)
        
        weather_output = prepare_weather_output(weather_transformed)
        air_quality_output = prepare_air_quality_output(air_quality_transformed)
        social_output = prepare_social_output(social_transformed)
        weather_output = prepare_weather_output(weather_transformed)
        air_quality_output = prepare_air_quality_output(air_quality_transformed)
        social_output = prepare_social_output(social_transformed)
        
        weather_query = write_stream_to_console(weather_output, "Weather")
        air_quality_query = write_stream_to_console(air_quality_output, "Air Quality")
        social_query = write_stream_to_console(social_output, "Social Posts")
        
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
