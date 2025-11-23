"""
Configuration module for Spark streaming processor
"""
import os

# Kafka configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = ["weather_raw", "air_raw", "youtube_raw", "sentiment_scored"]

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "climate_sentiment")

# Checkpoint configuration
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/checkpoint")
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# Location filtering
BOSTON_CITY = "boston"
BOSTON_REGION = "massachusetts"
BOSTON_COUNTRY = "usa"
BOSTON_LOCATION = "boston"
BOSTON_LATITUDE = 42.3601
BOSTON_LONGITUDE = 71.0589

# Feature flags
ENABLE_BERT = True  # Will be set to False if PyTorch not available
