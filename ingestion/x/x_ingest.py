#!/usr/bin/env python3

import os
import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from logging_utils import json_log




COMPONENT = "x_ingest"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_posts_raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "kafka:9092")
SLEEP_SECONDS = float(os.getenv("X_SLEEP_SECONDS", "10"))

SENTIMENTS_TEXTS = [
    "The weather is getting worse every year! #climatechange",
    "Can't believe how hot it is today. Global warming is real.",
    "Air quality in the city is terrible right now.",
    "Beautiful day, but worried about the future of our planet.",
    "We need to act now to save the environment.",
    "Floods in the south are devastating. #climatecrisis",
    "Government needs to do more about pollution.",
    "Just read a scary report on ice caps melting.",
    "Solar power is the way forward!",
    "Why is nobody talking about the smog today?"
]
LOCATIONS = ["New York", "London", "Paris", "Tokyo", "Sydney", "Boston", "Mumbai", "Beijing"]

def get_producer(bootstrap_servers):
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
                retries=5
            )
            if p.bootstrap_connected():
                return p
        except NoBrokersAvailable as e:
            json_log(COMPONENT, "kafka_connect_retry", error=str(e))
            time.sleep(2)
        except Exception as e:
            json_log(COMPONENT, "kafka_connect_unexpected", error=str(e))
            time.sleep(2)

def generate_mock_tweet():
    text = random.choice(SENTIMENTS_TEXTS)
    location = random.choice(LOCATIONS)
    return {
        "platform": "x",
        "id": str(int(time.time() * 1000)) + str(random.randint(100, 999)),
        "text": text,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "user": f"user_{random.randint(1000, 9999)}",
        "location": location,
        "hashtags": [w for w in text.split() if w.startswith("#")]
    }

def main():
    producer = get_producer(KAFKA_BOOTSTRAP)
    json_log(COMPONENT, "start", broker=KAFKA_BOOTSTRAP, sleep_seconds=SLEEP_SECONDS)
    json_log(COMPONENT, "producer_ready")
    
    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()
        json_log(COMPONENT, "cycle_begin", cycle=cycle)
        
        batch_size = random.randint(1, 5)
        tweets = [generate_mock_tweet() for _ in range(batch_size)]
        
        for tweet in tweets:
            try:
                producer.send(KAFKA_TOPIC, key=tweet["location"], value=tweet)
            except Exception as e:
                json_log(COMPONENT, "kafka_send_error", error=str(e))
        
        producer.flush()
        
        elapsed = round(time.time() - cycle_start, 2)
        json_log(COMPONENT, "cycle_complete",
                 cycle=cycle, elapsed_sec=elapsed, records_sent=len(tweets))
        
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
