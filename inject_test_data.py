#!/usr/bin/env python3

import json
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = {
    'weather': 'weather_raw',
    'air_quality': 'air_quality_raw',
    'social': 'social_posts_raw'
}

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def create_weather_data():
    return {
        "location": "Boston",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "temperature": 25.5,
        "humidity": 60.0,
        "precipitation": 0.0,
        "weather_code": 1,
        "wind_speed": 15.0
    }

def create_air_quality_data():
    return {
        "city": "Boston",
        "coordinates": "42.3601,-71.0589",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "measure_count": 1,
        "measurements": [
            {
                "sensor_id": "test_sensor_1",
                "location_id": 1,
                "location_name": "Downtown",
                "parameter_name": "pm25",
                "parameter_units": "µg/m³",
                "value": 12.5,
                "datetime_utc": datetime.utcnow().isoformat(),
                "latitude": 42.3601,
                "longitude": -71.0589,
                "source": "OpenAQ"
            }
        ]
    }

def create_social_post():
    return {
        "platform": "twitter",
        "id": f"test_post_{int(time.time())}",
        "text": "The heat in Boston is unbearable today! #climatechange #heatwave",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "user": "test_user",
        "location": "Boston, MA",
        "hashtags": ["climatechange", "heatwave"]
    }

def main():
    try:
        producer = get_producer()
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return
    
    print("Sending test data to Kafka...")
    
    weather_data = create_weather_data()
    producer.send(TOPICS['weather'], weather_data)
    print(f"✓ Sent weather data: {weather_data}")
    
    aq_data = create_air_quality_data()
    producer.send(TOPICS['air_quality'], aq_data)
    print(f"✓ Sent air quality data")
    
    social_data = create_social_post()
    producer.send(TOPICS['social'], social_data)
    print(f"✓ Sent social post data")
    
    producer.flush()
    print("All test data sent successfully!")

if __name__ == "__main__":
    main()

if__name__=="__main__":
    main()
