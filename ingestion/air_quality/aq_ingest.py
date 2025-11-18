import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os
from logging_utils import json_log

load_dotenv(".env.openaq")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
SLEEP_SECONDS = int(os.getenv("AIR_SLEEP_SECONDS", "300"))  # default 5 min
COMPONENT = "air_ingest"
# First get locations, then use /locations/{id}/latest for real-time values
LOCATIONS_URL = "https://api.openaq.org/v3/locations?coordinates=42.3601,-71.0589&radius=25000&limit=1"

# Validate API key
if not OPENAQ_API_KEY:
    json_log(COMPONENT, "config_warning", message="OPENAQ_API_KEY not set")
else:
    json_log(COMPONENT, "config_ok", key_length=len(OPENAQ_API_KEY))

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

def fetch_air_quality():
    try:
        headers = {"X-API-Key": OPENAQ_API_KEY}
        # Step 1: Get nearest location
        json_log(COMPONENT, "api_request_locations", url=LOCATIONS_URL)
        resp = requests.get(LOCATIONS_URL, headers=headers, timeout=15)
        if resp.status_code != 200:
            json_log(COMPONENT, "locations_error", status=resp.status_code, body=resp.text[:250])
            return None
        
        data = resp.json()
        results = data.get("results", [])
        if not results:
            json_log(COMPONENT, "no_locations")
            return None
        
        location = results[0]
        location_id = location.get("id")
        location_name = location.get("name", "Unknown")
        
        # Step 2: Get latest measurements for this location
        latest_url = f"https://api.openaq.org/v3/locations/{location_id}/latest"
        json_log(COMPONENT, "api_request_latest", url=latest_url, location_id=location_id)
        latest_resp = requests.get(latest_url, headers=headers, timeout=15)
        
        if latest_resp.status_code != 200:
            json_log(COMPONENT, "latest_error", status=latest_resp.status_code, body=latest_resp.text[:250])
            return None
        
        latest_data = latest_resp.json()
        latest_results = latest_data.get("results", [])
        
        if not latest_results:
            json_log(COMPONENT, "no_latest_data", location_id=location_id)
            return None
        
        # Flatten sensor readings into top-level fields
        pollutants = {}
        for reading in latest_results:
            value = reading.get("value")
            sensors_id = reading.get("sensorsId")
            # Get parameter info from sensors endpoint or infer from context
            # For now, we'll store by sensor ID (can enhance later)
            pollutants[f"sensor_{sensors_id}"] = value
        
        coords = location.get("coordinates", {})
        record = {
            "location_id": location_id,
            "location_name": location_name,
            "latitude": coords.get("latitude"),
            "longitude": coords.get("longitude"),
            "timestamp_utc": datetime.utcnow().isoformat(),
            "measurements": pollutants,
            "sensor_count": len(latest_results)
        }
        
        json_log(COMPONENT, "api_success", location=location_name, sensor_count=len(latest_results))
        return record
        
    except Exception as e:
        json_log(COMPONENT, "api_exception", error=str(e))
        return None

def main():
    json_log(COMPONENT, "start", broker=KAFKA_BROKER)
    producer = get_producer()
    json_log(COMPONENT, "producer_ready")
    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()
        json_log(COMPONENT, "cycle_begin", cycle=cycle)
        record = fetch_air_quality()
        sent = 0
        if record:
            producer.send("air_raw", record)
            producer.flush()
            json_log(COMPONENT, "send_success", cycle=cycle)
            sent = 1
        else:
            json_log(COMPONENT, "send_skipped", cycle=cycle)
        elapsed = round(time.time() - cycle_start, 3)
        throughput = round((sent / elapsed) if elapsed > 0 else 0.0, 3)
        json_log(COMPONENT, "cycle_complete", cycle=cycle, elapsed_sec=elapsed, records_sent=sent, throughput_rps=throughput, sleep_sec=SLEEP_SECONDS)
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
