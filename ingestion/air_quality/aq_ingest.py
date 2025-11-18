import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv(".env.openaq")

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
BOSTON_COORDS = "42.3601,-71.0589"
OPENAQ_URL = f"https://api.openaq.org/v3/latest?coordinates={BOSTON_COORDS}"

def get_producer():
    return KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_air_quality():
    try:
        headers = {"Authorization": f"Bearer {OPENAQ_API_KEY}"}
        resp = requests.get(OPENAQ_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if "results" not in data or len(data["results"]) == 0:
            return None

        measurements = {}
        for m in data["results"][0]["measurements"]:
            measurements[m["parameter"]] = m["value"]

        record = {
            "location": "Boston, USA",
            "timestamp_utc": datetime.utcnow().isoformat(),
            "pm25": measurements.get("pm25"),
            "pm10": measurements.get("pm10"),
            "o3": measurements.get("o3"),
            "no2": measurements.get("no2"),
            "so2": measurements.get("so2"),
            "co": measurements.get("co"),
        }

        return record

    except Exception as e:
        print("Error fetching air quality:", e)
        return None


def main():
    producer = get_producer()

    while True:
        record = fetch_air_quality()
        if record:
            producer.send("air_raw", record)
            producer.flush()
            print("Sent air quality record:", record)

        time.sleep(300)  # 5 minutes


if __name__ == "__main__":
    main()
