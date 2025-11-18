import json
import time
import os
import requests
from kafka import KafkaProducer
from datetime import datetime
from logging_utils import json_log

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SLEEP_SECONDS = int(os.getenv("WEATHER_SLEEP_SECONDS", "300"))  # default 5 min
COMPONENT = "weather_ingest"

BOSTON_COORDS = {
    "lat": 42.3601,
    "lon": -71.0589
}

OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={lon}"
    "&hourly=temperature_2m,relativehumidity_2m,precipitation,weathercode,windspeed_10m"
)

def fetch_weather():
    url = OPEN_METEO_URL.format(
        lat=BOSTON_COORDS["lat"],
        lon=BOSTON_COORDS["lon"]
    )
    try:
        json_log(COMPONENT, "api_request", url=url)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        hourly = data["hourly"]

        record = {
            "location": "Boston, USA",
            "timestamp_utc": datetime.utcnow().isoformat(),
            "temperature": hourly["temperature_2m"][0],
            "humidity": hourly["relativehumidity_2m"][0],
            "precipitation": hourly["precipitation"][0],
            "weather_code": hourly["weathercode"][0],
            "wind_speed": hourly["windspeed_10m"][0]
        }
        json_log(COMPONENT, "api_success", temperature=record["temperature"], humidity=record["humidity"])
        return record

    except Exception as e:
        json_log(COMPONENT, "api_error", error=str(e))
        return None


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def main():
    json_log(COMPONENT, "start", broker=KAFKA_BROKER)
    producer = get_producer()
    json_log(COMPONENT, "producer_ready")

    cycle = 0
    while True:
        cycle_start = time.time()
        cycle += 1
        json_log(COMPONENT, "cycle_begin", cycle=cycle)
        record = fetch_weather()
        sent = 0
        if record:
            producer.send("weather_raw", record)
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
