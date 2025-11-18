import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

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

        return record

    except Exception as e:
        print("Error fetching weather:", e)
        return None


def get_producer():
    return KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def main():
    producer = get_producer()

    while True:
        record = fetch_weather()
        if record:
            producer.send("weather_raw", record)
            producer.flush()
            print("Sent weather update:", record)

        time.sleep(300)  # 5 minutes


if __name__ == "__main__":
    main()
