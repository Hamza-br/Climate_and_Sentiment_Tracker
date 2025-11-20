import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from logging_utils import json_log
from kafka.errors import NoBrokersAvailable

load_dotenv(".env.openaq")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
SLEEP_SECONDS = int(os.getenv("AIR_SLEEP_SECONDS", "3600"))  # default 1 hour for daily data
COMPONENT = "air_ingest"
LOCATIONS_URL = "https://api.openaq.org/v3/locations?coordinates=42.3601,-71.0589&radius=25000&limit=5"
Kafka_config = {"bootstrap_servers": "kafka:9092"}

if not OPENAQ_API_KEY:
    json_log(COMPONENT, "config_warning", message="OPENAQ_API_KEY not set")
else:
    json_log(COMPONENT, "config_ok")

def get_producer(kafka_config):
    return KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        value_serializer=lambda x: json.dumps(x, indent=4).encode('utf-8'),
        retries=5,
        request_timeout_ms=30000,
        retry_backoff_ms=5000
    )

def discover_sensors():
    try:
        headers = {"X-API-Key": OPENAQ_API_KEY}
        json_log(COMPONENT, "discover_sensors_start", url=LOCATIONS_URL)
        resp = requests.get(LOCATIONS_URL, headers=headers, timeout=15)
        if resp.status_code != 200:
            json_log(COMPONENT, "locations_error", status=resp.status_code)
            return []

        data = resp.json()
        results = data.get("results", [])

        sensor_ids = []
        for location in results:
            location_id = location.get("id")
            location_name = location.get("name", "Unknown")
            coordinates = location.get("coordinates", {}) or {}
            latitude = coordinates.get("latitude")
            longitude = coordinates.get("longitude")

            # Get sensors for this location
            sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
            json_log(COMPONENT,"fetch_sensors", location=location_name, url=sensors_url)

            sensors_resp = requests.get(sensors_url, headers=headers, timeout=15)
            if sensors_resp.status_code == 200:
                sensors_data = sensors_resp.json()
                sensors = sensors_data.get("results", [])

                for sensor in sensors:
                    sensor_id = sensor.get("id")
                    parameter = sensor.get("parameter", {})
                    param_name = parameter.get("name", "unknown")

                    sensor_ids.append({
                        "sensor_id": sensor_id,
                        "location_id": location_id,
                        "location_name": location_name,
                        "parameter_name": param_name,
                        "parameter_units": parameter.get("units", ""),
                        "parameter_display": parameter.get("displayName", param_name),
                        "latitude": latitude,
                        "longitude": longitude
                    })

        json_log(COMPONENT, "discover_complete", total_sensors=len(sensor_ids))
        return sensor_ids

    except Exception as e:
        json_log(COMPONENT, "discover_exception", error=str(e))
        return []


def fetch_data(sensor_info):
    """Fetch data for a specific sensor"""
    try:
        headers = {"X-API-Key": OPENAQ_API_KEY}
        sensor_id = sensor_info["sensor_id"]
        location_id = sensor_info.get("location_id")
        location_name = sensor_info.get("location_name")
        latitude = sensor_info.get("latitude")
        longitude = sensor_info.get("longitude")

        # Get daily aggregated measurements by sensor_id with summary statistics
        # Using /days endpoint to get daily rollups with min, max, avg, median, quartiles, sd
        measurements_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days?limit=100"
        json_log(COMPONENT, "fetch", sensor_id=sensor_id, url=measurements_url)

        resp = requests.get(measurements_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            json_log(COMPONENT, "error", sensor_id=sensor_id, status=resp.status_code)
            return None

        data = resp.json()
        results = data.get("results", [])

        if not results:
            json_log(COMPONENT, "no_data", sensor_id=sensor_id)
            return None

        # Process each measurement
        records = []
        for measurement in results:
            param = measurement.get("parameter") or {}
            period = measurement.get("period") or {}
            datetime_from = period.get("datetimeFrom") or {}
            datetime_to = period.get("datetimeTo") or {}
            flag_info = measurement.get("flagInfo") or {}
            # Note: /days endpoint returns null for coordinates, using location coordinates instead
            coverage = measurement.get("coverage") or {}
            coverage_datetime_from = coverage.get("datetimeFrom") or {}
            coverage_datetime_to = coverage.get("datetimeTo") or {}
            summary_stats = measurement.get("summary") or {}
            
            record = {
                "sensor_id": sensor_id,
                "location_id": location_id,
                "location_name": location_name,
                "parameter_id": param.get("id"),
                "parameter_name": param.get("name"),
                "parameter_units": param.get("units"),
                "parameter_display": param.get("displayName"),
                "value": measurement.get("value"),
                "has_flags": flag_info.get("hasFlags", False),
                "period_label": period.get("label"),
                "period_interval": period.get("interval"),
                "datetime_from_utc": datetime_from.get("utc"),
                "datetime_from_local": datetime_from.get("local"),
                "datetime_to_utc": datetime_to.get("utc"),
                "datetime_to_local": datetime_to.get("local"),
                # Use location coordinates (measurement coordinates are null in /days endpoint)
                "latitude": latitude,
                "longitude": longitude,
                # Summary statistics
                "summary_min": summary_stats.get("min"),
                "summary_q02": summary_stats.get("q02"),
                "summary_q25": summary_stats.get("q25"),
                "summary_median": summary_stats.get("median"),
                "summary_q75": summary_stats.get("q75"),
                "summary_q98": summary_stats.get("q98"),
                "summary_max": summary_stats.get("max"),
                "summary_avg": summary_stats.get("avg"),
                "summary_sd": summary_stats.get("sd"),
                # Coverage information
                "coverage_expected_count": coverage.get("expectedCount"),
                "coverage_expected_interval": coverage.get("expectedInterval"),
                "coverage_observed_count": coverage.get("observedCount"),
                "coverage_observed_interval": coverage.get("observedInterval"),
                "coverage_percent_complete": coverage.get("percentComplete"),
                "coverage_percent_coverage": coverage.get("percentCoverage"),
                "coverage_datetime_from_utc": coverage_datetime_from.get("utc"),
                "coverage_datetime_to_utc": coverage_datetime_to.get("utc"),
                "ingested_at_utc": datetime.utcnow().isoformat()
            }
            records.append(record)

        json_log(COMPONENT, "success", sensor_id=sensor_id, records=len(records))
        return records

    except Exception as e:
        json_log(COMPONENT, "exception", sensor_id=sensor_id, error=str(e))
        return None


def main():
    json_log(COMPONENT, "start", broker="kafka:9092", sleep_seconds=SLEEP_SECONDS)
    producer = get_producer(Kafka_config)

    # Discover sensors once at startup
    sensors = discover_sensors()
    if not sensors:
        json_log(COMPONENT, "no_sensors_found")
        return

    json_log(COMPONENT, "sensors_discovered", count=len(sensors))

    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()
        json_log(COMPONENT, "cycle_begin", cycle=cycle, sensors_count=len(sensors))

        total_sent = 0
        for sensor_info in sensors:
            records = fetch_data(sensor_info)
            if records:
                for record in records:
                    producer.send("air_daily", record)
                    total_sent += 1
                producer.flush()

        elapsed = round(time.time() - cycle_start, 3)
        throughput = round((total_sent / elapsed) if elapsed > 0 else 0.0, 3)
        json_log(COMPONENT, "cycle_complete",
                 cycle=cycle,
                 elapsed_sec=elapsed,
                 records_sent=total_sent,
                 throughput_rps=throughput,
                 sleep_sec=SLEEP_SECONDS)

        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()