# Climate & Sentiment Tracker – Observability

This project now emits structured, machine-readable JSON logs across ingestion services and the Spark processor to make monitoring and troubleshooting simple.

## Log Format

- Fields: `ts_utc` (ISO8601 UTC), `component`, `event`, plus context fields.
- Output: one JSON object per line to stdout/stderr (Docker friendly).

### Core Events

- `start`: service start with key config (e.g., broker, host).
- `producer_ready`: producer/sinks initialized and ready.
- `cycle_begin`: a polling/processing cycle or Spark micro-batch begins.
- `send_success`: records written to Kafka/Cassandra successfully.
- `cycle_complete`: cycle finished with metrics.

### Cycle Metrics

- `elapsed_sec`: duration of the cycle/micro-batch.
- `records_sent`: number of records produced (ingestion) or rows written (Spark).
- `throughput_rps`: computed as `records_sent / elapsed_sec`.

Example log line:

```json
{"ts_utc":"2025-11-18T12:34:56.789012","component":"weather_ingest","event":"cycle_complete","cycle":42,"elapsed_sec":0.123,"records_sent":1,"throughput_rps":8.13,"sleep_sec":300}
```

## Where Logs Come From

- Ingestion services: `ingestion/weather`, `ingestion/air_quality`, `ingestion/youtube` emit events for API requests and Kafka sends.
- Spark processor: `spark/processor.py` emits events per micro-batch when writing to Cassandra (tables: raw, metrics hourly/daily).

## Viewing Logs

Use Docker to tail service logs in JSON:

```powershell
docker compose logs -f weather_ingest
docker compose logs -f air_ingest
docker compose logs -f youtube_ingest
docker compose logs -f spark_job
```

## Optional: Grafana Dashboard (Loki)

- A ready-to-import dashboard is provided at `grafana/dashboards/pipeline_observability.json`.
- Assumes a Grafana Loki data source named `Loki` (variable `DS_LOKI`).
- Panels include:
	- Recent pipeline logs
	- Send-success rate by component
	- Spark throughput by stream (rows/s)
	- Average cycle duration and last-cycle stats

To use: configure Loki + Promtail (or your preferred log pipeline) to collect container stdout, then import the dashboard JSON in Grafana.

