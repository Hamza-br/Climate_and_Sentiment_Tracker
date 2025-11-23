# Climate & Sentiment Tracker

## Status: Under Development & Continuous Improvement

Production-focused data pipeline integrating climate (weather, air quality) and social sentiment signals for real-time analytics and visualization. This project is actively being developed with regular updates and enhancements.

## Overview

- Ingestion microservices pull weather, air quality, and social content, normalizing and enriching events.
- Stream processing (Spark) cleans, transforms, and correlates climate + sentiment metrics.
- Cassandra stores raw and aggregated data for fast retrieval.
- FastAPI service exposes REST endpoints for dashboards and external consumers.
- Grafana provides interactive visualization (provisioned automatically).
- Airflow DAGs (optional) orchestrate batch ML training and Spark workflows.

## Core Components

- ingestion/: Source-specific ingestion services (weather, air quality, social) containerized individually.
- spark/: Transformation & correlation logic (stream/batch) producing enriched and aggregate datasets.
- pi/: FastAPI application serving health, latest metrics, anomalies, sentiment summaries, correlations.
- cassandra/: Keyspace & table initialization scripts.
- grafana/: Provisioned dashboards and datasource definitions.
- dags/: Airflow DAGs for ML model retraining and Spark batch jobs.
- model/: Inference utilities (sentiment or anomaly scoring integration point).

## Data Flow

1. Ingestion containers fetch external data (APIs, streams) and write normalized events to Kafka / staging.
2. Spark processors consume, clean, validate schemas, and derive correlation + aggregate metrics.
3. Cassandra stores both raw granular events and daily aggregates.
4. API layer queries Cassandra with optimized CQL patterns for dashboard consumption.
5. Grafana dashboards visualize real-time and historical climate & sentiment dynamics.

## Getting Started

Requirements: Docker + Docker Compose.

```powershell
docker compose up -d --build
```

Services brought up:

- Cassandra database
- Ingestion microservices
- Spark processing container
- FastAPI service (default: <http://localhost:8000>)
- Grafana (default: <http://localhost:3000>)

## API Examples

- Health: GET /health
- Latest Weather: GET /api/v1/weather/latest?limit=20
- Weather Anomalies: GET /api/v1/weather/anomalies?limit=50
- Latest Air Quality: GET /api/v1/air-quality/latest?parameter=pm25
- Pollution Spikes: GET /api/v1/air-quality/pollution-spikes?limit=50
- Latest Social Posts: GET /api/v1/social/posts/latest?limit=20
- Sentiment Summary: GET /api/v1/social/sentiment/summary
- Daily Aggregates: GET /api/v1/analytics/daily?date=2025-11-23&city=Boston, USA

## Configuration

Environment variables (override in compose or container runtime):

- CASSANDRA_HOST (default: localhost)
- CASSANDRA_PORT (default: 9042)
- CASSANDRA_KEYSPACE (default: climate_sentiment)
- CASSANDRA_USER / CASSANDRA_PASSWORD

## Development

Run API locally (after ensuring Cassandra accessible):

```powershell
python api/main.py
```

## Folder Structure (Essential Only)

```text
api/            FastAPI service
ingestion/      Source ingestion microservices
spark/          Stream & batch processing
cassandra/      Keyspace & schema setup
grafana/        Dashboards & provisioning
dags/           Airflow orchestration (optional)
model/          Inference utilities
docker-compose.yaml  Multi-service orchestration
API_DOCUMENTATION.md Endpoint reference (extended)
verify_system.py     Basic runtime/system checks
inject_test_data.py  Utility for sample/test inserts
```

## Production Hardening (Next Steps)

- Add auth layer (e.g. API keys or OAuth) for API.
- Implement retention policies / TTLs in Cassandra.
- Integrate structured logging with centralized aggregation.
- Add unit/integration tests and CI pipeline.
- Enable schema evolution strategy (migrations tooling).

## License

Specify project license here.

## Contributing

Open issues / PRs following conventional commits and include context + reproducible steps.
