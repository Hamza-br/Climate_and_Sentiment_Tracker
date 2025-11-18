CREATE KEYSPACE IF NOT EXISTS climateks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS climateks.climate_metrics (
    location text,
    window_start timestamp,
    window_end timestamp,
    avg_temp double,
    avg_humidity double,
    temp_anomaly double,
    avg_pollution double,
    avg_pm25 double,
    ts_ingested timestamp,
    PRIMARY KEY ((location), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
