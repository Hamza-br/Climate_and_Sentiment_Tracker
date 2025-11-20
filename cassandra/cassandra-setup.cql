CREATE KEYSPACE IF NOT EXISTS climateks WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor':1
};

CREATE TABLE IF NOT EXISTS climateks.climate_metrics (
  location text,
  window_ts timestamp,
  temperature double,
  humidity double,
  precipitation double,
  temp_7d_avg double,
  humidity_7d_avg double,
  ts_ingested timestamp,
  PRIMARY KEY ((location), window_ts)
) WITH CLUSTERING ORDER BY (window_ts DESC);
