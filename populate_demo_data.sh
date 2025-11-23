#!/usr/bin/env bash
# Populate Cassandra with sample demo data

echo "Inserting sample weather data..."
docker exec cassandra cqlsh -e "
INSERT INTO climate_sentiment.weather_data (city, event_time_utc, temperature_celsius, humidity, precipitation, wind_speed, ingestion_time) VALUES ('Boston', '2025-11-22 20:00:00+0000', 6.8, 75.0, 0.0, 12.5, toTimestamp(now()));
INSERT INTO climate_sentiment.weather_data (city, event_time_utc, temperature_celsius, humidity, precipitation, wind_speed, ingestion_time) VALUES ('Boston', '2025-11-22 19:00:00+0000', 5.2, 80.0, 2.5, 15.0, toTimestamp(now()));
INSERT INTO climate_sentiment.weather_data (city, event_time_utc, temperature_celsius, humidity, precipitation, wind_speed, ingestion_time) VALUES ('Boston', '2025-11-22 18:00:00+0000', 4.5, 82.0, 1.0, 10.0, toTimestamp(now()));
"

echo "Inserting sample social posts..."
docker exec cassandra cqlsh -e "
INSERT INTO climate_sentiment.social_posts (post_id, event_time_utc, body_normalized, author_normalized, source, keywords, ingestion_time) VALUES ('tweet1', '2025-11-22 20:00:00+0000', 'Climate change is getting worse!', 'user_1234', 'x', ['climate'], toTimestamp(now()));
INSERT INTO climate_sentiment.social_posts (post_id, event_time_utc, body_normalized, author_normalized, source, keywords, ingestion_time) VALUES ('tweet2', '2025-11-22 19:30:00+0000', 'Beautiful day but worried about future', 'user_5678', 'x', ['weather'], toTimestamp(now()));
INSERT INTO climate_sentiment.social_posts (post_id, event_time_utc, body_normalized, author_normalized, source, keywords, ingestion_time) VALUES ('yt1', '2025-11-22 19:00:00+0000', 'We need to act now to save the environment', 'climate_channel', 'youtube', ['environment'], toTimestamp(now()));
"

echo "Inserting sample sentiment data..."
docker exec cassandra cqlsh -e "
INSERT INTO climate_sentiment.social_sentiment (post_id, event_time_utc, sentiment_label, sentiment_confidence_pct, body_normalized, source, keywords, ingestion_time) VALUES ('tweet1', '2025-11-22 20:00:00+0000', 'Negative', 0.95, 'Climate change is getting worse!', 'x', ['climate'], toTimestamp(now()));
INSERT INTO climate_sentiment.social_sentiment (post_id, event_time_utc, sentiment_label, sentiment_confidence_pct, body_normalized, source, keywords, ingestion_time) VALUES ('tweet2', '2025-11-22 19:30:00+0000', 'Positive', 0.75, 'Beautiful day but worried about future', 'x', ['weather'], toTimestamp(now()));
INSERT INTO climate_sentiment.social_sentiment (post_id, event_time_utc, sentiment_label, sentiment_confidence_pct, body_normalized, source, keywords, ingestion_time) VALUES ('yt1', '2025-11-22 19:00:00+0000', 'Positive', 0.90, 'We need to act now to save the environment', 'youtube', ['environment'], toTimestamp(now()));
"

echo "Done! Verifying..."
docker exec cassandra cqlsh -e "
SELECT COUNT(*) as weather_count FROM climate_sentiment.weather_data;
SELECT COUNT(*) as posts_count FROM climate_sentiment.social_posts;
SELECT COUNT(*) as sentiment_count FROM climate_sentiment.social_sentiment;
"
