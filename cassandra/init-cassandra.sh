#!/bin/bash
# ============================================================
# Cassandra initialization script
# Creates keyspace and tables for Climate & Sentiment Tracker
# ============================================================

set -e

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
CQL_FILE="/cassandra-setup.cql"

echo "=================================================="
echo "Cassandra Initialization Script"
echo "=================================================="
echo "Target: $CASSANDRA_HOST:$CASSANDRA_PORT"
echo "CQL File: $CQL_FILE"
echo ""

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
until cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e "SELECT cluster_name FROM system.local;" 2>/dev/null; do
    echo "Cassandra is unavailable - sleeping"
    sleep 2
done

echo "✓ Cassandra is ready!"
echo ""

# Execute CQL setup script
echo "Executing Cassandra setup script..."
if [ -f "$CQL_FILE" ]; then
    cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -f "$CQL_FILE"
    echo ""
    echo "✓ Cassandra setup complete!"
    echo ""
    echo "Created keyspace: climate_sentiment"
    echo "Created tables:"
    echo "  1. weather_data"
    echo "  2. air_quality_data"
    echo "  3. social_posts"
    echo "  4. social_sentiment"
    echo "  5. daily_aggregates"
else
    echo "✗ CQL file not found: $CQL_FILE"
    exit 1
fi

echo ""
echo "✓ Cassandra initialization complete!"
