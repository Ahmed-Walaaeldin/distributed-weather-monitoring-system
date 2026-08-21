#!/usr/bin/env bash
# Generates docker-compose.stations.yml with N weather stations, each with a
# distinct STATION_ID, pointing at a remote Kafka broker (cloud bonus, VM #1).
set -euo pipefail
N=${1:-10}
KAFKA=${2:-$KAFKA_BOOTSTRAP_SERVERS}

{
  echo 'version: "3.8"'
  echo 'services:'
  for i in $(seq 1 "$N"); do
    cat <<SVC
  station-$i:
    image: \${IMAGE:-weather-station:1.0.0}
    restart: always
    environment:
      KAFKA_BOOTSTRAP_SERVERS: "$KAFKA"
      STATION_ID: "$i"
      EMIT_INTERVAL_MS: "1000"
      DROP_RATE: "0.10"
SVC
  done
} > docker-compose.stations.yml

echo "wrote docker-compose.stations.yml with $N stations -> $KAFKA"
