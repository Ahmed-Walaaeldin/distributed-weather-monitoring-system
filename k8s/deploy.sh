#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> Building Docker images..."
docker build -t weather-station:1.0.0  -f weather-station/Dockerfile  .
docker build -t rain-processor:1.0.0   -f rain-processor/Dockerfile   .
docker build -t central-station:1.0.0  -f central-station/Dockerfile  .

echo ">> Applying Kubernetes manifests..."
kubectl apply -f k8s/00-namespace-config.yaml
kubectl apply -f k8s/01-zookeeper.yaml
kubectl -n weather rollout status deployment/zookeeper --timeout=180s
kubectl apply -f k8s/02-kafka.yaml
kubectl apply -f k8s/02-postgres-storage.yaml
kubectl apply -f k8s/02-postgres.yaml

echo ">> Waiting for Kafka and Postgres..."
kubectl -n weather rollout status deploy/kafka --timeout=120s
kubectl -n weather rollout status deploy/postgres --timeout=120s

echo ">> Deploying application services..."
kubectl apply -f k8s/04-rain-processor.yaml
kubectl apply -f k8s/05-central-station.yaml
kubectl apply -f k8s/03-weather-stations.yaml

echo ">> Waiting for weather stations..."
kubectl -n weather rollout status statefulset/weather-station --timeout=300s

echo ""
echo ">> All deployed! Checking pods:"
kubectl -n weather get pods -o wide
