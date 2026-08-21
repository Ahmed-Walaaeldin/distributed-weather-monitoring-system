#!/usr/bin/env bash
# One-shot deployment on Minikube.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> pointing docker at the Minikube daemon"
eval "$(minikube docker-env)"

echo ">> building images"
docker build -t weather-station:1.0.0  -f weather-station/Dockerfile  .
docker build -t rain-processor:1.0.0   -f rain-processor/Dockerfile   .
docker build -t central-station:1.0.0  -f central-station/Dockerfile  .

echo ">> applying manifests"
kubectl apply -f k8s/00-namespace-config.yaml
kubectl apply -f k8s/01-zookeeper.yaml
kubectl apply -f k8s/03-postgres.yaml
kubectl apply -f k8s/02-kafka.yaml
kubectl -n weather rollout status deploy/kafka --timeout=180s
kubectl apply -f k8s/05-rain-processor.yaml
kubectl apply -f k8s/06-central-station.yaml
kubectl apply -f k8s/04-weather-stations.yaml

echo ">> waiting for the stations"
kubectl -n weather rollout status statefulset/weather-station --timeout=300s
kubectl -n weather get pods -o wide
