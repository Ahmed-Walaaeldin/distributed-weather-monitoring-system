# Lab 4 — Weather Stations Monitoring

Distributed IoT weather monitoring system: 10 weather stations → Apache Kafka →
Kafka Streams processor (rain detection) → central base station → PostgreSQL.

```
                       Data Acquisition                Data Processing & Archiving
 ┌──────────────┐
 │ station-1    │──┐
 │ station-2    │──┤        ┌──────────────────┐      ┌────────────────────┐
 │    ...       │──┼──────► │ Kafka            │────► │ rain-processor     │──┐
 │ station-10   │──┘  1 msg │ weather-readings │      │ humidity > 70 %    │  │
 └──────────────┘   /sec/st │ (10 partitions)  │      └────────────────────┘  │
                            │ rain-alerts      │◄───────────────────────────── ┘
                            └────────┬─────────┘
                                     │
                            ┌────────▼─────────┐        ┌──────────────┐
                            │ central-station  │──────► │ PostgreSQL   │
                            │ batch of 5000    │ batch  │ weather_     │
                            └──────────────────┘ INSERT │ readings     │
                                                        └──────────────┘
```

## Repository layout

| Path | What it is |
|---|---|
| `common/` | shared message model (`WeatherMessage`, `Weather`, `RainAlert`), JSON + env config helpers |
| `weather-station/` | **Part A + B** — mock sensor + Kafka producer |
| `rain-processor/` | **Part C** — Kafka Streams *Processor API* rain detection (+ a DSL variant) |
| `central-station/` | **Part D** — Kafka consumers + batched JDBC writer |
| `sql/` | **Part E** — schema and analysis queries |
| `k8s/` | **Part F** — all Kubernetes manifests + `deploy.sh` |
| `cloud/` | **Bonus** — two-VM cloud deployment + Aiven managed DB |
| `docs/REPORT.md` | technical report skeleton to fill with your screenshots |

## Message flow, step by step

1. **Station** (`WeatherStationApp`) wakes up every `EMIT_INTERVAL_MS` (1000 ms).
   `ReadingGenerator` builds a message: `s_no` is incremented **every tick**,
   `battery_status` is drawn 30 % low / 40 % medium / 30 % high, humidity is uniform
   0–100 so about 30 % of readings trigger rain.
2. With probability `DROP_RATE` (0.10) the message is **not sent** — but its `s_no`
   is already consumed. That gap in the sequence is exactly what query E-2 measures.
3. Otherwise it is serialised to JSON and produced to `weather-readings` with
   **key = station_id**, so a station's readings all land in one partition and keep
   their order. Producer is idempotent with `acks=all`.
4. **`rain-processor`** is a Kafka Streams topology built with the low-level
   Processor API: `source(weather-readings) → RainProcessor → sink(rain-alerts)`.
   `RainProcessor` parses the JSON, drops anything with `humidity <= 70`, bumps a
   per-station counter in a persistent state store, and forwards a `RainAlert`.
5. **`central-station`** runs two consumer threads.
   The readings thread buffers records and flushes when it reaches **5000 rows**
   *or* after `FLUSH_INTERVAL_MS`, using a single JDBC `executeBatch()`.
   Order matters: **DB commit first, Kafka offset commit second**. That gives
   at-least-once delivery, and `UNIQUE(station_id, sequence_number)` +
   `ON CONFLICT DO NOTHING` turns it into effectively-once.
6. The database is the historical store. `latest_station_status` (a `DISTINCT ON`
   view) answers "latest status per station" directly from SQL.

## Configuration (all via environment variables)

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | all |
| `STATION_ID` | derived from pod ordinal | station |
| `EMIT_INTERVAL_MS` | `1000` | station |
| `DROP_RATE` | `0.10` | station |
| `TOPIC_WEATHER` / `TOPIC_RAIN` | `weather-readings` / `rain-alerts` | all |
| `BATCH_SIZE` | `5000` | central |
| `FLUSH_INTERVAL_MS` | `10000` | central |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` / `DB_SSLMODE` | — | central |

No secret is ever written in the source: Kubernetes `Secret` in-cluster, `chmod 600`
env file on cloud VMs.

## Quick start

### Build
```bash
mvn -B -DskipTests package        # produces */target/<module>.jar
```

### Run locally with Docker Compose
```bash
docker compose up -d zookeeper kafka postgres
docker compose up -d --build rain-processor central-station weather-station
docker compose logs -f central-station
```

### Run the three services bare (no Docker)
```bash
# terminal 1 — Kafka must already be up on 127.0.0.1:9092
STATION_ID=1 java -jar weather-station/target/weather-station.jar
# terminal 2
java -jar rain-processor/target/rain-processor.jar
# terminal 3
DB_URL=jdbc:postgresql://localhost:5432/weather DB_USER=weather DB_PASSWORD=weatherpass \
  java -jar central-station/target/central-station.jar
```

### Sanity-check Kafka (as in the lab handout)
```bash
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
  --topic weather-readings --from-beginning
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
  --topic rain-alerts --from-beginning
```

### Deploy on Kubernetes (Minikube)
```bash
minikube start --cpus=4 --memory=8192
./k8s/deploy.sh
kubectl -n weather get pods
kubectl -n weather logs -f statefulset/weather-station
kubectl -n weather logs -f deploy/central-station
```

### Run the analysis
```bash
kubectl -n weather exec -it deploy/postgres -- \
  psql -U weather -d weather -f - < sql/analysis_queries.sql
# or interactively:
kubectl -n weather exec -it deploy/postgres -- psql -U weather -d weather
```

### Tear down
```bash
kubectl delete namespace weather && kubectl delete pv postgres-pv
```

## Expected results after ~10 minutes

- 10 stations × 60 msg/min × 0.9 ≈ **540 rows/minute** in `weather_readings`
- battery distribution converging on 30 / 40 / 30 (±2 % after a few thousand rows)
- drop rate converging on 10 %
- roughly 30 % of readings mirrored in `rain_alerts`
