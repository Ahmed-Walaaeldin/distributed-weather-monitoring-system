# Repository Context

This repository implements Lab 4 for Net Centric Computing: a distributed weather station monitoring system. The lab PDF is stored at `docs/Lab4-NetCentric.pdf`. Local PDF text extraction tools/libraries were not available when this context was created, but the repository docs and code explicitly map the implementation to the lab parts:

- Part A: mock weather station message generation.
- Part B: Kafka producer integration.
- Part C: Kafka Streams rain detection using the low-level Processor API.
- Part D: central base station consumer and batched SQL persistence.
- Part E: historical analysis SQL queries.
- Part F: Kubernetes deployment.
- Bonus: split cloud deployment with two VMs and managed PostgreSQL.

## High-Level System

The system simulates 10 IoT weather stations. Each station emits one weather status sample per second, intentionally drops about 10 percent of generated messages, and sends the remaining JSON messages to Kafka. A Kafka Streams application consumes weather readings, detects rain when humidity is higher than 70 percent, and forwards special rain alert messages. A central base station consumes readings and rain alerts from Kafka and persists them to PostgreSQL. SQL queries then analyze battery distribution, dropped messages, latest station status, rain alert correctness, and throughput.

Main flow:

```text
weather-station x10
  -> Kafka topic weather-readings
  -> rain-processor
  -> Kafka topic rain-alerts
  -> central-station
  -> PostgreSQL
```

The main architectural choices are Kafka for buffering, ordering, fan-out, and replay; PostgreSQL for historical storage; a Kafka Streams state store for per-station rain alert counts; and Kubernetes StatefulSet identity for stable station IDs.

## Repository Layout

```text
.
├── README.md
├── context.md
├── pom.xml
├── docker-compose.yml
├── analysis_queries.sql
├── common/
├── weather-station/
├── rain-processor/
├── central-station/
├── sql/
├── k8s/
├── cloud/
└── docs/
```

Important paths:

- `pom.xml`: root Maven parent project.
- `common/`: shared Java models and helpers.
- `weather-station/`: simulated station producer service.
- `rain-processor/`: Kafka Streams rain detector.
- `central-station/`: Kafka consumers and PostgreSQL writers.
- `sql/schema.sql`: standalone/reference PostgreSQL schema.
- `sql/analysis_queries.sql`: analysis queries for Part E.
- `analysis_queries.sql`: duplicate/root copy of the same analysis queries.
- `docker-compose.yml`: local single-machine development stack.
- `k8s/`: Kubernetes manifests and deployment script.
- `cloud/`: bonus two-VM cloud deployment assets.
- `docs/REPORT.md`: report skeleton.
- `docs/CLOUD-DEPLOYMENT.md`: duplicate/copy of cloud deployment instructions.
- `docs/Lab4-NetCentric.pdf`: lab handout PDF.

Generated build artifacts are present under module `target/` directories and are currently untracked by git:

- `common/target/`
- `weather-station/target/`
- `rain-processor/target/`
- `central-station/target/`

## Technology Stack

- Java 17.
- Maven multi-module build.
- Apache Kafka clients 3.7.0.
- Apache Kafka Streams 3.7.0.
- Jackson Databind 2.17.1.
- PostgreSQL JDBC driver 42.7.3.
- SLF4J Simple 2.0.13.
- Docker and Docker Compose for local/cloud runtime.
- Kubernetes/Minikube manifests for cluster deployment.
- PostgreSQL 16 for persistence.
- Bitnami Zookeeper 3.9 and Kafka 3.6 images in compose/k8s.

## Maven Project

The root Maven project has:

- `groupId`: `com.netcentric.weather`
- `artifactId`: `weather-monitoring`
- `version`: `1.0.0`
- `packaging`: `pom`
- Java source/target: `17`

Modules:

- `common`
- `weather-station`
- `rain-processor`
- `central-station`

The root POM centralizes dependency versions and configures `maven-shade-plugin` version `3.5.3` in plugin management. Service modules build shaded executable JARs with fixed final names:

- `weather-station/target/weather-station.jar`
- `rain-processor/target/rain-processor.jar`
- `central-station/target/central-station.jar`

Build command:

```bash
mvn -B -DskipTests package
```

There are no tests in the current source tree.

## Shared Module: `common`

Package: `com.netcentric.weather.common`

Purpose: shared message schema, JSON serialization, topic names, and environment/system-property config helpers.

Files:

- `Config.java`
- `WeatherMessage.java`
- `Weather.java`
- `RainAlert.java`
- `Json.java`
- `Topics.java`

### `Config`

`Config` reads configuration in a 12-factor style.

Lookup behavior:

1. Check environment variable.
2. If missing/blank, check Java system property.
3. If missing/blank, use default value.

Methods:

- `get(String key, String defaultValue)`
- `require(String key)`
- `getInt(String key, int defaultValue)`
- `getLong(String key, long defaultValue)`
- `getDouble(String key, double defaultValue)`

`require` throws `IllegalStateException` if a required value is not available. The central station uses this for `DB_PASSWORD`.

### `WeatherMessage`

Represents the lab weather status JSON message:

```json
{
  "station_id": 1,
  "s_no": 1,
  "battery_status": "low",
  "status_timestamp": 1681521224,
  "weather": {
    "humidity": 35,
    "temperature": 100,
    "wind_speed": 13
  }
}
```

Fields:

- `station_id`: long station ID.
- `s_no`: long sequence number, named `sNo` in Java.
- `battery_status`: string, expected values `low`, `medium`, `high`.
- `status_timestamp`: Unix epoch seconds.
- `weather`: nested `Weather` object.

Jackson annotations:

- Ignores unknown properties.
- Field visibility is enabled.
- Getter/isGetter/setter auto-detection is disabled.
- JSON names use `@JsonProperty`.

### `Weather`

Nested weather payload.

Fields:

- `humidity`: int, percent, generated from 0 to 100 inclusive.
- `temperature`: int, Fahrenheit, generated from 20 to 120 inclusive.
- `wind_speed`: int, km/h, generated from 0 to 50 inclusive.

### `RainAlert`

Special message emitted to `rain-alerts` when a reading has humidity greater than 70.

Fields:

- `station_id`
- `s_no`
- `status_timestamp`
- `humidity`
- `alert_count`
- `message`, defaulting to `It is raining`

The Processor API implementation sets `alert_count` from a persistent per-station Kafka Streams state store. The DSL example emits alerts with `alert_count = 0`.

### `Json`

Thin wrapper around a single shared Jackson `ObjectMapper`.

Behavior:

- `FAIL_ON_UNKNOWN_PROPERTIES` disabled.
- `write(Object)` serializes or throws `IllegalStateException`.
- `read(String, Class<T>)` deserializes or throws `IllegalArgumentException`.
- `mapper()` exposes the shared mapper.

### `Topics`

Central topic constants, overridable with environment variables:

- `TOPIC_WEATHER`, default `weather-readings`.
- `TOPIC_RAIN`, default `rain-alerts`.

## Weather Station Module

Package: `com.netcentric.weather.station`

Purpose: implements lab Parts A and B.

Files:

- `WeatherStationApp.java`
- `ReadingGenerator.java`
- `StationIdentity.java`
- `pom.xml`
- `Dockerfile`

### `WeatherStationApp`

Main class: `com.netcentric.weather.station.WeatherStationApp`

Responsibilities:

- Resolve a station ID.
- Configure a Kafka producer.
- Generate one reading per interval.
- Randomly drop about 10 percent of generated readings.
- Send non-dropped readings to Kafka topic `weather-readings`.
- Use station ID as Kafka message key.
- Flush producer on shutdown.

Important defaults:

- `KAFKA_BOOTSTRAP_SERVERS`: `127.0.0.1:9092`
- `EMIT_INTERVAL_MS`: `1000`
- `DROP_RATE`: `0.10`

Producer configuration:

- Key serializer: `StringSerializer`
- Value serializer: `StringSerializer`
- `client.id`: `weather-station-<stationId>`
- `acks=all`
- `enable.idempotence=true`
- `retries=3`
- `linger.ms=20`
- `compression.type=gzip`

Kafka keying:

- Key is `String.valueOf(stationId)`.
- This keeps all readings for a station on the same partition.
- That preserves per-station ordering.

Drop behavior:

- A sequence number is consumed before the drop decision.
- If the random drop check succeeds, the message is never produced to Kafka.
- This creates sequence gaps that the SQL analysis can detect.

Logging:

- Startup line includes station ID, bootstrap server, topic, interval, and drop rate.
- Every 10 dropped messages, logs dropped sequence.
- Every 30 sent messages, logs sent count, dropped count, and last JSON payload.
- Shutdown logs generated message count.

### `ReadingGenerator`

Generates one `WeatherMessage` per `next()` call.

Sequence:

- Uses `AtomicLong`.
- Starts at 0.
- `next()` increments with `sequence.incrementAndGet()`.
- `currentSequence()` returns the current sequence value.

Battery distribution:

- `low`: 30 percent.
- `medium`: 40 percent.
- `high`: 30 percent.

Implementation:

- Draws uniform `double p` in `[0,1)`.
- `p < 0.30`: low.
- `0.30 <= p < 0.70`: medium.
- `p >= 0.70`: high.

Weather generation:

- Humidity: random int `0..100` inclusive.
- Temperature: random int `20..120` Fahrenheit inclusive.
- Wind speed: random int `0..50` km/h inclusive.
- Timestamp: `Instant.now().getEpochSecond()`.

Because humidity is uniform over 0 to 100 and rain detection is `humidity > 70`, approximately 30 percent of produced readings become rain alerts.

### `StationIdentity`

Resolves station ID in this order:

1. `STATION_ID` environment variable/system property.
2. Kubernetes StatefulSet pod ordinal from `HOSTNAME`, e.g. `weather-station-3` becomes station ID `4`.
3. Fallback to `1`.

The ordinal-to-ID conversion adds 1, so Kubernetes pods `weather-station-0` through `weather-station-9` become station IDs `1` through `10`.

### Weather Station Dockerfile

Multi-stage Dockerfile:

1. Build stage: `maven:3.9-eclipse-temurin-17`.
2. Runtime stage: `eclipse-temurin:17-jre`.
3. Copies the shaded JAR to `/app/app.jar`.
4. Sets `JAVA_OPTS="-XX:MaxRAMPercentage=75"`.
5. Entrypoint: `java $JAVA_OPTS -jar /app/app.jar`.

The build stage copies all module POMs and all module source directories, then runs `mvn -B -DskipTests package`.

## Rain Processor Module

Package: `com.netcentric.weather.rain`

Purpose: implements lab Part C.

Files:

- `RainDetectorApp.java`
- `RainProcessor.java`
- `RainDetectorDslApp.java`
- `pom.xml`
- `Dockerfile`

### `RainDetectorApp`

Main class: `com.netcentric.weather.rain.RainDetectorApp`

This is the primary implementation. It uses the low-level Kafka Streams Processor API.

Default/configurable settings:

- `KAFKA_BOOTSTRAP_SERVERS`: default `127.0.0.1:9092`
- `STREAMS_APP_ID`: default `rain-detector-app`
- `STREAMS_STATE_DIR`: default `/tmp/kafka-streams`
- `STREAMS_THREADS`: default `1`

Kafka Streams settings:

- Default key serde: string.
- Default value serde: string.
- Processing guarantee: `AT_LEAST_ONCE`.
- State directory configurable by env.

Topology:

```text
weather-readings
  -> source "readings-source"
  -> processor "rain-detector"
  -> state store "rain-alert-counts"
  -> sink "rain-sink"
  -> rain-alerts
```

The app prints `topology.describe()` at startup, starts Kafka Streams, and installs a shutdown hook to close streams. Its uncaught exception handler logs the fatal error and requests `REPLACE_THREAD`.

### `RainProcessor`

Implements `Processor<String, String, String, String>`.

Constants:

- `STORE_NAME = "rain-alert-counts"`
- `RAIN_HUMIDITY_THRESHOLD = 70`

Processing steps:

1. Ignore null record values.
2. Parse JSON into `WeatherMessage`.
3. Skip malformed records and log the problem.
4. If `weather` is null, skip.
5. If humidity is `<= 70`, skip.
6. Convert station ID to string key.
7. Read station alert count from persistent key-value store.
8. Increment and store the alert count.
9. Build `RainAlert`.
10. Log rain event.
11. Forward alert JSON downstream with station ID as key.

This class filters non-rain readings and only forwards rain alerts.

### `RainDetectorDslApp`

Alternative implementation using the high-level Kafka Streams DSL.

Purpose:

- Kept for report comparison.
- Functionally similar but less stateful.

Behavior:

- Reads `weather-readings`.
- Maps JSON to `WeatherMessage`.
- Filters humidity greater than 70.
- Maps to `RainAlert`.
- Writes to `rain-alerts`.

Difference from Processor API implementation:

- Does not use the persistent alert count state store.
- Emits `alert_count = 0`.
- Uses fixed application ID `rain-detector-dsl-app`.

### Rain Processor Dockerfile

Same multi-stage pattern as weather station:

- Build with Maven and Java 17.
- Runtime with Eclipse Temurin 17 JRE.
- Copies `rain-processor.jar`.
- Uses `JAVA_OPTS`.
- Runs `/app/app.jar`.

## Central Station Module

Package: `com.netcentric.weather.central`

Purpose: implements lab Part D and supports Part E by populating PostgreSQL.

Files:

- `CentralStationApp.java`
- `Database.java`
- `ReadingsConsumer.java`
- `BatchWriter.java`
- `RainAlertsConsumer.java`
- `RainAlertWriter.java`
- `pom.xml`
- `Dockerfile`

### `CentralStationApp`

Main class: `com.netcentric.weather.central.CentralStationApp`

Responsibilities:

- Connect to PostgreSQL.
- Initialize schema.
- Start weather readings consumer thread.
- Optionally start rain alerts consumer thread.
- Handle graceful shutdown.

Connections:

- Uses one JDBC connection for readings.
- Uses a separate JDBC connection for rain alerts because JDBC connections are not thread-safe.

Config:

- `CONSUME_RAIN_ALERTS`: default `true`.

Shutdown behavior:

- Logs shutdown request.
- Tells consumers to stop.
- Joins consumer threads with 15 second timeout.
- Closes JDBC connections.

### `Database`

Handles JDBC connection and schema creation.

Config:

- `DB_URL`: default `jdbc:postgresql://localhost:5432/weather`
- `DB_USER`: default `weather`
- `DB_PASSWORD`: required
- `DB_SSLMODE`: optional, e.g. `require` for Aiven

Connection behavior:

- Builds `Properties` for JDBC user/password/sslmode.
- Retries connection up to 30 attempts.
- Sleeps 2 seconds between attempts.
- Retries for roughly one minute.
- Sets `autoCommit(false)`.
- Logs successful connection.

Schema creation:

- Runs idempotent DDL with `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, and `CREATE OR REPLACE VIEW`.
- Commits after schema creation.

Tables/view:

- `weather_readings`
- `rain_alerts`
- `latest_station_status`

### `ReadingsConsumer`

Consumes `weather-readings` and persists records through `BatchWriter`.

Config:

- `BATCH_SIZE`: default `5000`
- `FLUSH_INTERVAL_MS`: default `10000`
- `KAFKA_BOOTSTRAP_SERVERS`: default `127.0.0.1:9092`
- `CONSUMER_GROUP`: default `central-station`

Kafka consumer settings:

- Key/value deserializers: `StringDeserializer`.
- `auto.offset.reset=earliest`.
- `enable.auto.commit=false`.
- `max.poll.records=2000`.

Delivery semantics:

1. Poll records.
2. Parse JSON into `WeatherMessage`.
3. Add valid messages to batch buffer.
4. Flush when buffer reaches `BATCH_SIZE`.
5. Also flush when `FLUSH_INTERVAL_MS` elapses and there are pending records.
6. Commit PostgreSQL transaction.
7. Commit Kafka offsets with `commitSync()`.

This gives at-least-once delivery. If the process dies after DB commit but before offset commit, records can be replayed. The database unique constraint plus `ON CONFLICT DO NOTHING` makes replay idempotent.

Malformed records are skipped and logged.

Shutdown:

- `shutdown()` sets running false and wakes the consumer.
- On close, tries a best-effort `commitSync()` and closes the Kafka consumer.

### `BatchWriter`

Buffers and batch-inserts weather readings.

SQL target:

- `weather_readings`

Insert fields:

- `station_id`
- `sequence_number`
- `battery_status`
- `timestamp`
- `humidity`
- `temperature`
- `wind_speed`

SQL behavior:

- Uses prepared statement.
- Uses JDBC batch.
- Uses `ON CONFLICT (station_id, sequence_number) DO NOTHING`.
- Commits the DB transaction before Kafka offsets are committed by `ReadingsConsumer`.

Methods:

- `add(WeatherMessage)`
- `pending()`
- `isFull()`
- `flush()`
- `totalWritten()`
- `close()`

`flush()` returns the number of buffered rows sent to JDBC and logs:

```text
[central] batch flushed: <n> rows (total <totalWritten>)
```

### `RainAlertsConsumer`

Optionally consumes `rain-alerts` and archives alerts.

Config:

- `KAFKA_BOOTSTRAP_SERVERS`: default `127.0.0.1:9092`
- `CONSUMER_GROUP`: default `central-station`; actual group for rain alerts is `<CONSUMER_GROUP>-rain`.

Consumer settings:

- `auto.offset.reset=earliest`.
- `enable.auto.commit=false`.

Behavior:

- Polls every 500 ms.
- Parses records into `RainAlert`.
- Adds alerts to `RainAlertWriter`.
- Flushes when pending alerts reach 500.
- Also flushes when records are returned and there is any pending alert.
- Commits offsets after DB flush.

Malformed rain alert records are skipped and logged.

### `RainAlertWriter`

Writes rain alerts to PostgreSQL.

SQL target:

- `rain_alerts`

Insert fields:

- `station_id`
- `sequence_number`
- `timestamp`
- `humidity`

SQL behavior:

- Uses prepared batch.
- Uses `ON CONFLICT (station_id, sequence_number) DO NOTHING`.
- Commits after executing the batch.

### Central Station Dockerfile

Same multi-stage Java 17/Maven build pattern:

- Build all modules.
- Copy `central-station.jar` to `/app/app.jar`.
- Run with Eclipse Temurin 17 JRE.

## Database Schema

The schema exists in two places:

- Runtime schema creation in `Database.initSchema`.
- Reference SQL in `sql/schema.sql`.

The two definitions are aligned.

### `weather_readings`

Columns:

- `id BIGSERIAL PRIMARY KEY`
- `station_id BIGINT NOT NULL`
- `sequence_number BIGINT NOT NULL`
- `battery_status VARCHAR(10) NOT NULL`
- `timestamp BIGINT NOT NULL`
- `humidity INT`
- `temperature INT`
- `wind_speed INT`
- `ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()`

Constraint:

- `uq_station_sequence UNIQUE (station_id, sequence_number)`

Indexes:

- `idx_readings_station_seq ON weather_readings (station_id, sequence_number DESC)`
- `idx_readings_battery ON weather_readings (station_id, battery_status)`

Purpose:

- Stores all non-dropped station readings.
- Unique constraint makes replay safe.
- Station/sequence index supports latest status and gap analysis.
- Battery index supports distribution queries.

### `rain_alerts`

Columns:

- `id BIGSERIAL PRIMARY KEY`
- `station_id BIGINT NOT NULL`
- `sequence_number BIGINT NOT NULL`
- `timestamp BIGINT NOT NULL`
- `humidity INT NOT NULL`
- `detected_at TIMESTAMPTZ NOT NULL DEFAULT now()`

Constraint:

- `uq_alert_station_sequence UNIQUE (station_id, sequence_number)`

Purpose:

- Archives special rain alerts emitted by Kafka Streams.
- Unique constraint makes alert replay safe.

### `latest_station_status`

View:

```sql
SELECT DISTINCT ON (station_id)
       station_id, sequence_number, battery_status, timestamp,
       humidity, temperature, wind_speed, ingested_at
FROM weather_readings
ORDER BY station_id, sequence_number DESC;
```

Purpose:

- Answers latest status per station directly from PostgreSQL.

## Analysis SQL

Main file: `sql/analysis_queries.sql`

Duplicate root copy: `analysis_queries.sql`

Queries included:

1. Battery status distribution per station.
2. Pivoted battery distribution per station.
3. Global battery distribution across all stations.
4. Dropped messages per station.
5. Cluster-wide drop rate.
6. Missing sequence numbers for station 1.
7. Latest weather status per station.
8. Rain validation: readings with humidity over 70 compared to stored alerts.
9. Average/min/max weather per station.
10. Ingestion throughput per minute.

Expected behavior:

- Battery converges near low 30 percent, medium 40 percent, high 30 percent.
- Drop rate converges near 10 percent.
- Rain alerts should match readings where humidity is greater than 70, assuming both consumers have caught up.
- Ten stations at 1 reading/sec with 10 percent dropped produce roughly 540 stored readings per minute.

Run in Kubernetes:

```bash
kubectl -n weather exec -it deploy/postgres -- \
  psql -U weather -d weather -f - < sql/analysis_queries.sql
```

Interactive:

```bash
kubectl -n weather exec -it deploy/postgres -- psql -U weather -d weather
```

## Local Docker Compose

File: `docker-compose.yml`

Services:

- `zookeeper`
- `kafka`
- `postgres`
- `rain-processor`
- `central-station`
- `weather-station`

Images:

- Zookeeper: `bitnami/zookeeper:3.9`
- Kafka: `bitnami/kafka:3.6`
- PostgreSQL: `postgres:16`
- Java services are built from local Dockerfiles.

Kafka local settings:

- Uses Zookeeper mode.
- `KAFKA_ENABLE_KRAFT=no`.
- Listener: `PLAINTEXT://:9092`.
- Advertised listener: `PLAINTEXT://kafka:9092`.
- Auto-create topics enabled.
- Default number of partitions: 10.

PostgreSQL local settings:

- Database: `weather`
- User: `weather`
- Password: `weatherpass`
- Host port: `5432`
- Persistent named volume: `pgdata`

Central station env:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `DB_URL=jdbc:postgresql://postgres:5432/weather`
- `DB_USER=weather`
- `DB_PASSWORD=weatherpass`
- `BATCH_SIZE=5000`
- `FLUSH_INTERVAL_MS=10000`

Important compose limitation:

- `weather-station` is defined with `STATION_ID=1`.
- The file comments warn that `docker compose up --scale weather-station=10` will not create distinct station IDs.
- For local compose, distinct stations would need explicit service definitions or external env overrides.
- The cloud helper script generates distinct station services.

Local startup from README:

```bash
docker compose up -d zookeeper kafka postgres
docker compose up -d --build rain-processor central-station weather-station
docker compose logs -f central-station
```

## Bare-JAR Runtime

After building with Maven, the services can run without Docker if Kafka and PostgreSQL are already available.

Station:

```bash
STATION_ID=1 java -jar weather-station/target/weather-station.jar
```

Rain processor:

```bash
java -jar rain-processor/target/rain-processor.jar
```

Central station:

```bash
DB_URL=jdbc:postgresql://localhost:5432/weather \
DB_USER=weather \
DB_PASSWORD=weatherpass \
java -jar central-station/target/central-station.jar
```

## Kubernetes Deployment

Directory: `k8s/`

Files:

- `00-namespace-config.yaml`
- `01-zookeeper.yaml`
- `02-kafka.yaml`
- `03-postgres.yaml`
- `04-weather-stations.yaml`
- `05-rain-processor.yaml`
- `06-central-station.yaml`
- `deploy.sh`

Namespace:

- `weather`

### ConfigMap and Secret

File: `k8s/00-namespace-config.yaml`

ConfigMap: `weather-config`

Values:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `TOPIC_WEATHER=weather-readings`
- `TOPIC_RAIN=rain-alerts`
- `EMIT_INTERVAL_MS=1000`
- `DROP_RATE=0.10`
- `BATCH_SIZE=5000`
- `FLUSH_INTERVAL_MS=10000`
- `DB_URL=jdbc:postgresql://postgres:5432/weather`

Secret: `db-credentials`

Values:

- `DB_USER=weather`
- `DB_PASSWORD=weatherpass`

The manifest includes a lab/dev password in `stringData`, while comments describe creating the secret explicitly with `kubectl create secret`.

### Zookeeper

File: `k8s/01-zookeeper.yaml`

Resources:

- Deployment `zookeeper`, 1 replica.
- Service `zookeeper` on port 2181.

Container:

- Image `bitnami/zookeeper:3.9`.
- `ALLOW_ANONYMOUS_LOGIN=yes`.
- Readiness probe on TCP 2181.

Resources:

- Requests: CPU `100m`, memory `256Mi`.
- Limits: CPU `500m`, memory `768Mi`.

### Kafka

File: `k8s/02-kafka.yaml`

Resources:

- Deployment `kafka`, 1 replica.
- Service `kafka` on port 9092.
- Job `kafka-topics-init`.

Container:

- Image `bitnami/kafka:3.6`.
- Uses Zookeeper mode.
- Advertises `PLAINTEXT://kafka.weather.svc.cluster.local:9092`.
- Auto-create topics enabled.
- Default partitions: 10.
- Offset topic replication factor: 1.
- Heap opts: `-Xmx768m -Xms512m`.
- Data volume: `emptyDir` mounted at `/bitnami/kafka`.

Readiness:

- TCP socket on 9092.
- Initial delay 20 seconds.

Resources:

- Requests: CPU `300m`, memory `768Mi`.
- Limits: CPU `1`, memory `1536Mi`.

Topic init job:

- Waits for Kafka.
- Creates `weather-readings` with 10 partitions, replication factor 1.
- Creates `rain-alerts` with 10 partitions, replication factor 1.
- Describes topics.

### PostgreSQL

File: `k8s/03-postgres.yaml`

Resources:

- PersistentVolume `postgres-pv`.
- PersistentVolumeClaim `postgres-pvc`.
- Deployment `postgres`, 1 replica.
- Service `postgres` on port 5432.

PV:

- Capacity: 5Gi.
- Access mode: `ReadWriteOnce`.
- Reclaim policy: `Retain`.
- Storage class: `standard`.
- Host path: `/data/weather-postgres`.

Postgres deployment:

- Image `postgres:16`.
- Strategy: `Recreate`.
- Database: `weather`.
- User/password from `db-credentials`.
- `PGDATA=/var/lib/postgresql/data/pgdata`.
- PVC mounted at `/var/lib/postgresql/data`.
- Readiness via `pg_isready -U weather`.

Resources:

- Requests: CPU `200m`, memory `512Mi`.
- Limits: CPU `1`, memory `1Gi`.

### Weather Stations

File: `k8s/04-weather-stations.yaml`

Resources:

- Headless Service `weather-station`.
- StatefulSet `weather-station`.

StatefulSet:

- Replicas: 10.
- `podManagementPolicy: Parallel`.
- Image: `weather-station:1.0.0`.
- `imagePullPolicy: IfNotPresent`.
- Env from `weather-config`.
- `HOSTNAME` injected from pod metadata name.

Why StatefulSet:

- Stable pod names are used to derive station IDs.
- `weather-station-0` becomes station 1.
- `weather-station-9` becomes station 10.

Resources:

- Requests: CPU `50m`, memory `128Mi`.
- Limits: CPU `300m`, memory `384Mi`.

### Rain Processor

File: `k8s/05-rain-processor.yaml`

Resource:

- Deployment `rain-processor`.

Settings:

- Replicas: 1.
- Can scale up, bounded by 10 topic partitions.
- Image: `rain-processor:1.0.0`.
- Env from `weather-config`.
- `STREAMS_STATE_DIR=/tmp/kafka-streams`.
- `emptyDir` mounted at `/tmp/kafka-streams`.

Resources:

- Requests: CPU `100m`, memory `256Mi`.
- Limits: CPU `500m`, memory `768Mi`.

### Central Station

File: `k8s/06-central-station.yaml`

Resources:

- Deployment `central-station`.
- Service `central-station` on port 8080.

Settings:

- Replicas: 1.
- Can scale to at most number of partitions if needed.
- Image: `central-station:1.0.0`.
- Env from `weather-config`.
- Env from `db-credentials` secret.

Resources:

- Requests: CPU `200m`, memory `384Mi`.
- Limits: CPU `1`, memory `1Gi`.

The service exposes port 8080 even though the Java app currently does not define an HTTP server.

### Kubernetes Deploy Script

File: `k8s/deploy.sh`

Behavior:

1. Changes directory to repo root.
2. Points Docker CLI at Minikube daemon with `eval "$(minikube docker-env)"`.
3. Builds local images:
   - `weather-station:1.0.0`
   - `rain-processor:1.0.0`
   - `central-station:1.0.0`
4. Applies manifests:
   - namespace/config/secret
   - zookeeper
   - postgres
   - kafka
5. Waits for Kafka rollout.
6. Applies rain processor.
7. Applies central station.
8. Applies weather stations.
9. Waits for station StatefulSet rollout.
10. Prints pods with `kubectl -n weather get pods -o wide`.

Run:

```bash
minikube start --cpus=4 --memory=8192
./k8s/deploy.sh
```

Inspect:

```bash
kubectl -n weather get pods
kubectl -n weather logs -f statefulset/weather-station
kubectl -n weather logs -f deploy/central-station
```

Teardown:

```bash
kubectl delete namespace weather
kubectl delete pv postgres-pv
```

## Cloud Bonus Deployment

Directory: `cloud/`

Files:

- `CLOUD-DEPLOYMENT.md`
- `docker-compose.central.yml`
- `central.env.example`
- `stations-compose.sh`

There is also a duplicate/copy of the deployment guide at `docs/CLOUD-DEPLOYMENT.md`.

### Cloud Topology

The bonus design uses:

- VM 1: weather stations.
- VM 2: central base station machine.
- Aiven Cloud: managed PostgreSQL.

Flow:

```text
VM #1 station-1..station-10
  -> VM #2 Kafka/Zookeeper
  -> VM #2 rain-processor and central-station
  -> Aiven PostgreSQL over TLS
```

The lab requirement that stations and base station run on different machines is satisfied by splitting station containers and central services across two VMs.

### `cloud/docker-compose.central.yml`

Runs on central VM.

Services:

- `zookeeper`
- `kafka`
- `rain-processor`
- `central-station`

Kafka:

- Exposes `9092:9092` for stations VM.
- Also uses internal listener `INTERNAL://:29092`.
- Advertised listeners:
  - External: `PLAINTEXT://${CENTRAL_VM_IP}:9092`
  - Internal: `INTERNAL://kafka:29092`
- Inter-broker listener: `INTERNAL`.
- Partitions: 10.
- Auto-create topics enabled.

Rain processor:

- Image `rain-processor:1.0.0`.
- `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.

Central station:

- Image `central-station:1.0.0`.
- `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.
- `DB_URL`, `DB_USER`, `DB_PASSWORD` from env file.
- `DB_SSLMODE=require`.
- `BATCH_SIZE=5000`.
- `FLUSH_INTERVAL_MS=10000`.

### `cloud/central.env.example`

Template for central VM env file:

```text
CENTRAL_VM_IP=20.30.40.50
DB_URL=jdbc:postgresql://weather-db-yourproject.a.aivencloud.com:12345/defaultdb
DB_USER=avnadmin
DB_PASSWORD=REPLACE_ME
```

Instructions say to copy to `central.env`, set real values, `chmod 600`, and never commit it.

### `cloud/stations-compose.sh`

Generates `docker-compose.stations.yml` with N station services.

Usage:

```bash
./cloud/stations-compose.sh 10 "$KAFKA_BOOTSTRAP_SERVERS"
```

Behavior:

- First argument: number of station services, default 10.
- Second argument: Kafka bootstrap server, default from `KAFKA_BOOTSTRAP_SERVERS`.
- Creates services `station-1` through `station-N`.
- Each service uses image `${IMAGE:-weather-station:1.0.0}`.
- Each service has a distinct `STATION_ID`.
- Each service uses `EMIT_INTERVAL_MS=1000`.
- Each service uses `DROP_RATE=0.10`.

### Cloud Security Notes

The guide recommends:

- SSH port 22 open only to admin IP.
- Kafka port 9092 on central VM open only to stations VM IP.
- Aiven PostgreSQL access restricted to both VM IPs.
- Database TLS via `DB_SSLMODE=require`.
- No credentials committed to source.

## Documentation

### `README.md`

The README is the main quick-start and architecture overview. It includes:

- System diagram.
- Repo layout table.
- Step-by-step message flow.
- Environment variable table.
- Build instructions.
- Docker Compose instructions.
- Bare Java instructions.
- Kafka console consumer sanity checks.
- Kubernetes deployment instructions.
- Analysis query instructions.
- Expected results after about 10 minutes.

### `docs/REPORT.md`

Report skeleton for the lab submission.

Sections:

1. Introduction.
2. System architecture.
3. Weather station mock.
4. Kafka integration.
5. Rain detection.
6. Central station.
7. Historical analysis.
8. Kubernetes deployment.
9. Cloud deployment bonus.
10. Challenges and lessons learned.
11. Conclusion.

The skeleton tells the author where to paste screenshots, topology output, query results, and deployment evidence.

### `docs/CLOUD-DEPLOYMENT.md`

Same content as `cloud/CLOUD-DEPLOYMENT.md` based on the read context. It documents the bonus cloud setup.

## Configuration Reference

Environment variables used by the code:

| Variable | Default | Used by | Meaning |
|---|---:|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | all Kafka apps | Kafka bootstrap servers |
| `TOPIC_WEATHER` | `weather-readings` | all | Weather readings topic |
| `TOPIC_RAIN` | `rain-alerts` | all | Rain alerts topic |
| `STATION_ID` | derived/fallback | weather station | Explicit station ID |
| `HOSTNAME` | empty | weather station | Used to derive StatefulSet ordinal |
| `EMIT_INTERVAL_MS` | `1000` | weather station | Sampling cadence |
| `DROP_RATE` | `0.10` | weather station | Probability generated sample is not sent |
| `STREAMS_APP_ID` | `rain-detector-app` | rain processor | Kafka Streams application ID |
| `STREAMS_STATE_DIR` | `/tmp/kafka-streams` | rain processor | Kafka Streams local state path |
| `STREAMS_THREADS` | `1` | rain processor | Number of stream threads |
| `DB_URL` | `jdbc:postgresql://localhost:5432/weather` | central station | JDBC URL |
| `DB_USER` | `weather` | central station | Database user |
| `DB_PASSWORD` | required | central station | Database password |
| `DB_SSLMODE` | unset | central station | PostgreSQL SSL mode |
| `BATCH_SIZE` | `5000` | central station | Readings batch size |
| `FLUSH_INTERVAL_MS` | `10000` | central station | Time-based readings flush interval |
| `CONSUMER_GROUP` | `central-station` | central station | Kafka consumer group base |
| `CONSUME_RAIN_ALERTS` | `true` | central station | Whether to archive rain alerts |

## Kafka Topics

Default topics:

- `weather-readings`
- `rain-alerts`

Partitioning:

- Kubernetes topic init job creates both topics with 10 partitions.
- Docker Compose config sets Kafka default partitions to 10 and auto-create topics.
- Ten partitions align with ten stations.

Message keys:

- Weather station producer uses station ID as key.
- Rain processor forwards alerts using station ID as key.

Ordering:

- Per-station ordering is preserved because each station ID maps consistently to one Kafka partition.

## Message Semantics

### Weather Reading

Produced by:

- `weather-station`

Topic:

- `weather-readings`

Key:

- station ID string.

Value:

- JSON serialized `WeatherMessage`.

Example shape:

```json
{
  "station_id": 1,
  "s_no": 42,
  "battery_status": "medium",
  "status_timestamp": 1760000000,
  "weather": {
    "humidity": 81,
    "temperature": 73,
    "wind_speed": 12
  }
}
```

### Rain Alert

Produced by:

- `rain-processor`

Topic:

- `rain-alerts`

Key:

- station ID string.

Value:

- JSON serialized `RainAlert`.

Produced only when:

- `weather.humidity > 70`

Example shape:

```json
{
  "station_id": 1,
  "s_no": 42,
  "status_timestamp": 1760000000,
  "humidity": 81,
  "alert_count": 13,
  "message": "It is raining"
}
```

## Reliability and Delivery Guarantees

Weather station producer:

- Uses `acks=all`.
- Enables idempotence.
- Retries three times.
- Uses gzip compression.
- Does not block for send acknowledgement except via callback logging and final flush.

Rain processor:

- Kafka Streams at-least-once processing guarantee.
- Persistent state store for alert counts.
- Skips malformed records instead of crashing topology.
- Replaces stream thread on uncaught exception.

Central station readings consumer:

- Manual offset commits.
- PostgreSQL commit happens before Kafka offset commit.
- At-least-once consumption.
- Idempotent database writes via unique constraint and `ON CONFLICT DO NOTHING`.

Central station rain alert consumer:

- Manual offset commits.
- Writes and commits DB batch before Kafka offset commit.
- Idempotent alert writes via unique constraint and `ON CONFLICT DO NOTHING`.

Database:

- Unique `(station_id, sequence_number)` protects against duplicate replay.
- `latest_station_status` derives latest state directly from history.

## Expected Runtime Numbers

At steady state:

- 10 stations.
- 1 generated sample per second per station.
- 10 generated samples per second total.
- 600 generated samples per minute total.
- 10 percent dropped by station apps.
- About 540 readings per minute stored.
- Humidity generated uniformly from 0 to 100 inclusive.
- Humidity greater than 70 is about 30 out of 101 integer values, so roughly 29.7 percent of produced readings trigger rain alerts.
- README rounds this to about 30 percent.
- Battery distribution should converge to 30/40/30 as sample size grows.

## Commands

Build:

```bash
mvn -B -DskipTests package
```

Local Docker Compose:

```bash
docker compose up -d zookeeper kafka postgres
docker compose up -d --build rain-processor central-station weather-station
docker compose logs -f central-station
```

Kafka console consumer examples:

```bash
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
  --topic weather-readings --from-beginning

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
  --topic rain-alerts --from-beginning
```

Kubernetes:

```bash
minikube start --cpus=4 --memory=8192
./k8s/deploy.sh
kubectl -n weather get pods
kubectl -n weather logs -f statefulset/weather-station
kubectl -n weather logs -f deploy/central-station
```

Kubernetes analysis:

```bash
kubectl -n weather exec -it deploy/postgres -- \
  psql -U weather -d weather -f - < sql/analysis_queries.sql
```

Cloud central VM:

```bash
docker build -t rain-processor:1.0.0  -f rain-processor/Dockerfile  .
docker build -t central-station:1.0.0 -f central-station/Dockerfile .
cp cloud/central.env.example central.env
chmod 600 central.env
docker compose --env-file central.env -f cloud/docker-compose.central.yml up -d
```

Cloud stations VM:

```bash
docker build -t weather-station:1.0.0 -f weather-station/Dockerfile .
export KAFKA_BOOTSTRAP_SERVERS=<CENTRAL_VM_IP>:9092
./cloud/stations-compose.sh 10 "$KAFKA_BOOTSTRAP_SERVERS"
docker compose -f docker-compose.stations.yml up -d
```

## Known Notes and Caveats

- No automated tests are present.
- `target/` directories are present and untracked.
- `analysis_queries.sql` is duplicated at repo root and under `sql/`.
- `docs/CLOUD-DEPLOYMENT.md` appears to duplicate `cloud/CLOUD-DEPLOYMENT.md`.
- Local `docker-compose.yml` defines only one `weather-station` service with `STATION_ID=1`; scaling it directly will not create unique station IDs.
- Kubernetes `central-station` Service exposes port 8080, but the Java central station app currently does not expose an HTTP server.
- Kubernetes Kafka uses `emptyDir` for broker data, so Kafka data is not persisted across pod recreation.
- Kubernetes PostgreSQL uses a retained hostPath PV, suitable for Minikube/kind-style single-node labs, not a production storage setup.
- The Kubernetes Secret manifest contains a sample password for lab convenience.
- The Processor API rain detector is the real stateful implementation. The DSL app is a comparison/demo and does not maintain alert counts.
- The station producer logs asynchronous send failures but does not retry application-level failed records beyond Kafka producer retries.
- The central readings consumer's final `finally` block attempts `commitSync()` even if no final DB flush was triggered in the try loop; normal close of `BatchWriter` flushes pending records before leaving the try-with-resources block.
- PDF metadata visible from raw strings shows the PDF is version 1.3 and was produced by macOS Quartz PDFContext, but clean assignment text was not extractable with the available local tools.

## How Lab Requirements Are Represented

Part A, weather station mock:

- `ReadingGenerator` produces the exact JSON-compatible message shape.
- Sequence numbers are monotonic per station.
- Battery probability is 30/40/30.
- Timestamps are Unix seconds.
- Weather values are randomized.
- `WeatherStationApp` emits one sample per configured interval and drops approximately 10 percent.

Part B, Kafka:

- `WeatherStationApp` produces to `weather-readings`.
- Station ID is Kafka key for per-station ordering.
- Producer uses durable/idempotent settings.

Part C, rain detection:

- `RainDetectorApp` builds a Processor API topology.
- `RainProcessor` detects humidity greater than 70.
- Alerts are forwarded to `rain-alerts`.
- Stateful per-station alert counts are kept in a persistent state store.
- `RainDetectorDslApp` is present for DSL comparison.

Part D, central station:

- `CentralStationApp` starts central consumers.
- `ReadingsConsumer` uses manual offset commit.
- `BatchWriter` performs batch inserts with batch size default 5000.
- PostgreSQL schema is initialized automatically.
- Rain alerts can also be persisted.

Part E, analysis:

- `sql/analysis_queries.sql` contains battery distribution, drop rate, latest status, rain validation, and extra analytics.

Part F, Kubernetes:

- `k8s/` contains namespace, config, secret, Zookeeper, Kafka, PostgreSQL, weather station StatefulSet, rain processor deployment, central station deployment, and deploy script.

Bonus:

- `cloud/` contains two-VM deployment guide and compose assets.
- Central VM runs Kafka, rain processor, and central station.
- Stations VM runs generated station services.
- Aiven PostgreSQL is used as managed database with TLS.
