# Bonus - Cloud Deployment (no Kubernetes)

Two VMs + one managed database. Fill in the bracketed values with what you actually used.

## 1. Topology

```
VM #1  weather-stations           VM #2  central base station        Aiven Cloud
+------------------------+        +---------------------------+     +--------------+
| station-1 .. station-10|  ---->  | Zookeeper + Kafka broker  | --> | PostgreSQL   |
| (10 docker containers) |  :9092  | rain-processor            | TLS | (managed)    |
|                        |        | central-station            |     +--------------+
+------------------------+        +---------------------------+
```

The stations and the base station run on **different machines**, as required.

## 2. Machine specifications (fill in)

| Role | Provider | Size | vCPU / RAM | OS | IP |
|---|---|---|---|---|---|
| Stations VM | [AWS EC2 / Azure / GCP] | [t3.small] | [2 / 2 GB] | Ubuntu 22.04 | [x.x.x.x] |
| Central VM  | [AWS EC2 / Azure / GCP] | [t3.medium] | [2 / 4 GB] | Ubuntu 22.04 | [y.y.y.y] |
| Database    | Aiven Cloud | free/hobbyist | - | PostgreSQL 16 | [host:port] |

## 3. Network configuration

| Port | Where | Source allowed | Why |
|---|---|---|---|
| 22   | both VMs | your admin IP only | SSH |
| 9092 | central VM | **stations VM private/public IP only** | Kafka producers |
| 5432 (Aiven port) | Aiven | both VM IPs (Aiven "Allowed IP" list) | JDBC over TLS |

Everything else stays closed. Kafka is *not* exposed to 0.0.0.0 — an open broker is
an open write endpoint for anyone.

## 4. Deployment steps

### 4.1 Managed database (Aiven)
1. Create a PostgreSQL service (free/trial plan), region close to the VMs.
2. Copy the *service URI* — host, port, database, user, password.
3. Restrict access to the two VM IPs.
4. Convert the URI to a JDBC URL:
   `jdbc:postgresql://<host>:<port>/<db>` with `DB_SSLMODE=require`.
5. Load the schema (optional — the app creates it on startup):
   `psql "<service-uri>" -f sql/schema.sql`

### 4.2 Central VM
```bash
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
git clone <your-repo> && cd weather-monitoring
docker build -t rain-processor:1.0.0  -f rain-processor/Dockerfile  .
docker build -t central-station:1.0.0 -f central-station/Dockerfile .
cp cloud/central.env.example central.env && chmod 600 central.env && nano central.env
docker compose --env-file central.env -f cloud/docker-compose.central.yml up -d
docker compose -f cloud/docker-compose.central.yml logs -f central-station
```
`CENTRAL_VM_IP` must be the address the stations VM can reach (private IP if both VMs
share a VNet/VPC, otherwise the public IP).

### 4.3 Stations VM
```bash
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
git clone <your-repo> && cd weather-monitoring
docker build -t weather-station:1.0.0 -f weather-station/Dockerfile .
export KAFKA_BOOTSTRAP_SERVERS=<CENTRAL_VM_IP>:9092
./cloud/stations-compose.sh 10 "$KAFKA_BOOTSTRAP_SERVERS"
docker compose -f docker-compose.stations.yml up -d
docker compose -f docker-compose.stations.yml logs -f station-1
```

### 4.4 Verify connectivity
```bash
# from the stations VM
nc -zv <CENTRAL_VM_IP> 9092

# on the central VM: are messages arriving?
docker exec -it $(docker ps -qf name=kafka) \
  kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic weather-readings --max-messages 5

# is the DB filling up?
psql "<aiven-uri>" -c "SELECT station_id, COUNT(*) FROM weather_readings GROUP BY 1 ORDER BY 1;"
```

## 5. Evidence to attach to the report
- `docker compose logs station-N` showing `sent=... dropped=...`
- `docker compose logs central-station` showing `batch flushed: N rows`
- `kafka-console-consumer` output on `weather-readings` and `rain-alerts`
- psql output of the two analysis queries
- Aiven dashboard screenshot showing the service running + connection count
- Security group / NSG screenshot showing only 22 and 9092 open, and to whom

## 6. Secrets
No credential appears in the source tree. The central station reads `DB_URL`,
`DB_USER`, `DB_PASSWORD`, `DB_SSLMODE` from the environment: a Kubernetes `Secret`
in the k8s deployment, a `chmod 600` env file on the cloud VM. `central.env` is
listed in `.gitignore`.
