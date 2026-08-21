# Lab 4 — Weather Stations Monitoring — Technical Report

**Course:** Net Centric Computing, Fall 2025–2026
**Team:** [names / IDs]
**Date:** [date]

## 1. Introduction
Purpose of the system, the IoT streaming problem it addresses, and a one-paragraph
summary of what was built.

## 2. System architecture
Insert the architecture diagram. Describe the three stages: data acquisition
(10 station services), data processing (Kafka + Streams processor), data persistence
(PostgreSQL). Explain why Kafka sits between them (decoupling, buffering, replay).

## 3. Weather station mock (Part A)
- 1 message/second, exact schema of the handout.
- `s_no` monotonic per station, incremented even for dropped messages — justify why.
- Battery distribution 30/40/30: implementation with a single uniform draw.
- 10 % random drop.
- Screenshot: station logs.

## 4. Kafka integration (Part B)
- Producer configuration table (`acks=all`, idempotence, linger, gzip, key = station_id).
- Why keying by station id preserves per-station ordering.
- Screenshot: `kafka-console-consumer` output on `weather-readings`.

## 5. Rain detection (Part C)
- Topology description (paste the output of `topology.describe()`).
- `RainProcessor` logic: parse → filter humidity > 70 → state store counter → forward.
- Processor API vs DSL: trade-offs (explicit state/forwarding control vs conciseness).
- Screenshot: `rain-alerts` topic contents.

## 6. Central station (Part D)
- Two consumer threads, manual offset commit.
- Batch insert of 5000 rows: measured effect on I/O vs row-by-row inserts.
- Delivery semantics: DB commit → offset commit → at-least-once, made idempotent by
  `UNIQUE(station_id, sequence_number)`.
- Database schema (ER / DDL) and index rationale.
- Screenshot: `batch flushed: 5000 rows` log lines.

## 7. Historical analysis (Part E)
Paste each query and its result table:
1. battery status distribution per station — confirm ≈30/40/30
2. dropped messages per station — confirm ≈10 %
3. latest status per station
4. rain alert cross-check

Comment on the deviation from the theoretical values and why it shrinks with sample size.

## 8. Kubernetes deployment (Part F)
- Dockerfiles (multi-stage) and image sizes.
- Manifest inventory and why a StatefulSet is used for the stations.
- PV/PVC for the database; what happens to data when the pod restarts.
- `kubectl get pods -o wide` screenshot.
- Failure test: delete a station pod / the central station pod and show recovery.

## 9. Cloud deployment (Bonus)
Fill in from `cloud/CLOUD-DEPLOYMENT.md`: provider, VM specs, network configuration,
step-by-step deployment, Aiven managed database, and the connectivity evidence.

## 10. Challenges and lessons learned
E.g. Kafka `advertised.listeners` across machines, consumer rebalance during long
batch flushes, back-pressure, secret management.

## 11. Conclusion
