-- =====================================================================
-- PART E : Historical Weather Statuses Analysis
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) BATTERY STATUS DISTRIBUTION PER STATION
--    Expected: low = 30%, medium = 40%, high = 30%
-- ---------------------------------------------------------------------
SELECT station_id,
       battery_status,
       COUNT(*)                                                       AS messages,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2)
                                                                      AS percentage
FROM weather_readings
GROUP BY station_id, battery_status
ORDER BY station_id,
         CASE battery_status WHEN 'low' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END;

-- Pivoted version (one row per station) - easier to eyeball 30/40/30
SELECT station_id,
       COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) FILTER (WHERE battery_status = 'low')    / COUNT(*), 2) AS low_pct,
       ROUND(100.0 * COUNT(*) FILTER (WHERE battery_status = 'medium') / COUNT(*), 2) AS medium_pct,
       ROUND(100.0 * COUNT(*) FILTER (WHERE battery_status = 'high')   / COUNT(*), 2) AS high_pct
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;

-- Global distribution across the whole cluster
SELECT battery_status,
       COUNT(*) AS messages,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM weather_readings
GROUP BY battery_status
ORDER BY 3 DESC;

-- ---------------------------------------------------------------------
-- 2) DROPPED MESSAGES PER STATION
--    Every station increments s_no for EVERY sample, including the ones it
--    deliberately drops. So the highest s_no stored = number of messages the
--    station generated (expected), and COUNT(*) = number actually received.
--    Expected drop rate = 10%.
-- ---------------------------------------------------------------------
SELECT station_id,
       MAX(sequence_number)                             AS expected_messages,
       COUNT(*)                                         AS received_messages,
       MAX(sequence_number) - COUNT(*)                  AS dropped_messages,
       ROUND(100.0 * (MAX(sequence_number) - COUNT(*)) / MAX(sequence_number), 2)
                                                        AS drop_rate_pct
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;

-- Cluster-wide drop rate
SELECT SUM(expected)             AS expected_total,
       SUM(received)             AS received_total,
       SUM(expected - received)  AS dropped_total,
       ROUND(100.0 * SUM(expected - received) / SUM(expected), 2) AS drop_rate_pct
FROM (SELECT station_id,
             MAX(sequence_number) AS expected,
             COUNT(*)             AS received
      FROM weather_readings
      GROUP BY station_id) per_station;

-- Which exact sequence numbers are missing for a given station (gap analysis)
-- (change 1 to the station you want to inspect)
SELECT gs.missing_sequence
FROM generate_series(
        1,
        (SELECT MAX(sequence_number) FROM weather_readings WHERE station_id = 1)
     ) AS gs(missing_sequence)
LEFT JOIN weather_readings w
       ON w.station_id = 1 AND w.sequence_number = gs.missing_sequence
WHERE w.id IS NULL
ORDER BY 1
LIMIT 50;

-- ---------------------------------------------------------------------
-- 3) LATEST WEATHER STATUS PER STATION (served straight from the DB)
-- ---------------------------------------------------------------------
SELECT * FROM latest_station_status ORDER BY station_id;

-- ---------------------------------------------------------------------
-- 4) RAINING VALIDATION - do the Kafka processor alerts match the raw data?
-- ---------------------------------------------------------------------
SELECT r.station_id,
       COUNT(*) FILTER (WHERE r.humidity > 70) AS readings_above_70,
       (SELECT COUNT(*) FROM rain_alerts a WHERE a.station_id = r.station_id) AS alerts_stored
FROM weather_readings r
GROUP BY r.station_id
ORDER BY r.station_id;

-- ---------------------------------------------------------------------
-- 5) Extra historical analytics used in the report
-- ---------------------------------------------------------------------
-- average / min / max weather per station
SELECT station_id,
       ROUND(AVG(temperature), 1) AS avg_temp_f,
       MIN(temperature)           AS min_temp_f,
       MAX(temperature)           AS max_temp_f,
       ROUND(AVG(humidity), 1)    AS avg_humidity,
       ROUND(AVG(wind_speed), 1)  AS avg_wind_kmh,
       COUNT(*)                   AS samples
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;

-- ingestion throughput per minute (useful to prove batching works)
SELECT date_trunc('minute', to_timestamp(timestamp)) AS minute,
       COUNT(*) AS readings
FROM weather_readings
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;
