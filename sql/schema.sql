-- =====================================================================
-- Lab 4 - Weather Stations Monitoring : PostgreSQL schema
-- The central station also creates this automatically at startup
-- (Database.initSchema); this file is the standalone/reference version.
-- =====================================================================

CREATE TABLE IF NOT EXISTS weather_readings (
    id              BIGSERIAL PRIMARY KEY,
    station_id      BIGINT      NOT NULL,
    sequence_number BIGINT      NOT NULL,     -- s_no coming from the station
    battery_status  VARCHAR(10) NOT NULL,     -- low | medium | high
    timestamp       BIGINT      NOT NULL,     -- unix epoch seconds
    humidity        INT,                      -- %
    temperature     INT,                      -- Fahrenheit
    wind_speed      INT,                      -- km/h
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- makes re-processing after a crash harmless (at-least-once -> effectively once)
    CONSTRAINT uq_station_sequence UNIQUE (station_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_readings_station_seq
    ON weather_readings (station_id, sequence_number DESC);

CREATE INDEX IF NOT EXISTS idx_readings_battery
    ON weather_readings (station_id, battery_status);

-- archive of the special messages emitted by the Kafka rain processor
CREATE TABLE IF NOT EXISTS rain_alerts (
    id              BIGSERIAL PRIMARY KEY,
    station_id      BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    timestamp       BIGINT NOT NULL,
    humidity        INT    NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_alert_station_sequence UNIQUE (station_id, sequence_number)
);

-- "Latest weather status per station is queried directly from the database"
CREATE OR REPLACE VIEW latest_station_status AS
SELECT DISTINCT ON (station_id)
       station_id, sequence_number, battery_status, timestamp,
       humidity, temperature, wind_speed, ingested_at
FROM weather_readings
ORDER BY station_id, sequence_number DESC;
