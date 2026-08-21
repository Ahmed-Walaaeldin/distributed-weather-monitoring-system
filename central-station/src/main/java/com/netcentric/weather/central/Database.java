package com.netcentric.weather.central;

import com.netcentric.weather.common.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * JDBC connectivity. All credentials come from environment variables
 * (Kubernetes Secret / cloud env file) - nothing is hardcoded in source.
 *
 *   DB_URL       e.g. jdbc:postgresql://postgres:5432/weather
 *   DB_USER      e.g. weather
 *   DB_PASSWORD  e.g. ******
 *   DB_SSLMODE   e.g. require   (needed by Aiven / managed providers)
 */
public final class Database {

    private Database() {
    }

    public static Connection connect() throws SQLException {
        String url = Config.get("DB_URL", "jdbc:postgresql://localhost:5432/weather");
        Properties props = new Properties();
        props.setProperty("user", Config.get("DB_USER", "weather"));
        props.setProperty("password", Config.require("DB_PASSWORD"));
        String sslMode = Config.get("DB_SSLMODE", null);
        if (sslMode != null) {
            props.setProperty("sslmode", sslMode);   // "require" for Aiven
        }

        Connection conn = null;
        SQLException last = null;
        // the DB pod may still be starting up -> retry for ~1 minute
        for (int attempt = 1; attempt <= 30 && conn == null; attempt++) {
            try {
                conn = DriverManager.getConnection(url, props);
            } catch (SQLException e) {
                last = e;
                System.out.printf("[central] DB not ready (attempt %d): %s%n", attempt, e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (conn == null) {
            throw last == null ? new SQLException("Cannot connect to " + url) : last;
        }

        conn.setAutoCommit(false);   // we control transactions around each batch
        System.out.println("[central] connected to " + url);
        return conn;
    }

    /** Creates the schema on first start (idempotent). */
    public static void initSchema(Connection conn) throws SQLException {
        String ddl = """
                CREATE TABLE IF NOT EXISTS weather_readings (
                    id              BIGSERIAL PRIMARY KEY,
                    station_id      BIGINT      NOT NULL,
                    sequence_number BIGINT      NOT NULL,
                    battery_status  VARCHAR(10) NOT NULL,
                    timestamp       BIGINT      NOT NULL,
                    humidity        INT,
                    temperature     INT,
                    wind_speed      INT,
                    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT uq_station_sequence UNIQUE (station_id, sequence_number)
                );
                CREATE INDEX IF NOT EXISTS idx_readings_station_seq
                    ON weather_readings (station_id, sequence_number DESC);
                CREATE INDEX IF NOT EXISTS idx_readings_battery
                    ON weather_readings (station_id, battery_status);

                CREATE TABLE IF NOT EXISTS rain_alerts (
                    id              BIGSERIAL PRIMARY KEY,
                    station_id      BIGINT NOT NULL,
                    sequence_number BIGINT NOT NULL,
                    timestamp       BIGINT NOT NULL,
                    humidity        INT    NOT NULL,
                    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT uq_alert_station_sequence UNIQUE (station_id, sequence_number)
                );

                CREATE OR REPLACE VIEW latest_station_status AS
                SELECT DISTINCT ON (station_id)
                       station_id, sequence_number, battery_status, timestamp,
                       humidity, temperature, wind_speed, ingested_at
                FROM weather_readings
                ORDER BY station_id, sequence_number DESC;
                """;
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
            conn.commit();
        }
        System.out.println("[central] schema ready");
    }
}
