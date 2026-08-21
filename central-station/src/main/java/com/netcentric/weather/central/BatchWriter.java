package com.netcentric.weather.central;

import com.netcentric.weather.common.WeatherMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Buffers weather readings and writes them with JDBC batch inserts
 * (recommended batch size = 5000) so we do one round trip per 5000 rows
 * instead of one per row.
 *
 * ON CONFLICT DO NOTHING makes the write idempotent: if the consumer
 * re-processes records after a crash (at-least-once delivery), duplicates
 * are silently ignored instead of corrupting the analysis.
 */
public class BatchWriter implements AutoCloseable {

    private static final String INSERT_SQL = """
            INSERT INTO weather_readings
                (station_id, sequence_number, battery_status, timestamp,
                 humidity, temperature, wind_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, sequence_number) DO NOTHING
            """;

    private final Connection conn;
    private final PreparedStatement stmt;
    private final int batchSize;
    private final List<WeatherMessage> buffer = new ArrayList<>();
    private long totalWritten = 0;

    public BatchWriter(Connection conn, int batchSize) throws SQLException {
        this.conn = conn;
        this.batchSize = batchSize;
        this.stmt = conn.prepareStatement(INSERT_SQL);
    }

    public void add(WeatherMessage message) {
        buffer.add(message);
    }

    public int pending() {
        return buffer.size();
    }

    public boolean isFull() {
        return buffer.size() >= batchSize;
    }

    /** Flushes the buffer inside a single transaction. Returns the number of rows sent. */
    public int flush() throws SQLException {
        if (buffer.isEmpty()) {
            return 0;
        }
        for (WeatherMessage m : buffer) {
            stmt.setLong(1, m.getStationId());
            stmt.setLong(2, m.getSNo());
            stmt.setString(3, m.getBatteryStatus());
            stmt.setLong(4, m.getStatusTimestamp());
            if (m.getWeather() == null) {
                stmt.setNull(5, java.sql.Types.INTEGER);
                stmt.setNull(6, java.sql.Types.INTEGER);
                stmt.setNull(7, java.sql.Types.INTEGER);
            } else {
                stmt.setInt(5, m.getWeather().getHumidity());
                stmt.setInt(6, m.getWeather().getTemperature());
                stmt.setInt(7, m.getWeather().getWindSpeed());
            }
            stmt.addBatch();
        }

        int sent = buffer.size();
        stmt.executeBatch();
        conn.commit();          // DB commit BEFORE the Kafka offset commit
        stmt.clearBatch();
        buffer.clear();
        totalWritten += sent;
        System.out.printf("[central] batch flushed: %d rows (total %d)%n", sent, totalWritten);
        return sent;
    }

    public long totalWritten() {
        return totalWritten;
    }

    @Override
    public void close() throws SQLException {
        try {
            flush();
        } finally {
            stmt.close();
        }
    }
}
