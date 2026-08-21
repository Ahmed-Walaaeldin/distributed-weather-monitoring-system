package com.netcentric.weather.central;

import com.netcentric.weather.common.RainAlert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Optional part of D: persist the raining alerts produced by the Kafka processor. */
public class RainAlertWriter implements AutoCloseable {

    private static final String SQL = """
            INSERT INTO rain_alerts (station_id, sequence_number, timestamp, humidity)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (station_id, sequence_number) DO NOTHING
            """;

    private final Connection conn;
    private final PreparedStatement stmt;
    private int pending = 0;

    public RainAlertWriter(Connection conn) throws SQLException {
        this.conn = conn;
        this.stmt = conn.prepareStatement(SQL);
    }

    public void add(RainAlert alert) throws SQLException {
        stmt.setLong(1, alert.getStationId());
        stmt.setLong(2, alert.getSNo());
        stmt.setLong(3, alert.getStatusTimestamp());
        stmt.setInt(4, alert.getHumidity());
        stmt.addBatch();
        pending++;
    }

    public int flush() throws SQLException {
        if (pending == 0) {
            return 0;
        }
        stmt.executeBatch();
        conn.commit();
        int flushed = pending;
        pending = 0;
        return flushed;
    }

    public int pending() {
        return pending;
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
