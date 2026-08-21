package com.netcentric.weather.central;

import com.netcentric.weather.common.Config;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;

/**
 * PART D: the Central Base Station.
 *
 *  - connects to the SQL database and creates the schema
 *  - starts one thread consuming the weather readings  -> batch inserts
 *  - starts one thread consuming the raining alerts    -> rain_alerts table
 */
public class CentralStationApp {

    public static void main(String[] args) throws Exception {
        Connection readingsConn = Database.connect();
        Database.initSchema(readingsConn);

        boolean consumeAlerts = Boolean.parseBoolean(Config.get("CONSUME_RAIN_ALERTS", "true"));

        ReadingsConsumer readings = new ReadingsConsumer(readingsConn);
        Thread readingsThread = new Thread(readings, "readings-consumer");

        Connection alertsConn = consumeAlerts ? Database.connect() : null;
        RainAlertsConsumer alerts = consumeAlerts ? new RainAlertsConsumer(alertsConn) : null;
        Thread alertsThread = consumeAlerts ? new Thread(alerts, "rain-consumer") : null;

        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[central] shutdown requested");
            readings.shutdown();
            if (alerts != null) {
                alerts.shutdown();
            }
            try {
                readingsThread.join(15_000);
                if (alertsThread != null) {
                    alertsThread.join(15_000);
                }
                readingsConn.close();
                if (alertsConn != null) {
                    alertsConn.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
            stopped.countDown();
        }));

        readingsThread.start();
        if (alertsThread != null) {
            alertsThread.start();
        }

        readingsThread.join();
        if (alertsThread != null) {
            alertsThread.join();
        }
        stopped.await();
    }
}
