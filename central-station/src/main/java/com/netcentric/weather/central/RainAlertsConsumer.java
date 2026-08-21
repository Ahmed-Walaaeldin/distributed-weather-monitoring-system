package com.netcentric.weather.central;

import com.netcentric.weather.common.Config;
import com.netcentric.weather.common.Json;
import com.netcentric.weather.common.RainAlert;
import com.netcentric.weather.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Optional: consumes the "rain-alerts" topic produced by the Kafka Processor
 * and archives the alerts in the rain_alerts table.
 * Uses its OWN JDBC connection because JDBC connections are not thread-safe.
 */
public class RainAlertsConsumer implements Runnable {

    private final Connection connection;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public RainAlertsConsumer(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Config.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                Config.get("CONSUMER_GROUP", "central-station") + "-rain");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(Topics.RAIN_ALERTS));
        System.out.println("[central] consuming " + Topics.RAIN_ALERTS);

        try (RainAlertWriter writer = new RainAlertWriter(connection)) {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        writer.add(Json.read(record.value(), RainAlert.class));
                    } catch (RuntimeException bad) {
                        System.err.println("[central] bad rain alert: " + bad.getMessage());
                    }
                }
                if (writer.pending() >= 500 || (!records.isEmpty() && writer.pending() > 0)) {
                    int n = writer.flush();
                    consumer.commitSync();
                    if (n > 0) {
                        System.out.printf("[central] persisted %d rain alerts%n", n);
                    }
                }
            }
        } catch (WakeupException shuttingDown) {
            System.out.println("[central] rain consumer wakeup -> draining");
        } catch (Exception e) {
            System.err.println("[central] rain consumer failed: " + e);
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
