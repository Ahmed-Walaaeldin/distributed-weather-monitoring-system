package com.netcentric.weather.central;

import com.netcentric.weather.common.Config;
import com.netcentric.weather.common.Json;
import com.netcentric.weather.common.Topics;
import com.netcentric.weather.common.WeatherMessage;
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
 * Consumes "weather-readings" and persists them through the BatchWriter.
 *
 * Delivery semantics: at-least-once.
 *   1. buffer records
 *   2. when the buffer reaches BATCH_SIZE (5000) or FLUSH_INTERVAL_MS elapses:
 *        a. INSERT + COMMIT in Postgres
 *        b. only then commitSync() the Kafka offsets
 *   If the process dies between (a) and (b) the records are re-read and the
 *   UNIQUE(station_id, sequence_number) + ON CONFLICT DO NOTHING absorbs them.
 */
public class ReadingsConsumer implements Runnable {

    private final Connection connection;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public ReadingsConsumer(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        int batchSize = Config.getInt("BATCH_SIZE", 5000);
        long flushIntervalMs = Config.getLong("FLUSH_INTERVAL_MS", 10_000L);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Config.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                Config.get("CONSUMER_GROUP", "central-station"));
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // manual commit
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(Topics.WEATHER_READINGS));

        System.out.printf("[central] consuming %s (batchSize=%d, flushEvery=%dms)%n",
                Topics.WEATHER_READINGS, batchSize, flushIntervalMs);

        try (BatchWriter writer = new BatchWriter(connection, batchSize)) {
            long lastFlush = System.currentTimeMillis();

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        writer.add(Json.read(record.value(), WeatherMessage.class));
                    } catch (RuntimeException bad) {
                        System.err.println("[central] skipping malformed record at offset "
                                + record.offset() + ": " + bad.getMessage());
                    }
                }

                boolean timeToFlush =
                        System.currentTimeMillis() - lastFlush >= flushIntervalMs && writer.pending() > 0;

                if (writer.isFull() || timeToFlush) {
                    writer.flush();          // 1. persist
                    consumer.commitSync();   // 2. then acknowledge to Kafka
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (WakeupException shuttingDown) {
            System.out.println("[central] readings consumer wakeup -> draining");
        } catch (Exception e) {
            System.err.println("[central] readings consumer failed: " + e);
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {
                // best effort
            }
            consumer.close();
            System.out.println("[central] readings consumer closed");
        }
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
