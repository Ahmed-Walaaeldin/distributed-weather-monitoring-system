package com.netcentric.weather.station;

import com.netcentric.weather.common.Config;
import com.netcentric.weather.common.Json;
import com.netcentric.weather.common.Topics;
import com.netcentric.weather.common.WeatherMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * PART A + PART B of the lab.
 *
 * Emits one weather status message every second to the Kafka topic "weather-readings",
 * dropping ~10% of them on purpose.
 *
 * The Kafka message key is the station id: this guarantees that all readings of one
 * station land in the same partition and therefore keep their relative order.
 */
public class WeatherStationApp {

    public static void main(String[] args) {
        final long stationId = StationIdentity.resolve();
        final String bootstrap = Config.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        final long intervalMs = Config.getLong("EMIT_INTERVAL_MS", 1000L);   // every 1 second
        final double dropRate = Config.getDouble("DROP_RATE", 0.10);          // 10% dropped

        System.out.printf("[station-%d] starting: bootstrap=%s topic=%s interval=%dms dropRate=%.2f%n",
                stationId, bootstrap, Topics.WEATHER_READINGS, intervalMs, dropRate);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "weather-station-" + stationId);
        // durability + no duplicates on retry
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // small batching window: cheap throughput win, still well under the 1s cadence
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        ReadingGenerator generator = new ReadingGenerator(stationId);
        CountDownLatch shutdown = new CountDownLatch(1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.printf("[station-%d] shutting down after %d generated messages%n",
                        stationId, generator.currentSequence());
                shutdown.countDown();
            }));

            long sent = 0, dropped = 0;

            while (shutdown.getCount() > 0) {
                long tick = System.currentTimeMillis();

                WeatherMessage message = generator.next();   // s_no consumed even if dropped

                if (ThreadLocalRandom.current().nextDouble() < dropRate) {
                    dropped++;
                    if (dropped % 10 == 0) {
                        System.out.printf("[station-%d] dropped s_no=%d (total dropped=%d)%n",
                                stationId, message.getSNo(), dropped);
                    }
                } else {
                    String payload = Json.write(message);
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(Topics.WEATHER_READINGS,
                                    String.valueOf(stationId), payload);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("[station-%d] send failed: %s%n",
                                    stationId, exception.getMessage());
                        }
                    });
                    sent++;
                    if (sent % 30 == 0) {
                        System.out.printf("[station-%d] sent=%d dropped=%d last=%s%n",
                                stationId, sent, dropped, payload);
                    }
                }

                // keep an exact 1 message / second cadence
                long elapsed = System.currentTimeMillis() - tick;
                long sleep = Math.max(0, intervalMs - elapsed);
                try {
                    if (shutdown.await(sleep, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            producer.flush();
        }
        System.out.printf("[station-%d] stopped.%n", stationId);
    }
}
