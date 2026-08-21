package com.netcentric.weather.rain;

import com.netcentric.weather.common.Config;
import com.netcentric.weather.common.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application built with the low-level Processor API:
 *
 *   weather-readings  -->  [RainProcessor]  -->  rain-alerts
 *                                |
 *                          state store "rain-alert-counts"
 */
public class RainDetectorApp {

    public static void main(String[] args) {
        String bootstrap = Config.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.get("STREAMS_APP_ID", "rain-detector-app"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, Config.get("STREAMS_STATE_DIR", "/tmp/kafka-streams"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Config.getInt("STREAMS_THREADS", 1));

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(RainProcessor.STORE_NAME),
                        Serdes.String(),
                        Serdes.Long());

        Topology topology = new Topology();
        topology.addSource("readings-source",
                        new StringDeserializer(), new StringDeserializer(),
                        Topics.WEATHER_READINGS)
                .addProcessor("rain-detector", RainProcessor::new, "readings-source")
                .addStateStore(storeBuilder, "rain-detector")
                .addSink("rain-sink", Topics.RAIN_ALERTS,
                        new StringSerializer(), new StringSerializer(),
                        "rain-detector");

        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.setUncaughtExceptionHandler(ex -> {
            System.err.println("[rain-processor] fatal: " + ex.getMessage());
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        System.out.println("[rain-processor] started, threshold humidity > 70%");

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
