package com.netcentric.weather.rain;

import com.netcentric.weather.common.Config;
import com.netcentric.weather.common.Json;
import com.netcentric.weather.common.RainAlert;
import com.netcentric.weather.common.Topics;
import com.netcentric.weather.common.WeatherMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Alternative implementation of PART C using the high-level Kafka Streams DSL.
 * Functionally equivalent to RainDetectorApp; kept for the report comparison.
 * Run it with:  java -cp rain-processor.jar com.netcentric.weather.rain.RainDetectorDslApp
 */
public class RainDetectorDslApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-dsl-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Config.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> readings =
                builder.stream(Topics.WEATHER_READINGS, Consumed.with(Serdes.String(), Serdes.String()));

        readings.mapValues(v -> Json.read(v, WeatherMessage.class))
                .filter((k, m) -> m.getWeather() != null && m.getWeather().getHumidity() > 70)
                .mapValues(m -> Json.write(new RainAlert(m.getStationId(), m.getSNo(),
                        m.getStatusTimestamp(), m.getWeather().getHumidity(), 0L)))
                .to(Topics.RAIN_ALERTS, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(); latch.countDown(); }));
        streams.start();
        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
