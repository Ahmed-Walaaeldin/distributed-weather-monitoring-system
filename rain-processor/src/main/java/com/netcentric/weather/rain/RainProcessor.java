package com.netcentric.weather.rain;

import com.netcentric.weather.common.Json;
import com.netcentric.weather.common.RainAlert;
import com.netcentric.weather.common.WeatherMessage;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * PART C: low-level Kafka Processor API node.
 *
 * For every incoming weather reading:
 *   - parse the JSON
 *   - if weather.humidity > 70  ->  it is raining
 *   - keep a per-station counter in a persistent state store (stateful processing)
 *   - forward a special RainAlert message downstream (to the "rain-alerts" sink)
 * Non-raining readings are simply not forwarded (filtering).
 */
public class RainProcessor implements Processor<String, String, String, String> {

    public static final String STORE_NAME = "rain-alert-counts";
    private static final int RAIN_HUMIDITY_THRESHOLD = 70;   // "higher than 70%"

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, Long> counts;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.counts = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.value() == null) {
            return;
        }

        final WeatherMessage reading;
        try {
            reading = Json.read(record.value(), WeatherMessage.class);
        } catch (RuntimeException malformed) {
            System.err.println("[rain-processor] skipping malformed record: " + malformed.getMessage());
            return;   // poison-pill safety: never kill the topology on one bad record
        }

        if (reading.getWeather() == null
                || reading.getWeather().getHumidity() <= RAIN_HUMIDITY_THRESHOLD) {
            return;   // not raining -> filtered out
        }

        String key = String.valueOf(reading.getStationId());
        long current = counts.get(key) == null ? 0L : counts.get(key);
        long updated = current + 1;
        counts.put(key, updated);

        RainAlert alert = new RainAlert(
                reading.getStationId(),
                reading.getSNo(),
                reading.getStatusTimestamp(),
                reading.getWeather().getHumidity(),
                updated);

        System.out.printf("[rain-processor] RAIN station=%d s_no=%d humidity=%d%% (alert #%d)%n",
                alert.getStationId(), alert.getSNo(), alert.getHumidity(), updated);

        // forward downstream, preserving the station id as key
        context.forward(record.withKey(key).withValue(Json.write(alert)));
    }

    @Override
    public void close() {
        // state store is closed by Kafka Streams
    }
}
