package com.netcentric.weather.station;

import com.netcentric.weather.common.Weather;
import com.netcentric.weather.common.WeatherMessage;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock sensor. Produces one WeatherMessage per call.
 *
 * Lab requirements implemented here:
 *  - s_no is auto-incremental per service (never reused, and it keeps incrementing
 *    even for dropped messages, which is what makes drop detection possible in SQL).
 *  - battery_status distribution: low 30% / medium 40% / high 30%.
 */
public class ReadingGenerator {

    private static final String[] BATTERY = {"low", "medium", "high"};

    private final long stationId;
    private final AtomicLong sequence = new AtomicLong(0);

    public ReadingGenerator(long stationId) {
        this.stationId = stationId;
    }

    public WeatherMessage next() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // ---- battery status: 30% low, 40% medium, 30% high -------------------
        double p = rnd.nextDouble();          // uniform in [0,1)
        String battery = p < 0.30 ? BATTERY[0]
                       : p < 0.70 ? BATTERY[1]
                                  : BATTERY[2];

        // ---- sampled weather values -----------------------------------------
        // humidity is uniform 0..100 so that roughly 30% of readings are > 70%
        // and therefore trigger the "raining" detection in the Kafka processor.
        int humidity = rnd.nextInt(0, 101);              // %
        int temperature = rnd.nextInt(20, 121);          // Fahrenheit
        int windSpeed = rnd.nextInt(0, 51);              // km/h

        return new WeatherMessage(
                stationId,
                sequence.incrementAndGet(),
                battery,
                Instant.now().getEpochSecond(),          // Long Unix timestamp (seconds)
                new Weather(humidity, temperature, windSpeed));
    }

    public long currentSequence() {
        return sequence.get();
    }
}
