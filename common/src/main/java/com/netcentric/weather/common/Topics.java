package com.netcentric.weather.common;

/** Central place for Kafka topic names (overridable via env). */
public final class Topics {

    public static final String WEATHER_READINGS = Config.get("TOPIC_WEATHER", "weather-readings");
    public static final String RAIN_ALERTS = Config.get("TOPIC_RAIN", "rain-alerts");

    private Topics() {
    }
}
