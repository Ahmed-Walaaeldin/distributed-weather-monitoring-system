package com.netcentric.weather.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Weather Status Message exchanged over Kafka.
 * Schema is exactly the one required by the lab:
 * {
 *   "station_id": 1, "s_no": 1, "battery_status": "low",
 *   "status_timestamp": 1681521224,
 *   "weather": { "humidity": 35, "temperature": 100, "wind_speed": 13 }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class WeatherMessage {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sNo;

    /** one of: low, medium, high */
    @JsonProperty("battery_status")
    private String batteryStatus;

    /** Unix timestamp in SECONDS */
    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    @JsonProperty("weather")
    private Weather weather;

    public WeatherMessage() {
    }

    public WeatherMessage(long stationId, long sNo, String batteryStatus,
                          long statusTimestamp, Weather weather) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public long getStationId() { return stationId; }
    public void setStationId(long stationId) { this.stationId = stationId; }

    public long getSNo() { return sNo; }
    public void setSNo(long sNo) { this.sNo = sNo; }

    public String getBatteryStatus() { return batteryStatus; }
    public void setBatteryStatus(String batteryStatus) { this.batteryStatus = batteryStatus; }

    public long getStatusTimestamp() { return statusTimestamp; }
    public void setStatusTimestamp(long statusTimestamp) { this.statusTimestamp = statusTimestamp; }

    public Weather getWeather() { return weather; }
    public void setWeather(Weather weather) { this.weather = weather; }

    @Override
    public String toString() {
        return "WeatherMessage{station_id=" + stationId + ", s_no=" + sNo
                + ", battery_status='" + batteryStatus + "', status_timestamp=" + statusTimestamp
                + ", weather=" + weather + '}';
    }
}
