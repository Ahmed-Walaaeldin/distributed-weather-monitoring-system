package com.netcentric.weather.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Special message emitted by the Kafka Processor on the "rain-alerts" topic. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RainAlert {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sNo;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    @JsonProperty("humidity")
    private int humidity;

    /** how many rain alerts this station produced so far (state store counter) */
    @JsonProperty("alert_count")
    private long alertCount;

    @JsonProperty("message")
    private String message = "It is raining";

    public RainAlert() {
    }

    public RainAlert(long stationId, long sNo, long statusTimestamp, int humidity, long alertCount) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.statusTimestamp = statusTimestamp;
        this.humidity = humidity;
        this.alertCount = alertCount;
    }

    public long getStationId() { return stationId; }
    public void setStationId(long stationId) { this.stationId = stationId; }

    public long getSNo() { return sNo; }
    public void setSNo(long sNo) { this.sNo = sNo; }

    public long getStatusTimestamp() { return statusTimestamp; }
    public void setStatusTimestamp(long statusTimestamp) { this.statusTimestamp = statusTimestamp; }

    public int getHumidity() { return humidity; }
    public void setHumidity(int humidity) { this.humidity = humidity; }

    public long getAlertCount() { return alertCount; }
    public void setAlertCount(long alertCount) { this.alertCount = alertCount; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
