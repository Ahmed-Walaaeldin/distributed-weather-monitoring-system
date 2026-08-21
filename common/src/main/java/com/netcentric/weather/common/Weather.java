package com.netcentric.weather.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Nested "weather" object of the status message. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Weather {

    /** percentage 0..100 */
    @JsonProperty("humidity")
    private int humidity;

    /** degrees Fahrenheit */
    @JsonProperty("temperature")
    private int temperature;

    /** km/h */
    @JsonProperty("wind_speed")
    private int windSpeed;

    public Weather() {
    }

    public Weather(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public int getHumidity() { return humidity; }
    public void setHumidity(int humidity) { this.humidity = humidity; }

    public int getTemperature() { return temperature; }
    public void setTemperature(int temperature) { this.temperature = temperature; }

    public int getWindSpeed() { return windSpeed; }
    public void setWindSpeed(int windSpeed) { this.windSpeed = windSpeed; }

    @Override
    public String toString() {
        return "{humidity=" + humidity + ", temperature=" + temperature
                + ", wind_speed=" + windSpeed + '}';
    }
}
