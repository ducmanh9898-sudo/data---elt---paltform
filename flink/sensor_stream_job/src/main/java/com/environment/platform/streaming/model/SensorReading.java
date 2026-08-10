package com.environment.platform.streaming.model;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SensorReading {

    private String eventId;
    private String eventType;
    private String schemaVersion;

    private Integer cityId;
    private String cityName;
    private String countryCode;

    private String deviceId;
    @JsonProperty("temperature_2m")
    private Double temperature2m;


    @JsonProperty("relative_humidity_2m")
    private Double relativeHumidity2m;
    @JsonProperty("pm2_5")
    private Double pm25;
    private Double pm10;
    private String eventTimeUtc;
    public SensorReading() {
        // Required by Jackson
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(
        final String eventId
    ) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(
        final String eventType
    ) {
        this.eventType = eventType;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(
        final String schemaVersion
    ) {
        this.schemaVersion = schemaVersion;
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(
        final Integer cityId
    ) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(
        final String cityName
    ) {
        this.cityName = cityName;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(
        final String countryCode
    ) {
        this.countryCode = countryCode;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(
        final String deviceId
    ) {
        this.deviceId = deviceId;
    }

    public Double getTemperature2m() {
        return temperature2m;
    }

    public void setTemperature2m(
        final Double temperature2m
    ) {
        this.temperature2m = temperature2m;
    }

    public Double getRelativeHumidity2m() {
        return relativeHumidity2m;
    }

    public void setRelativeHumidity2m(
        final Double relativeHumidity2m
    ) {
        this.relativeHumidity2m = relativeHumidity2m;
    }

    public Double getPm25() {
        return pm25;
    }

    public void setPm25(
        final Double pm25
    ) {
        this.pm25 = pm25;
    }

    public Double getPm10() {
        return pm10;
    }

    public void setPm10(
        final Double pm10
    ) {
        this.pm10 = pm10;
    }
    public String getEventTimeUtc() {
    return eventTimeUtc;
}
public void setEventTimeUtc(
    final String eventTimeUtc
) {
    this.eventTimeUtc = eventTimeUtc;
}
}
