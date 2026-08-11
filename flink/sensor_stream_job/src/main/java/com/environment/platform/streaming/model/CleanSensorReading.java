package com.environment.platform.streaming.model;


public class CleanSensorReading {

    private String eventId;

    private String eventTimeUtc;

    private String deviceId;

    private Integer cityId;

    private String cityName;

    private String countryCode;


    private Double temperature2m;

    private Double relativeHumidity2m;

    private Double pm25;

    private Double pm10;


    public CleanSensorReading() {
    }


    public CleanSensorReading(
        final String eventId,
        final String eventTimeUtc,
        final String deviceId,
        final Integer cityId,
        final String cityName,
        final String countryCode,
        final Double temperature2m,
        final Double relativeHumidity2m,
        final Double pm25,
        final Double pm10
    ) {

        this.eventId = eventId;
        this.eventTimeUtc = eventTimeUtc;
        this.deviceId = deviceId;
        this.cityId = cityId;
        this.cityName = cityName;
        this.countryCode = countryCode;
        this.temperature2m = temperature2m;
        this.relativeHumidity2m = relativeHumidity2m;
        this.pm25 = pm25;
        this.pm10 = pm10;
    }


    public String getEventId() {
        return eventId;
    }


    public String getEventTimeUtc() {
        return eventTimeUtc;
    }


    public String getDeviceId() {
        return deviceId;
    }


    public Integer getCityId() {
        return cityId;
    }


    public String getCityName() {
        return cityName;
    }


    public String getCountryCode() {
        return countryCode;
    }


    public Double getTemperature2m() {
        return temperature2m;
    }


    public Double getRelativeHumidity2m() {
        return relativeHumidity2m;
    }


    public Double getPm25() {
        return pm25;
    }


    public Double getPm10() {
        return pm10;
    }
}