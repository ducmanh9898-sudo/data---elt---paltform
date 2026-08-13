package com.environment.platform.streaming.model;

import java.io.Serializable;

public class AirQuality5MinAggregate
    implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long windowStartEpochMillis;
    private Long windowEndEpochMillis;

    private Integer cityId;
    private String cityName;
    private String countryCode;

    private Long readingCount;

    private Double avgTemperature2m;
    private Double avgRelativeHumidity2m;

    private Double avgPm25;
    private Double minPm25;
    private Double maxPm25;

    private Double avgPm10;
    private Double minPm10;
    private Double maxPm10;

    private Double avgCarbonMonoxide;
    private Double avgNitrogenDioxide;

    private Long processedAtEpochMillis;

    public AirQuality5MinAggregate() {
    }

    public Long getWindowStartEpochMillis() {
        return windowStartEpochMillis;
    }

    public void setWindowStartEpochMillis(
        Long windowStartEpochMillis
    ) {
        this.windowStartEpochMillis =
            windowStartEpochMillis;
    }

    public Long getWindowEndEpochMillis() {
        return windowEndEpochMillis;
    }

    public void setWindowEndEpochMillis(
        Long windowEndEpochMillis
    ) {
        this.windowEndEpochMillis =
            windowEndEpochMillis;
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(
        Integer cityId
    ) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(
        String cityName
    ) {
        this.cityName = cityName;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(
        String countryCode
    ) {
        this.countryCode =
            countryCode;
    }

    public Long getReadingCount() {
        return readingCount;
    }

    public void setReadingCount(
        Long readingCount
    ) {
        this.readingCount =
            readingCount;
    }

    public Double getAvgTemperature2m() {
        return avgTemperature2m;
    }

    public void setAvgTemperature2m(
        Double value
    ) {
        this.avgTemperature2m =
            value;
    }

    public Double getAvgRelativeHumidity2m() {
        return avgRelativeHumidity2m;
    }

    public void setAvgRelativeHumidity2m(
        Double value
    ) {
        this.avgRelativeHumidity2m =
            value;
    }

    public Double getAvgPm25() {
        return avgPm25;
    }

    public void setAvgPm25(
        Double value
    ) {
        this.avgPm25 = value;
    }

    public Double getMinPm25() {
        return minPm25;
    }

    public void setMinPm25(
        Double value
    ) {
        this.minPm25 = value;
    }

    public Double getMaxPm25() {
        return maxPm25;
    }

    public void setMaxPm25(
        Double value
    ) {
        this.maxPm25 = value;
    }

    public Double getAvgPm10() {
        return avgPm10;
    }

    public void setAvgPm10(
        Double value
    ) {
        this.avgPm10 = value;
    }

    public Double getMinPm10() {
        return minPm10;
    }

    public void setMinPm10(
        Double value
    ) {
        this.minPm10 = value;
    }

    public Double getMaxPm10() {
        return maxPm10;
    }

    public void setMaxPm10(
        Double value
    ) {
        this.maxPm10 = value;
    }

    public Double getAvgCarbonMonoxide() {
        return avgCarbonMonoxide;
    }

    public void setAvgCarbonMonoxide(
        Double value
    ) {
        this.avgCarbonMonoxide =
            value;
    }

    public Double getAvgNitrogenDioxide() {
        return avgNitrogenDioxide;
    }

    public void setAvgNitrogenDioxide(
        Double value
    ) {
        this.avgNitrogenDioxide =
            value;
    }

    public Long getProcessedAtEpochMillis() {
        return processedAtEpochMillis;
    }

    public void setProcessedAtEpochMillis(
        Long processedAtEpochMillis
    ) {
        this.processedAtEpochMillis =
            processedAtEpochMillis;
    }
}