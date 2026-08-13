package com.environment.platform.streaming.process;

import java.io.Serializable;

public class AirQualityWindowAccumulator
    implements Serializable {

    private static final long serialVersionUID = 1L;

    public long readingCount;

    public String cityName;
    public String countryCode;

    public double temperatureSum;
    public long temperatureCount;

    public double humiditySum;
    public long humidityCount;

    public double pm25Sum;
    public long pm25Count;
    public Double pm25Min;
    public Double pm25Max;

    public double pm10Sum;
    public long pm10Count;
    public Double pm10Min;
    public Double pm10Max;

    public double carbonMonoxideSum;
    public long carbonMonoxideCount;

    public double nitrogenDioxideSum;
    public long nitrogenDioxideCount;

    public AirQualityWindowAccumulator() {
    }
}