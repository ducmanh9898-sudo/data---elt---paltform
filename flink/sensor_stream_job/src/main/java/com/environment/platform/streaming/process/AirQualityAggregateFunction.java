package com.environment.platform.streaming.process;

import com.environment.platform.streaming.model.SensorReading;

import org.apache.flink.api.common.functions.AggregateFunction;

public final class AirQualityAggregateFunction
    implements AggregateFunction<
        SensorReading,
        AirQualityWindowAccumulator,
        AirQualityWindowAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public AirQualityWindowAccumulator createAccumulator() {
        return new AirQualityWindowAccumulator();
    }

    @Override
    public AirQualityWindowAccumulator add(
        SensorReading reading,
        AirQualityWindowAccumulator accumulator
    ) {

        accumulator.readingCount++;

        if (reading.getCityName() != null) {
            accumulator.cityName =
                reading.getCityName();
        }

        if (reading.getCountryCode() != null) {
            accumulator.countryCode =
                reading.getCountryCode();
        }

        if (reading.getTemperature2m() != null) {
            accumulator.temperatureSum +=
                reading.getTemperature2m();

            accumulator.temperatureCount++;
        }

        if (reading.getRelativeHumidity2m() != null) {
            accumulator.humiditySum +=
                reading.getRelativeHumidity2m();

            accumulator.humidityCount++;
        }

        if (reading.getPm25() != null) {
            double value = reading.getPm25();

            accumulator.pm25Sum += value;
            accumulator.pm25Count++;

            accumulator.pm25Min =
                min(
                    accumulator.pm25Min,
                    value
                );

            accumulator.pm25Max =
                max(
                    accumulator.pm25Max,
                    value
                );
        }

        if (reading.getPm10() != null) {
            double value = reading.getPm10();

            accumulator.pm10Sum += value;
            accumulator.pm10Count++;

            accumulator.pm10Min =
                min(
                    accumulator.pm10Min,
                    value
                );

            accumulator.pm10Max =
                max(
                    accumulator.pm10Max,
                    value
                );
        }

        if (reading.getCarbonMonoxide() != null) {
            accumulator.carbonMonoxideSum +=
                reading.getCarbonMonoxide();

            accumulator.carbonMonoxideCount++;
        }

        if (reading.getNitrogenDioxide() != null) {
            accumulator.nitrogenDioxideSum +=
                reading.getNitrogenDioxide();

            accumulator.nitrogenDioxideCount++;
        }

        return accumulator;
    }

    @Override
    public AirQualityWindowAccumulator getResult(
        AirQualityWindowAccumulator accumulator
    ) {
        return accumulator;
    }

    @Override
    public AirQualityWindowAccumulator merge(
        AirQualityWindowAccumulator left,
        AirQualityWindowAccumulator right
    ) {

        left.readingCount +=
            right.readingCount;

        if (right.cityName != null) {
            left.cityName =
                right.cityName;
        }

        if (right.countryCode != null) {
            left.countryCode =
                right.countryCode;
        }

        left.temperatureSum +=
            right.temperatureSum;

        left.temperatureCount +=
            right.temperatureCount;

        left.humiditySum +=
            right.humiditySum;

        left.humidityCount +=
            right.humidityCount;

        left.pm25Sum +=
            right.pm25Sum;

        left.pm25Count +=
            right.pm25Count;

        left.pm25Min =
            min(
                left.pm25Min,
                right.pm25Min
            );

        left.pm25Max =
            max(
                left.pm25Max,
                right.pm25Max
            );

        left.pm10Sum +=
            right.pm10Sum;

        left.pm10Count +=
            right.pm10Count;

        left.pm10Min =
            min(
                left.pm10Min,
                right.pm10Min
            );

        left.pm10Max =
            max(
                left.pm10Max,
                right.pm10Max
            );

        left.carbonMonoxideSum +=
            right.carbonMonoxideSum;

        left.carbonMonoxideCount +=
            right.carbonMonoxideCount;

        left.nitrogenDioxideSum +=
            right.nitrogenDioxideSum;

        left.nitrogenDioxideCount +=
            right.nitrogenDioxideCount;

        return left;
    }

    private static Double min(
        Double left,
        Double right
    ) {

        if (left == null) {
            return right;
        }

        if (right == null) {
            return left;
        }

        return Math.min(
            left,
            right
        );
    }

    private static Double max(
        Double left,
        Double right
    ) {

        if (left == null) {
            return right;
        }

        if (right == null) {
            return left;
        }

        return Math.max(
            left,
            right
        );
    }

    private static Double min(
        Double current,
        double value
    ) {

        if (current == null) {
            return value;
        }

        return Math.min(
            current,
            value
        );
    }

    private static Double max(
        Double current,
        double value
    ) {

        if (current == null) {
            return value;
        }

        return Math.max(
            current,
            value
        );
    }
}