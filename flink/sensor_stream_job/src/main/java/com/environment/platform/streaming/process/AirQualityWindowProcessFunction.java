package com.environment.platform.streaming.process;

import com.environment.platform.streaming.model.AirQuality5MinAggregate;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;

public final class AirQualityWindowProcessFunction
    extends ProcessWindowFunction<
        AirQualityWindowAccumulator,
        AirQuality5MinAggregate,
        Integer,
        TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
        Integer cityId,
        Context context,
        Iterable<AirQualityWindowAccumulator> elements,
        Collector<AirQuality5MinAggregate> out
    ) {

        AirQualityWindowAccumulator accumulator =
            elements
                .iterator()
                .next();

        AirQuality5MinAggregate result =
            new AirQuality5MinAggregate();

        result.setWindowStartEpochMillis(
            context
                .window()
                .getStart()
        );

        result.setWindowEndEpochMillis(
            context
                .window()
                .getEnd()
        );

        result.setCityId(
            cityId
        );

        result.setCityName(
            accumulator.cityName
        );

        result.setCountryCode(
            accumulator.countryCode
        );

        result.setReadingCount(
            accumulator.readingCount
        );

        result.setAvgTemperature2m(
            average(
                accumulator.temperatureSum,
                accumulator.temperatureCount
            )
        );

        result.setAvgRelativeHumidity2m(
            average(
                accumulator.humiditySum,
                accumulator.humidityCount
            )
        );

        result.setAvgPm25(
            average(
                accumulator.pm25Sum,
                accumulator.pm25Count
            )
        );

        result.setMinPm25(
            accumulator.pm25Min
        );

        result.setMaxPm25(
            accumulator.pm25Max
        );

        result.setAvgPm10(
            average(
                accumulator.pm10Sum,
                accumulator.pm10Count
            )
        );

        result.setMinPm10(
            accumulator.pm10Min
        );

        result.setMaxPm10(
            accumulator.pm10Max
        );

        result.setAvgCarbonMonoxide(
            average(
                accumulator.carbonMonoxideSum,
                accumulator.carbonMonoxideCount
            )
        );

        result.setAvgNitrogenDioxide(
            average(
                accumulator.nitrogenDioxideSum,
                accumulator.nitrogenDioxideCount
            )
        );

        result.setProcessedAtEpochMillis(
            System.currentTimeMillis()
        );

        out.collect(
            result
        );
    }

    private static Double average(
        double sum,
        long count
    ) {

        if (count == 0L) {
            return null;
        }

        return sum / count;
    }
}