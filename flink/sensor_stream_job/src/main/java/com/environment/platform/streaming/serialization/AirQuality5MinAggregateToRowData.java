package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.AirQuality5MinAggregate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public final class AirQuality5MinAggregateToRowData
    implements MapFunction<AirQuality5MinAggregate, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(
        AirQuality5MinAggregate aggregate
    ) {

        GenericRowData row =
            new GenericRowData(17);

        // 0 window_start
        row.setField(
            0,
            timestampData(
                aggregate.getWindowStartEpochMillis()
            )
        );

        // 1 window_end
        row.setField(
            1,
            timestampData(
                aggregate.getWindowEndEpochMillis()
            )
        );

        // 2 city_id
        row.setField(
            2,
            aggregate.getCityId()
        );

        // 3 city_name
        row.setField(
            3,
            stringData(
                aggregate.getCityName()
            )
        );

        // 4 country_code
        row.setField(
            4,
            stringData(
                aggregate.getCountryCode()
            )
        );

        // 5 reading_count
        row.setField(
            5,
            aggregate.getReadingCount()
        );

        // 6 avg_temperature_2m
        row.setField(
            6,
            aggregate.getAvgTemperature2m()
        );

        // 7 avg_relative_humidity_2m
        row.setField(
            7,
            aggregate.getAvgRelativeHumidity2m()
        );

        // 8 avg_pm2_5
        row.setField(
            8,
            aggregate.getAvgPm25()
        );

        // 9 min_pm2_5
        row.setField(
            9,
            aggregate.getMinPm25()
        );

        // 10 max_pm2_5
        row.setField(
            10,
            aggregate.getMaxPm25()
        );

        // 11 avg_pm10
        row.setField(
            11,
            aggregate.getAvgPm10()
        );

        // 12 min_pm10
        row.setField(
            12,
            aggregate.getMinPm10()
        );

        // 13 max_pm10
        row.setField(
            13,
            aggregate.getMaxPm10()
        );

        // 14 avg_carbon_monoxide
        row.setField(
            14,
            aggregate.getAvgCarbonMonoxide()
        );

        // 15 avg_nitrogen_dioxide
        row.setField(
            15,
            aggregate.getAvgNitrogenDioxide()
        );

        // 16 processed_at
        row.setField(
            16,
            timestampData(
                aggregate.getProcessedAtEpochMillis()
            )
        );

        return row;
    }

    private static StringData stringData(
        String value
    ) {

        if (value == null) {
            return null;
        }

        return StringData.fromString(
            value
        );
    }

    private static TimestampData timestampData(
        Long epochMillis
    ) {

        if (epochMillis == null) {
            return null;
        }

        return TimestampData.fromEpochMillis(
            epochMillis
        );
    }
}