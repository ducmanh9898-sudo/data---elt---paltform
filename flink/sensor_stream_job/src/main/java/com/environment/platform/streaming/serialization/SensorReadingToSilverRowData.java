package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.SensorReading;

import java.time.Instant;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public final class SensorReadingToSilverRowData
    implements MapFunction<SensorReading, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(
        SensorReading reading
    ) {

        GenericRowData row =
            new GenericRowData(21);

        // 0 schema_version
        row.setField(
            0,
            stringData(
                reading.getSchemaVersion()
            )
        );

        // 1 event_type
        row.setField(
            1,
            stringData(
                reading.getEventType()
            )
        );

        // 2 event_id
        row.setField(
            2,
            stringData(
                reading.getEventId()
            )
        );

        // 3 source_system
        row.setField(
            3,
            stringData(
                reading.getSourceSystem()
            )
        );

        // 4 dataset_name
        row.setField(
            4,
            stringData(
                reading.getDatasetName()
            )
        );

        // 5 device_id
        row.setField(
            5,
            stringData(
                reading.getDeviceId()
            )
        );

        // 6 sequence_number
        row.setField(
            6,
            reading.getSequenceNumber()
        );

        // 7 city_id
        row.setField(
            7,
            reading.getCityId()
        );

        // 8 city_name
        row.setField(
            8,
            stringData(
                reading.getCityName()
            )
        );

        // 9 country_code
        row.setField(
            9,
            stringData(
                reading.getCountryCode()
            )
        );

        // 10 event_time_utc
        row.setField(
            10,
            timestampData(
                reading.getEventTimeUtc()
            )
        );

        // 11 produced_at_utc
        row.setField(
            11,
            timestampData(
                reading.getProducedAtUtc()
            )
        );

        // 12 temperature_2m
        row.setField(
            12,
            reading.getTemperature2m()
        );

        // 13 relative_humidity_2m
        row.setField(
            13,
            reading.getRelativeHumidity2m()
        );

        // 14 pm2_5
        row.setField(
            14,
            reading.getPm25()
        );

        // 15 pm10
        row.setField(
            15,
            reading.getPm10()
        );

        // 16 carbon_monoxide
        row.setField(
            16,
            reading.getCarbonMonoxide()
        );

        // 17 nitrogen_dioxide
        row.setField(
            17,
            reading.getNitrogenDioxide()
        );

        // 18 quality_status
        row.setField(
            18,
            StringData.fromString(
                "VALID"
            )
        );

        // 19 quality_error
        row.setField(
            19,
            null
        );

        // 20 processed_at
        row.setField(
            20,
            TimestampData.fromEpochMillis(
                Instant
                    .now()
                    .toEpochMilli()
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
        String value
    ) {

        if (
            value == null ||
            value.isBlank()
        ) {
            return null;
        }

        return TimestampData.fromEpochMillis(
            Instant
                .parse(value)
                .toEpochMilli()
        );
    }
}