package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.SensorReading;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.flink.api.common.serialization.SerializationSchema;


public final class LateSensorReadingSerializer
    implements SerializationSchema<SensorReading> {

    private static final long serialVersionUID = 1L;


    @Override
    public byte[] serialize(
        SensorReading reading
    ) {

        String json =
            "{"
                + "\"late_reason\":"
                + jsonString(
                    "TOO_LATE_FOR_5MIN_WINDOW"
                )
                + ","

                + "\"routed_at_utc\":"
                + jsonString(
                    Instant.now().toString()
                )
                + ","

                + "\"sensor_reading\":{"

                + "\"schema_version\":"
                + jsonString(
                    reading.getSchemaVersion()
                )
                + ","

                + "\"event_type\":"
                + jsonString(
                    reading.getEventType()
                )
                + ","

                + "\"event_id\":"
                + jsonString(
                    reading.getEventId()
                )
                + ","

                + "\"source_system\":"
                + jsonString(
                    reading.getSourceSystem()
                )
                + ","

                + "\"dataset_name\":"
                + jsonString(
                    reading.getDatasetName()
                )
                + ","

                + "\"device_id\":"
                + jsonString(
                    reading.getDeviceId()
                )
                + ","

                + "\"sequence_number\":"
                + jsonNumber(
                    reading.getSequenceNumber()
                )
                + ","

                + "\"city_id\":"
                + jsonNumber(
                    reading.getCityId()
                )
                + ","

                + "\"city_name\":"
                + jsonString(
                    reading.getCityName()
                )
                + ","

                + "\"country_code\":"
                + jsonString(
                    reading.getCountryCode()
                )
                + ","

                + "\"event_time_utc\":"
                + jsonString(
                    reading.getEventTimeUtc()
                )
                + ","

                + "\"produced_at_utc\":"
                + jsonString(
                    reading.getProducedAtUtc()
                )
                + ","

                + "\"temperature_2m\":"
                + jsonNumber(
                    reading.getTemperature2m()
                )
                + ","

                + "\"relative_humidity_2m\":"
                + jsonNumber(
                    reading.getRelativeHumidity2m()
                )
                + ","

                + "\"pm2_5\":"
                + jsonNumber(
                    reading.getPm25()
                )
                + ","

                + "\"pm10\":"
                + jsonNumber(
                    reading.getPm10()
                )
                + ","

                + "\"carbon_monoxide\":"
                + jsonNumber(
                    reading.getCarbonMonoxide()
                )
                + ","

                + "\"nitrogen_dioxide\":"
                + jsonNumber(
                    reading.getNitrogenDioxide()
                )

                + "}"
                + "}";

        return json.getBytes(
            StandardCharsets.UTF_8
        );
    }


    private static String jsonString(
        String value
    ) {

        if (value == null) {
            return "null";
        }

        return "\""
            + escape(value)
            + "\"";
    }


    private static String jsonNumber(
        Number value
    ) {

        if (value == null) {
            return "null";
        }

        return value.toString();
    }


    private static String escape(
        String value
    ) {

        return value
            .replace(
                "\\",
                "\\\\"
            )
            .replace(
                "\"",
                "\\\""
            )
            .replace(
                "\n",
                "\\n"
            )
            .replace(
                "\r",
                "\\r"
            )
            .replace(
                "\t",
                "\\t"
            );
    }
}