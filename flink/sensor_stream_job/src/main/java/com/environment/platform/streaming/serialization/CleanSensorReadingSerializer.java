package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.CleanSensorReading;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;


public class CleanSensorReadingSerializer
    implements SerializationSchema<CleanSensorReading> {


    @Override
    public byte[] serialize(
        CleanSensorReading reading
    ) {

        String json =
            String.format(
                """
                {
                  "event_id":"%s",
                  "event_time_utc":"%s",
                  "device_id":"%s",
                  "city_id":%d,
                  "city_name":"%s",
                  "country_code":"%s",
                  "temperature_2m":%s,
                  "relative_humidity_2m":%s,
                  "pm25":%s,
                  "pm10":%s
                }
                """,
                reading.getEventId(),
                reading.getEventTimeUtc(),
                reading.getDeviceId(),
                reading.getCityId(),
                reading.getCityName(),
                reading.getCountryCode(),
                reading.getTemperature2m(),
                reading.getRelativeHumidity2m(),
                reading.getPm25(),
                reading.getPm10()
            );


        return json.getBytes(
            StandardCharsets.UTF_8
        );
    }
}