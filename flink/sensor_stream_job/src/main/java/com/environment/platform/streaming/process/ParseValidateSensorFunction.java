package com.environment.platform.streaming.process;

import com.environment.platform.streaming.model.DlqMessage;
import com.environment.platform.streaming.model.RawKafkaEvent;
import com.environment.platform.streaming.model.SensorReading;
import com.environment.platform.streaming.validation.SensorReadingValidator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.time.Instant;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ParseValidateSensorFunction
    extends ProcessFunction<RawKafkaEvent, SensorReading> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<DlqMessage> DLQ_TAG =
        new OutputTag<DlqMessage>(
            "sensor-readings-dlq"
        ) {
            private static final long serialVersionUID = 1L;
        };

    private transient ObjectMapper objectMapper;

    private ObjectMapper mapper() {

        if (objectMapper == null) {

            objectMapper =
                new ObjectMapper()
                    .setPropertyNamingStrategy(
                        PropertyNamingStrategies.SNAKE_CASE
                    )
                    .configure(
                        DeserializationFeature
                            .FAIL_ON_UNKNOWN_PROPERTIES,
                        false
                    );
        }

        return objectMapper;
    }

    @Override
    public void processElement(
        RawKafkaEvent rawEvent,
        Context context,
        Collector<SensorReading> out
    ) {

        try {

            SensorReading reading =
                mapper().readValue(
                    rawEvent.getRawPayload(),
                    SensorReading.class
                );

            var validation =
                SensorReadingValidator.validate(
                    reading
                );

            if (validation.valid()) {

                out.collect(reading);
                return;
            }

            context.output(
                DLQ_TAG,
                new DlqMessage(
                    Instant.now().toString(),
                    "VALIDATION_ERROR: "
                        + validation.reason(),
                    rawEvent.getRawPayload(),
                    rawEvent.getTopic()
                )
            );

        } catch (Exception exception) {

            context.output(
                DLQ_TAG,
                new DlqMessage(
                    Instant.now().toString(),
                    "INVALID_JSON: "
                        + safeMessage(exception),
                    rawEvent.getRawPayload(),
                    rawEvent.getTopic()
                )
            );
        }
    }

    private static String safeMessage(
        Exception exception
    ) {

        String message =
            exception.getMessage();

        if (
            message == null ||
            message.isBlank()
        ) {
            return exception
                .getClass()
                .getSimpleName();
        }

        return message;
    }
}
