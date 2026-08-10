package com.environment.platform.streaming;
import com.environment.platform.streaming.serialization.CleanSensorReadingSerializer;


import com.environment.platform.streaming.model.SensorReading;
import com.environment.platform.streaming.validation.SensorReadingValidator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.environment.platform.streaming.model.ValidationResult;
import com.environment.platform.streaming.model.DlqMessage;
import com.environment.platform.streaming.serialization.DlqMessageSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.environment.platform.streaming.model.CleanSensorReading;
import java.time.Duration;
import java.time.Instant;

public final class KafkaValidationJob {

    private KafkaValidationJob() {
    }

    public static void main(
        final String[] args
    ) throws Exception {

        final StreamExecutionEnvironment env =
            StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setParallelism(2);

        final KafkaSource<String> source =
            KafkaSource
                .<String>builder()
                .setBootstrapServers(
                    "kafka:9092"
                )
                .setTopics(
                    "environment.sensor-readings.raw"
                )
                .setGroupId(
                    "environment-validation-v1"
                )
                .setStartingOffsets(
                    OffsetsInitializer.latest()
                )
                .setValueOnlyDeserializer(
                    new SimpleStringSchema()
                )
                .build();


        final DataStream<String> rawEvents =
            env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Raw Sensor Source"
            );


        final ObjectMapper mapper =
            new ObjectMapper()
                .setPropertyNamingStrategy(
                    PropertyNamingStrategies.SNAKE_CASE
                )
                .configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                    false
                );


        final DataStream<ValidationResult> validationResults =
    rawEvents
        .map(
            event -> {

                try {

                    SensorReading reading =
                        mapper.readValue(
                            event,
                            SensorReading.class
                        );

                    var result =
                        SensorReadingValidator
                            .validate(
                                reading
                            );

                    if (result.valid()) {

                        return new ValidationResult(
                            true,
                            reading,
                            null,
                            event
                        );
                    }

                    return new ValidationResult(
                        false,
                        reading,
                        result.reason(),
                        event
                    );

                } catch (Exception e) {

                    return new ValidationResult(
                        false,
                        null,
                        "INVALID_JSON: "
                            + e.getMessage(),
                        event
                    );
                }
            }
        );


final DataStream<SensorReading> validSensorReadings =
    validationResults
        .filter(
            ValidationResult::isValid
        )
        .map(
            ValidationResult::getReading
        )
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(
                    Duration.ofSeconds(10)
                )
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<SensorReading>() {

                        @Override
                        public long extractTimestamp(
                            SensorReading reading,
                            long recordTimestamp
                        ) {

                            return Instant
                                .parse(
                                    reading.getEventTimeUtc()
                                )
                                .toEpochMilli();
                        }
                    }
                )
        )
        .name(
            "Assign Sensor Event Time"
        );

    final DataStream<CleanSensorReading> cleanReadings =
    validSensorReadings
        .map(
            reading ->
                new CleanSensorReading(
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
                )
        )
        .name(
            "Transform To Clean Sensor Reading"
        );

    final KafkaSink<CleanSensorReading> cleanSink =
    KafkaSink
        .<CleanSensorReading>builder()
        .setBootstrapServers(
            "kafka:9092"
        )
        .setRecordSerializer(
            KafkaRecordSerializationSchema
                .builder()
                .setTopic(
                    "environment.sensor-readings.clean"
                )
                .setValueSerializationSchema(
                    new CleanSensorReadingSerializer()
                )
                .build()
        )
        .build();

final DataStream<ValidationResult> invalidEvents =
    validationResults
        .filter(
            result -> !result.isValid()
        );
final KafkaSink<DlqMessage> dlqSink =
    KafkaSink
        .<DlqMessage>builder()
        .setBootstrapServers(
            "kafka:9092"
        )
        .setRecordSerializer(
            KafkaRecordSerializationSchema
                .builder()
                .setTopic(
                    "environment.sensor-readings.dlq"
                )
                .setValueSerializationSchema(
                    new DlqMessageSerializer()
                )
                .build()
        )
        .build();

validSensorReadings
    .map(
        reading ->
            "VALID_STREAM: "
                + reading.getEventId()
    )
    .print("VALIDATION");


invalidEvents
    .map(
        result ->
            new DlqMessage(
                java.time.Instant
                    .now()
                    .toString(),
                result.getErrorReason(),
                result.getRawEvent(),
                "environment.sensor-readings.raw"
            )
    )
    .sinkTo(
        dlqSink
    )
    .name(
        "Write Invalid Events To DLQ"
    );

    cleanReadings
    .map(
        reading ->
            "CLEAN_STREAM: "
            + reading.getEventId()
            + " city="
            + reading.getCityName()
            + " pm25="
            + reading.getPm25()
    )
    .print("VALIDATION");


cleanReadings
    .sinkTo(
        cleanSink
    )
    .name(
        "Write Clean Sensor Events"
    );

        env.execute(
            "Environment Sensor Validation Job"
        );
    }


    
}
