package com.environment.platform.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * First bounded checkpoint for the streaming pipeline.
 *
 * This job only verifies:
 * Kafka -> Flink source -> TaskManager logs
 *
 * JSON parsing, validation, watermarking and Iceberg sinks
 * will be added in later checkpoints.
 */
public final class KafkaSourceSmokeJob {

    private static final String DEFAULT_BOOTSTRAP_SERVERS =
        "kafka:9092";

    private static final String DEFAULT_RAW_TOPIC =
        "environment.sensor-readings.raw";

    private static final String DEFAULT_CONSUMER_GROUP =
        "environment-flink-sensor-smoke-v1";

    private KafkaSourceSmokeJob() {
        // Utility class.
    }

    public static void main(final String[] args)
        throws Exception {

        final String bootstrapServers = getEnvironmentValue(
            "KAFKA_BOOTSTRAP_SERVERS",
            DEFAULT_BOOTSTRAP_SERVERS
        );

        final String rawTopic = getEnvironmentValue(
            "KAFKA_RAW_TOPIC",
            DEFAULT_RAW_TOPIC
        );

        final String consumerGroup = getEnvironmentValue(
            "KAFKA_CONSUMER_GROUP",
            DEFAULT_CONSUMER_GROUP
        );

        System.out.printf(
            "Starting Kafka source smoke job: "
                + "bootstrapServers=%s, topic=%s, groupId=%s%n",
            bootstrapServers,
            rawTopic,
            consumerGroup
        );

        final StreamExecutionEnvironment environment =
            StreamExecutionEnvironment
                .getExecutionEnvironment();

        environment.setParallelism(2);

        final KafkaSource<String> kafkaSource =
            KafkaSource
                .<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(rawTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(
                    OffsetsInitializer.latest()
                )
                .setValueOnlyDeserializer(
                    new SimpleStringSchema()
                )
                .build();

        final DataStream<String> rawEvents =
            environment.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Sensor Raw Source"
            );

        rawEvents
            .print("SENSOR_RAW")
            .name("Print Raw Sensor Events")
            .uid("print-raw-sensor-events");

        environment.execute(
            "Environment Sensor Kafka Source Smoke Job"
        );
    }

    private static String getEnvironmentValue(
        final String name,
        final String defaultValue
    ) {
        final String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value.trim();
    }
}
