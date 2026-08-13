    package com.environment.platform.streaming;
    import com.environment.platform.streaming.sink.BronzeIcebergSink;
    import com.environment.platform.streaming.model.RawKafkaEvent;
    import com.environment.platform.streaming.serialization.RawKafkaEventDeserializationSchema;

    import org.apache.flink.api.common.eventtime.WatermarkStrategy;
    import org.apache.flink.connector.kafka.source.KafkaSource;
    import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import com.environment.platform.streaming.model.DlqMessage;
    import com.environment.platform.streaming.model.SensorReading;
    import com.environment.platform.streaming.process.ParseValidateSensorFunction;
    import com.environment.platform.streaming.serialization.DlqMessageSerializer;
    import com.environment.platform.streaming.sink.SilverIcebergSink;
    import org.apache.flink.connector.base.DeliveryGuarantee;
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
    import org.apache.flink.connector.kafka.sink.KafkaSink;
    import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
    import java.time.Duration;
    import java.time.Instant;
    import com.environment.platform.streaming.process.DeduplicateSensorEventFunction;
    import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner; 
    public final class CanonicalSensorStreamJob {

        private CanonicalSensorStreamJob() {
        }

        public static void main(String[] args) throws Exception {

            final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

            // Source smoke test: dùng parallelism thấp trước.
            env.setParallelism(1);

            final KafkaSource<RawKafkaEvent> source =
                KafkaSource
                    .<RawKafkaEvent>builder()
                    .setBootstrapServers("kafka:9092")
                    .setTopics("environment.sensor-readings.raw")
                    .setGroupId("environment-canonical-v1")

                    // Canonical job mới:
                    // không đọc lại toàn bộ Kafka history ở smoke test.
                    .setStartingOffsets(
                        OffsetsInitializer.latest()
                    )

                    // Quan trọng:
                    // lấy toàn bộ ConsumerRecord + Kafka metadata.
                    .setDeserializer(
                        new RawKafkaEventDeserializationSchema()
                    )
                    .build();

            final DataStream<RawKafkaEvent> rawEvents =
                env.fromSource(
                    source,

                    // Chưa parse event_time ở ingestion layer,
                    // nên raw source chưa gán watermark.
                    WatermarkStrategy.noWatermarks(),

                    "Canonical Kafka Raw Source"
                );
                BronzeIcebergSink.attach(rawEvents);


            rawEvents
                .map(
                    event ->
                        String.format(
                            "RAW_KAFKA_EVENT " +
                            "topic=%s " +
                            "partition=%d " +
                            "offset=%d " +
                            "kafkaTimestamp=%d " +
                            "ingestedAtUtc=%s " +
                            "payload=%s",
                            event.getTopic(),
                            event.getPartition(),
                            event.getOffset(),
                            event.getKafkaTimestamp(),
                            event.getIngestedAtUtc(),
                            event.getRawPayload()
                        )
                )
                .name("Print Kafka Metadata")
                .print("CANONICAL");

          
        final SingleOutputStreamOperator<SensorReading> validSensorReadings =
            rawEvents
                .process(
                    new ParseValidateSensorFunction()
                )
                .name(
                    "Parse And Validate Sensor Events"
                );

        final DataStream<SensorReading> eventTimeSensorReadings =
    validSensorReadings
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(
                    Duration.ofSeconds(10)
                )
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<SensorReading>() {

                        private static final long serialVersionUID = 1L;

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
            "Assign Sensor Event Time And Watermarks"
        );

        final DataStream<DlqMessage> dlqEvents =
            validSensorReadings
                .getSideOutput(
                    ParseValidateSensorFunction.DLQ_TAG
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
        .setDeliveryGuarantee(
            DeliveryGuarantee.AT_LEAST_ONCE
        )
        .build();

        final DataStream<SensorReading> deduplicatedSensorReadings =
    eventTimeSensorReadings
        .keyBy(
            SensorReading::getEventId
        )
        .process(
            new DeduplicateSensorEventFunction()
        )
        .name(
            "Deduplicate Sensor Events By Event ID"
        )
        .uid(
            "deduplicate-sensor-events-v1"
        );
        SilverIcebergSink.attach(
    deduplicatedSensorReadings
);
        deduplicatedSensorReadings
    .map(
        reading ->
            "DEDUP_STREAM event_id="
                + reading.getEventId()
                + " event_time_utc="
                + reading.getEventTimeUtc()
    )
    .name(
        "Debug Deduplicated Stream"
    )
    .print(
        "DEDUP"
    );

dlqEvents
    .sinkTo(
        dlqSink
    )
    .name(
        "Write Invalid Events To Kafka DLQ"
    );
    eventTimeSensorReadings
    .map(
        reading ->
            "EVENT_TIME_STREAM event_id="
                + reading.getEventId()
                + " event_time_utc="
                + reading.getEventTimeUtc()
    )
    .name(
        "Debug Event Time Stream"
    )
    .print(
        "EVENT_TIME"
    );
        
          env.execute(
                "Canonical Sensor Stream Source "
            );
    }
    }
