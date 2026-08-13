    package com.environment.platform.streaming;
    import com.environment.platform.streaming.sink.BronzeIcebergSink;
    import com.environment.platform.streaming.model.RawKafkaEvent;
    import com.environment.platform.streaming.serialization.RawKafkaEventDeserializationSchema;
    import com.environment.platform.streaming.model.AirQuality5MinAggregate;
    import com.environment.platform.streaming.process.AirQualityAggregateFunction;
    import com.environment.platform.streaming.process.AirQualityWindowProcessFunction;
    import com.environment.platform.streaming.sink.AirQuality5MinIcebergSink;
    import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
    import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
    import org.apache.flink.streaming.api.windowing.time.Time;
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
    import com.environment.platform.streaming.sink.LateKafkaSink;

    import org.apache.flink.util.OutputTag;
    public final class CanonicalSensorStreamJob {

    private CanonicalSensorStreamJob() {
    }

    public static void main(
        String[] args
    ) throws Exception {

        final StreamExecutionEnvironment env =
            StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setParallelism(1);


        // =========================================================
        // 1. KAFKA RAW SOURCE
        // =========================================================

        final KafkaSource<RawKafkaEvent> source =
            KafkaSource
                .<RawKafkaEvent>builder()
                .setBootstrapServers(
                    "kafka:9092"
                )
                .setTopics(
                    "environment.sensor-readings.raw"
                )
                .setGroupId(
                    "environment-canonical-v1"
                )
                .setStartingOffsets(
                    OffsetsInitializer.latest()
                )
                .setDeserializer(
                    new RawKafkaEventDeserializationSchema()
                )
                .build();


        final DataStream<RawKafkaEvent> rawEvents =
            env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Canonical Kafka Raw Source"
            );


        // =========================================================
        // 2. BRONZE
        // =========================================================

        BronzeIcebergSink.attach(
            rawEvents
        );


        // =========================================================
        // 3. PARSE + VALIDATE
        // =========================================================

        final SingleOutputStreamOperator<SensorReading>
            validSensorReadings =
                rawEvents
                    .process(
                        new ParseValidateSensorFunction()
                    )
                    .name(
                        "Parse And Validate Sensor Events"
                    );


        final DataStream<DlqMessage> dlqEvents =
            validSensorReadings
                .getSideOutput(
                    ParseValidateSensorFunction.DLQ_TAG
                );


        // =========================================================
        // 4. DLQ KAFKA SINK
        // =========================================================

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


        dlqEvents
            .sinkTo(
                dlqSink
            )
            .name(
                "Write Invalid Events To Kafka DLQ"
            );


        // =========================================================
        // 5. EVENT TIME + WATERMARK
        // =========================================================

        final DataStream<SensorReading>
            eventTimeSensorReadings =
                validSensorReadings
                    .assignTimestampsAndWatermarks(

                        WatermarkStrategy
                            .<SensorReading>
                                forBoundedOutOfOrderness(
                                    Duration.ofSeconds(10)
                                )

                            .withIdleness(
                                Duration.ofSeconds(30)
                            )

                            .withTimestampAssigner(
                                new SerializableTimestampAssigner<
                                    SensorReading
                                >() {

                                    private static final long
                                        serialVersionUID = 1L;

                                    @Override
                                    public long extractTimestamp(
                                        SensorReading reading,
                                        long recordTimestamp
                                    ) {

                                        return Instant
                                            .parse(
                                                reading
                                                    .getEventTimeUtc()
                                            )
                                            .toEpochMilli();
                                    }
                                }
                            )
                    )
                    .name(
                        "Assign Sensor Event Time And Watermarks"
                    );



        

        // =========================================================
        // 6. STATEFUL DEDUP
        // =========================================================

        final DataStream<SensorReading>
            deduplicatedSensorReadings =
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


       
        // =========================================================
        // 7. SILVER CLEAN
        // =========================================================

        SilverIcebergSink.attach(
            deduplicatedSensorReadings
        );


        // =========================================================
        // 8. 5-MINUTE EVENT-TIME WINDOW
        // =========================================================

        final OutputTag<SensorReading>
            lateSensorReadingTag =
                new OutputTag<SensorReading>(
                    "late-sensor-readings"
                ) {
                    private static final long
                        serialVersionUID = 1L;
                };


        final SingleOutputStreamOperator<
            AirQuality5MinAggregate
        > airQuality5MinAggregates =

            deduplicatedSensorReadings

                .keyBy(
                    SensorReading::getCityId
                )

                .window(
                    TumblingEventTimeWindows.of(
                        Time.minutes(5)
                    )
                )

                // Event vượt quá window sau khi watermark
                // đã đóng window sẽ không được aggregate lại.
                .allowedLateness(
                    Time.seconds(0)
                )

                // Route event quá muộn sang side output.
                .sideOutputLateData(
                    lateSensorReadingTag
                )

                .aggregate(
                    new AirQualityAggregateFunction(),
                    new AirQualityWindowProcessFunction()
                )

                .name(
                    "Aggregate Air Quality In 5 Minute Event Time Windows"
                )

                .uid(
                    "air-quality-5min-window-v1"
                );


        // =========================================================
        // 9. ON-TIME AGGREGATE → ICEBERG
        // =========================================================

        AirQuality5MinIcebergSink.attach(
            airQuality5MinAggregates
        );


        // Temporary aggregate debug
        


        // =========================================================
        // 10. TOO-LATE EVENTS → KAFKA LATE
        // =========================================================

        final DataStream<SensorReading>
            lateSensorReadings =
                airQuality5MinAggregates
                    .getSideOutput(
                        lateSensorReadingTag
                    );


        LateKafkaSink.attach(
            lateSensorReadings
        );


        
        // =========================================================
        // 11. EXECUTE
        // =========================================================

        env.execute(
            "Canonical Sensor Streaming Pipeline"
        );
    }
}
