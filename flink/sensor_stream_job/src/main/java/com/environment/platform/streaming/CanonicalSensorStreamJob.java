    package com.environment.platform.streaming;
    import com.environment.platform.streaming.sink.BronzeIcebergSink;
    import com.environment.platform.streaming.model.RawKafkaEvent;
    import com.environment.platform.streaming.serialization.RawKafkaEventDeserializationSchema;

    import org.apache.flink.api.common.eventtime.WatermarkStrategy;
    import org.apache.flink.connector.kafka.source.KafkaSource;
    import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

            env.execute(
                "Canonical Sensor Stream Source "
            );
        }
    }
