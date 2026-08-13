package com.environment.platform.streaming.sink;

import com.environment.platform.streaming.model.SensorReading;
import com.environment.platform.streaming.serialization.LateSensorReadingSerializer;

import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.kafka.clients.producer.ProducerRecord;


public final class LateKafkaSink {

    private static final String LATE_TOPIC =
        "environment.sensor-readings.late";


    private LateKafkaSink() {
    }


    public static void attach(
        DataStream<SensorReading> lateReadings
    ) {

        /*
         * Quan trọng:
         *
         * Không dùng timestamp event-time của Flink làm
         * Kafka record timestamp cho Late topic.
         *
         * event_time_utc vẫn được giữ bên trong JSON payload.
         *
         * ProducerRecord constructor bên dưới không truyền
         * timestamp -> Kafka producer dùng thời gian hiện tại.
         */
        KafkaRecordSerializationSchema<SensorReading>
            recordSerializer =
                new KafkaRecordSerializationSchema<
                    SensorReading
                >() {

                    private static final long
                        serialVersionUID = 1L;

                    private final
                        LateSensorReadingSerializer
                            valueSerializer =
                                new LateSensorReadingSerializer();


                    @Override
                    public ProducerRecord<byte[], byte[]>
                        serialize(
                            SensorReading reading,
                            KafkaSinkContext context,
                            Long timestamp
                        ) {

                        byte[] key = null;

                        if (
                            reading.getEventId() != null
                        ) {

                            key =
                                reading
                                    .getEventId()
                                    .getBytes(
                                        StandardCharsets.UTF_8
                                    );
                        }


                        byte[] value =
                            valueSerializer.serialize(
                                reading
                            );


                        /*
                         * Constructor không truyền timestamp.
                         *
                         * Kafka metadata timestamp
                         * = producer current time.
                         *
                         * Original event time
                         * vẫn nằm ở:
                         *
                         * sensor_reading.event_time_utc
                         */
                        return new ProducerRecord<
                            byte[],
                            byte[]
                        >(
                            LATE_TOPIC,
                            key,
                            value
                        );
                    }
                };


        KafkaSink<SensorReading> sink =
            KafkaSink
                .<SensorReading>builder()

                .setBootstrapServers(
                    envOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "kafka:9092"
                    )
                )

                .setRecordSerializer(
                    recordSerializer
                )

                .setDeliveryGuarantee(
                    DeliveryGuarantee.AT_LEAST_ONCE
                )

                .build();


        lateReadings
            .sinkTo(
                sink
            )
            .name(
                "Write Too Late Sensor Events To Kafka"
            )
            .uid(
                "late-kafka-sink-v1"
            );
    }


    private static String envOrDefault(
        String name,
        String defaultValue
    ) {

        String value =
            System.getenv(name);

        if (
            value == null ||
            value.isBlank()
        ) {
            return defaultValue;
        }

        return value;
    }
}