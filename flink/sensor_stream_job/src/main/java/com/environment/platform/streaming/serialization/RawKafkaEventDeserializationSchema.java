package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.RawKafkaEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawKafkaEventDeserializationSchema
    implements KafkaRecordDeserializationSchema<RawKafkaEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(
        ConsumerRecord<byte[], byte[]> record,
        Collector<RawKafkaEvent> out
    ) {

        String rawPayload =
            record.value() == null
                ? null
                : new String(
                    record.value(),
                    StandardCharsets.UTF_8
                );

        RawKafkaEvent event =
            new RawKafkaEvent(
                rawPayload,
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                Instant.now().toString()
            );

        out.collect(event);
    }

    @Override
    public TypeInformation<RawKafkaEvent> getProducedType() {
        return TypeInformation.of(
            RawKafkaEvent.class
        );
    }
}
