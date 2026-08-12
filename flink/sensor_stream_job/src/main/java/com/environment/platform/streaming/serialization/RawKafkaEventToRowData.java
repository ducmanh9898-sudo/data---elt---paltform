package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.RawKafkaEvent;

import java.time.Instant;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public class RawKafkaEventToRowData
    implements MapFunction<RawKafkaEvent, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(RawKafkaEvent event) {

        GenericRowData row = new GenericRowData(6);

        // raw_payload
        row.setField(
            0,
            event.getRawPayload() == null
                ? null
                : StringData.fromString(event.getRawPayload())
        );

        // kafka_topic
        row.setField(
            1,
            event.getTopic() == null
                ? null
                : StringData.fromString(event.getTopic())
        );

        // kafka_partition
        row.setField(
            2,
            event.getPartition()
        );

        // kafka_offset
        row.setField(
            3,
            event.getOffset()
        );

        // kafka_timestamp
        row.setField(
            4,
            TimestampData.fromEpochMillis(
                event.getKafkaTimestamp()
            )
        );

        // ingested_at_utc
        row.setField(
            5,
            TimestampData.fromEpochMillis(
                Instant.parse(
                    event.getIngestedAtUtc()
                ).toEpochMilli()
            )
        );

        return row;
    }
}
