package com.environment.platform.streaming.model;

import java.io.Serializable;

public class RawKafkaEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String rawPayload;
    private String topic;
    private int partition;
    private long offset;
    private long kafkaTimestamp;
    private String ingestedAtUtc;

    public RawKafkaEvent() {
    }

    public RawKafkaEvent(
        String rawPayload,
        String topic,
        int partition,
        long offset,
        long kafkaTimestamp,
        String ingestedAtUtc
    ) {
        this.rawPayload = rawPayload;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.ingestedAtUtc = ingestedAtUtc;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public void setKafkaTimestamp(long kafkaTimestamp) {
        this.kafkaTimestamp = kafkaTimestamp;
    }

    public String getIngestedAtUtc() {
        return ingestedAtUtc;
    }

    public void setIngestedAtUtc(String ingestedAtUtc) {
        this.ingestedAtUtc = ingestedAtUtc;
    }
}
