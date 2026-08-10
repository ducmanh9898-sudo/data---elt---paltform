package com.environment.platform.streaming.serialization;

import com.environment.platform.streaming.model.DlqMessage;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;


public class DlqMessageSerializer
    implements SerializationSchema<DlqMessage> {


    @Override
    public byte[] serialize(
        final DlqMessage message
    ) {

        String json =
            "{"
            + "\"failedAtUtc\":\""
            + escape(message.getFailedAtUtc())
            + "\","
            + "\"errorReason\":\""
            + escape(message.getErrorReason())
            + "\","
            + "\"rawEvent\":\""
            + escape(message.getRawEvent())
            + "\","
            + "\"sourceTopic\":\""
            + escape(message.getSourceTopic())
            + "\""
            + "}";


        return json.getBytes(
            StandardCharsets.UTF_8
        );
    }


    private String escape(
        String value
    ) {

        if (value == null) {
            return "";
        }

        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"");
    }
}