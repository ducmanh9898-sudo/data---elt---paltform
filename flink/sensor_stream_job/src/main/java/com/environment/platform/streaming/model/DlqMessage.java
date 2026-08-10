package com.environment.platform.streaming.model;

public class DlqMessage {

    private String failedAtUtc;

    private String errorReason;

    private String rawEvent;

    private String sourceTopic;


    public DlqMessage() {
    }


    public DlqMessage(
        final String failedAtUtc,
        final String errorReason,
        final String rawEvent,
        final String sourceTopic
    ) {
        this.failedAtUtc = failedAtUtc;
        this.errorReason = errorReason;
        this.rawEvent = rawEvent;
        this.sourceTopic = sourceTopic;
    }


    public String getFailedAtUtc() {
        return failedAtUtc;
    }


    public void setFailedAtUtc(
        final String failedAtUtc
    ) {
        this.failedAtUtc = failedAtUtc;
    }


    public String getErrorReason() {
        return errorReason;
    }


    public void setErrorReason(
        final String errorReason
    ) {
        this.errorReason = errorReason;
    }


    public String getRawEvent() {
        return rawEvent;
    }


    public void setRawEvent(
        final String rawEvent
    ) {
        this.rawEvent = rawEvent;
    }


    public String getSourceTopic() {
        return sourceTopic;
    }


    public void setSourceTopic(
        final String sourceTopic
    ) {
        this.sourceTopic = sourceTopic;
    }
}
