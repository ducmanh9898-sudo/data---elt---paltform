package com.environment.platform.streaming.model;

public class ValidationResult {

    private boolean valid;

    private SensorReading reading;

    private String errorReason;

    private String rawEvent;


    public ValidationResult() {
    }


    public ValidationResult(
        final boolean valid,
        final SensorReading reading,
        final String errorReason,
        final String rawEvent
    ) {
        this.valid = valid;
        this.reading = reading;
        this.errorReason = errorReason;
        this.rawEvent = rawEvent;
    }


    public boolean isValid() {
        return valid;
    }


    public SensorReading getReading() {
        return reading;
    }


    public String getErrorReason() {
        return errorReason;
    }


    public String getRawEvent() {
        return rawEvent;
    }
}
