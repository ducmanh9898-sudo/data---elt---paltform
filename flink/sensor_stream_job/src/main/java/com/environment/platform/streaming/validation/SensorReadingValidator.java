package com.environment.platform.streaming.validation;

import com.environment.platform.streaming.model.SensorReading;

public final class SensorReadingValidator {

    private SensorReadingValidator() {
        // Utility class
    }

    public static ValidationResult validate(
        final SensorReading reading
    ) {

        if (reading == null) {
            return ValidationResult.invalid(
                "event is null"
            );
        }

        if (reading.getEventId() == null
            || reading.getEventId().isBlank()) {

            return ValidationResult.invalid(
                "missing event_id"
            );
        }

        if (reading.getSchemaVersion() == null
            || reading.getSchemaVersion().isBlank()) {

            return ValidationResult.invalid(
                "missing schema_version"
            );
        }

        if (reading.getCityId() == null) {

            return ValidationResult.invalid(
                "missing city_id"
            );
        }

        if (reading.getTemperature2m() != null
            && (
                reading.getTemperature2m() < -100
                || reading.getTemperature2m() > 100
            )) {

            return ValidationResult.invalid(
                "temperature_2m out of range"
            );
        }

        return ValidationResult.success();
    }


    public record ValidationResult(
        boolean valid,
        String reason
    ) {

       public static ValidationResult success() {
    return new ValidationResult(
        true,
        null
    );
}


        public static ValidationResult invalid(
            final String reason
        ) {
            return new ValidationResult(
                false,
                reason
            );
        }
    }
}
