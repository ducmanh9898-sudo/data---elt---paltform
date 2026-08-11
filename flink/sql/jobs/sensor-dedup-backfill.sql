SET 'execution.runtime-mode' = 'batch';
USE CATALOG polaris;


INSERT INTO silver.sensor_readings_dedup_v2

SELECT
    schema_version,
    event_type,
    event_id,
    source_system,
    dataset_name,
    device_id,
    sequence_number,
    city_id,
    city_name,
    country_code,
    event_time_utc,
    produced_at_utc,
    temperature_2m,
    relative_humidity_2m,
    pm2_5,
    pm10,
    carbon_monoxide,
    nitrogen_dioxide,
    quality_status,
    quality_error,
    processed_at

FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY processed_at DESC
        ) AS rn

    FROM silver.sensor_readings_clean
)

WHERE rn = 1;