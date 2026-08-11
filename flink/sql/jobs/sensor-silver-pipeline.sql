SET 'execution.runtime-mode' = 'streaming';

SET 'table.dynamic-table-options.enabled' = 'true';

SET 'table.local-time-zone' = 'UTC';


USE CATALOG polaris;


INSERT INTO silver.sensor_readings_clean

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

    CAST(
        TO_TIMESTAMP_LTZ(
            UNIX_TIMESTAMP(
                event_time_utc,
                'yyyy-MM-dd''T''HH:mm:ss.SSSX'
            ) * 1000,
            3
        )
        AS TIMESTAMP_LTZ(6)
    ) AS event_time_utc,

    CAST(
        TO_TIMESTAMP_LTZ(
            UNIX_TIMESTAMP(
                produced_at_utc,
                'yyyy-MM-dd''T''HH:mm:ss.SSSX'
            ) * 1000,
            3
        )
        AS TIMESTAMP_LTZ(6)
    ) AS produced_at_utc,

    temperature_2m,
    relative_humidity_2m,
    pm2_5,
    pm10,
    carbon_monoxide,
    nitrogen_dioxide,

    CAST('VALID' AS STRING) AS quality_status,

    CAST(NULL AS STRING) AS quality_error,

    CAST(
        CURRENT_TIMESTAMP
        AS TIMESTAMP_LTZ(6)
    ) AS processed_at

FROM bronze.sensor_readings_raw_v3
/*+ OPTIONS(
    'streaming'='true',
    'monitor-interval'='5s',
    'starting-strategy'='TABLE_SCAN_THEN_INCREMENTAL'
) */

WHERE
    event_id IS NOT NULL
    AND TRIM(event_id) <> ''

    AND source_system IS NOT NULL
    AND TRIM(source_system) <> ''

    AND dataset_name IS NOT NULL
    AND TRIM(dataset_name) <> ''

    AND device_id IS NOT NULL
    AND TRIM(device_id) <> ''

    AND sequence_number IS NOT NULL

    AND city_id BETWEEN 1 AND 12

    AND city_name IS NOT NULL
    AND TRIM(city_name) <> ''

    AND country_code IS NOT NULL
    AND CHAR_LENGTH(country_code) = 2

    AND temperature_2m BETWEEN -80 AND 70

    AND relative_humidity_2m BETWEEN 0 AND 100

    AND pm2_5 BETWEEN 0 AND 2000

    AND pm10 BETWEEN 0 AND 3000

    AND carbon_monoxide BETWEEN 0 AND 100000

    AND nitrogen_dioxide BETWEEN 0 AND 2000

    AND UNIX_TIMESTAMP(
        event_time_utc,
        'yyyy-MM-dd''T''HH:mm:ss.SSSX'
    ) <> -9223372036854775808

    AND UNIX_TIMESTAMP(
        produced_at_utc,
        'yyyy-MM-dd''T''HH:mm:ss.SSSX'
    ) <> -9223372036854775808;