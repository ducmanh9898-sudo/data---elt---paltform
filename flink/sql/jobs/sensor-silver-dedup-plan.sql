SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

-- Tránh watermark bị đứng khi Kafka partition tạm thời không có dữ liệu.
SET 'table.exec.source.idle-timeout' = '30 s';

-- Không giữ event_id trong state vô hạn.
SET 'table.exec.state.ttl' = '24 h';

CREATE TEMPORARY TABLE sensor_kafka_silver_source (
    schema_version STRING,
    event_type STRING,
    event_id STRING,

    source_system STRING,
    dataset_name STRING,

    device_id STRING,
    sequence_number BIGINT,

    city_id INT,
    city_name STRING,
    country_code STRING,

    event_time_utc STRING,
    produced_at_utc STRING,

    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    pm2_5 DOUBLE,
    pm10 DOUBLE,
    carbon_monoxide DOUBLE,
    nitrogen_dioxide DOUBLE,

    event_time_ts AS (
        CASE
            WHEN event_time_utc IS NULL THEN
                CAST(NULL AS TIMESTAMP_LTZ(3))
            WHEN UNIX_TIMESTAMP(
                event_time_utc,
                'yyyy-MM-dd''T''HH:mm:ss.SSSX'
            ) = -9223372036854775808 THEN
                CAST(NULL AS TIMESTAMP_LTZ(3))
            ELSE TO_TIMESTAMP_LTZ(
                UNIX_TIMESTAMP(
                    event_time_utc,
                    'yyyy-MM-dd''T''HH:mm:ss.SSSX'
                ) * 1000,
                3
            )
        END
    ),

    WATERMARK FOR event_time_ts
        AS event_time_ts - INTERVAL '10' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'environment.sensor-readings.raw',
    'properties.bootstrap.servers' = 'kafka:9092',

    -- Khác Bronze consumer group.
    'properties.group.id' = 'flink-sensor-silver-v1',

    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

EXPLAIN PLAN FOR
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
    event_time_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time_ts ASC
        ) AS row_num
    FROM sensor_kafka_silver_source
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

        AND event_time_ts IS NOT NULL

        AND temperature_2m BETWEEN -80 AND 70
        AND relative_humidity_2m BETWEEN 0 AND 100
        AND pm2_5 BETWEEN 0 AND 2000
        AND pm10 BETWEEN 0 AND 3000
        AND carbon_monoxide BETWEEN 0 AND 100000
        AND nitrogen_dioxide BETWEEN 0 AND 2000
)
WHERE row_num = 1;
