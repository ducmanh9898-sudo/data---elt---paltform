SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

-- Tránh một Kafka partition idle làm watermark toàn job đứng lại.
SET 'table.exec.source.idle-timeout' = '30 s';

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

    'properties.group.id' = 'flink-sensor-silver-v1',

    'scan.startup.mode' = 'latest-offset',

    'format' = 'json'
);

DESCRIBE sensor_kafka_silver_source;
