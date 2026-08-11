USE CATALOG polaris;


CREATE TABLE IF NOT EXISTS silver.sensor_readings_dedup_stream (

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

    event_time_utc TIMESTAMP_LTZ(6),
    produced_at_utc TIMESTAMP_LTZ(6),

    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,

    pm2_5 DOUBLE,
    pm10 DOUBLE,

    carbon_monoxide DOUBLE,
    nitrogen_dioxide DOUBLE,

    quality_status STRING,
    quality_error STRING,

    processed_at TIMESTAMP_LTZ(6),

    PRIMARY KEY (event_id) NOT ENFORCED
)
WITH (
    'format-version'='2'
);