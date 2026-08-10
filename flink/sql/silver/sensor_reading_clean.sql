CREATE TABLE silver.sensor_readings_clean (
    schema_version STRING,
    event_type STRING,
    event_id STRING,

    device_id STRING,

    city_id INT,
    city_name STRING,
    country_code STRING,

    event_time_utc TIMESTAMP_LTZ(3),
    produced_at_utc TIMESTAMP_LTZ(3),

    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    pm2_5 DOUBLE,
    pm10 DOUBLE,
    carbon_monoxide DOUBLE,
    nitrogen_dioxide DOUBLE,

    quality_status STRING,
    quality_error STRING,

    processed_at TIMESTAMP_LTZ(3)
);