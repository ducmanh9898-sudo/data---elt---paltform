CREATE TABLE IF NOT EXISTS polaris.silver.sensor_air_quality_5min (
    window_start                 TIMESTAMP_LTZ(6),
    window_end                   TIMESTAMP_LTZ(6),

    city_id                      INT,
    city_name                    STRING,
    country_code                 STRING,

    reading_count                BIGINT,

    avg_temperature_2m           DOUBLE,
    avg_relative_humidity_2m     DOUBLE,

    avg_pm2_5                    DOUBLE,
    min_pm2_5                    DOUBLE,
    max_pm2_5                    DOUBLE,

    avg_pm10                     DOUBLE,
    min_pm10                     DOUBLE,
    max_pm10                     DOUBLE,

    avg_carbon_monoxide          DOUBLE,
    avg_nitrogen_dioxide         DOUBLE,

    processed_at                 TIMESTAMP_LTZ(6)
)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
