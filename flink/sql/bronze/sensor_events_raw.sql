CREATE TABLE IF NOT EXISTS polaris.bronze.sensor_events_raw (
    raw_payload       STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP_LTZ(3),
    ingested_at_utc   TIMESTAMP_LTZ(3)
)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
