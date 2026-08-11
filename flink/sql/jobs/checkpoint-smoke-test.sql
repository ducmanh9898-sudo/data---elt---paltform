SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE checkpoint_test_source (
    event_id STRING
)
WITH (
    'connector' = 'kafka',
    'topic' = 'environment.sensor-readings.raw',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-checkpoint-smoke-v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

CREATE TEMPORARY TABLE checkpoint_test_sink (
    event_id STRING
)
WITH (
    'connector' = 'blackhole'
);

INSERT INTO checkpoint_test_sink
SELECT event_id
FROM checkpoint_test_source;
