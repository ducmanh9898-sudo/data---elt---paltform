CREATE TABLE sensor_kafka_source (

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

nitrogen_dioxide DOUBLE


)

WITH (

'connector'='kafka',

'topic'='environment.sensor-readings.raw',

'properties.bootstrap.servers'='kafka:9092',

'properties.group.id'='flink-sensor-bronze-v1',

'scan.startup.mode'='earliest-offset',

'format'='json'

);


USE CATALOG polaris;


INSERT INTO bronze.sensor_readings_raw_v3


SELECT *


FROM default_catalog.default_database.sensor_kafka_source;

