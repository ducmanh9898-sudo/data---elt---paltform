USE CATALOG polaris;


INSERT INTO bronze.sensor_readings_raw

SELECT *

FROM default_catalog.default_database.sensor_kafka_source;