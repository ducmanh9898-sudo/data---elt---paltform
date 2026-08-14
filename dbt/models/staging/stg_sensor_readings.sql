with source_data as (

    select *
    from {{ source('silver', 'sensor_readings_clean') }}

    where source_system = 'iot_simulator'
      and dataset_name = 'sensor_readings'
      and quality_status = 'VALID'
      and event_type = 'environment.sensor_reading'

)

select
    trim(schema_version) as schema_version,
    trim(event_type) as event_type,
    trim(event_id) as event_id,

    trim(source_system) as source_system,
    trim(dataset_name) as dataset_name,

    trim(device_id) as device_id,
    cast(sequence_number as bigint) as sequence_number,

    cast(city_id as bigint) as city_id,
    trim(city_name) as city_name,
    upper(trim(country_code)) as country_code,

    cast(event_time_utc as timestamp(6) with time zone) as event_time_utc,
    cast(produced_at_utc as timestamp(6) with time zone) as produced_at_utc,
    cast(processed_at as timestamp(6) with time zone) as processed_at,

    cast(temperature_2m as double) as temperature_2m,
    cast(relative_humidity_2m as double) as relative_humidity_2m,

    cast(pm2_5 as double) as pm2_5,
    cast(pm10 as double) as pm10,
    cast(carbon_monoxide as double) as carbon_monoxide,
    cast(nitrogen_dioxide as double) as nitrogen_dioxide,

    trim(quality_status) as quality_status,
    quality_error

from source_data