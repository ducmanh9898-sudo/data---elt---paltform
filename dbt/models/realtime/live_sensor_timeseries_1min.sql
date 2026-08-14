{{ config(
    materialized='view',
    schema='realtime'
) }}

with recent_events as (

    select *

    from {{ ref('stg_sensor_readings') }}

    where event_time_utc
        >= current_timestamp - interval '60' minute

),

aggregated as (

    select
        city_id,

        max(city_name)
            as city_name,

        max(country_code)
            as country_code,

        date_trunc(
            'minute',
            event_time_utc
        ) as minute_utc,

        count(*)
            as sensor_event_count,

        count(distinct device_id)
            as sensor_device_count,

        avg(pm2_5)
            as avg_pm2_5,

        min(pm2_5)
            as min_pm2_5,

        max(pm2_5)
            as max_pm2_5,

        avg(pm10)
            as avg_pm10,

        avg(temperature_2m)
            as avg_temperature_2m,

        avg(relative_humidity_2m)
            as avg_relative_humidity_2m,

        max(processed_at)
            as data_updated_at_utc

    from recent_events

    group by
        city_id,
        date_trunc(
            'minute',
            event_time_utc
        )

)

select
    concat(
        cast(city_id as varchar),
        '|',
        cast(minute_utc as varchar)
    ) as city_minute_key,

    city_id,
    city_name,
    country_code,

    minute_utc,

    sensor_event_count,
    sensor_device_count,

    avg_pm2_5,
    min_pm2_5,
    max_pm2_5,

    avg_pm10,
    avg_temperature_2m,
    avg_relative_humidity_2m,

    data_updated_at_utc

from aggregated