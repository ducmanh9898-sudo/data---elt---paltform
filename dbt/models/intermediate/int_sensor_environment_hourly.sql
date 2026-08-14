with sensor_events as (

    select
        event_id,
        device_id,

        city_id,
        city_name,
        country_code,

        event_time_utc,
        processed_at,

        temperature_2m,
        relative_humidity_2m,

        pm2_5,
        pm10,
        carbon_monoxide,
        nitrogen_dioxide

    from {{ ref('stg_sensor_readings') }}

),

hourly as (

    select
        city_id,

        date_trunc(
            'hour',
            event_time_utc
        ) as measured_at_utc,

        max(city_name) as city_name,
        max(country_code) as country_code,

        count(*) as sensor_event_count,
        count(distinct device_id) as sensor_device_count,

        avg(temperature_2m) as sensor_avg_temperature_2m,
        avg(relative_humidity_2m) as sensor_avg_relative_humidity_2m,

        avg(pm2_5) as sensor_avg_pm2_5,
        min(pm2_5) as sensor_min_pm2_5,
        max(pm2_5) as sensor_max_pm2_5,

        avg(pm10) as sensor_avg_pm10,
        min(pm10) as sensor_min_pm10,
        max(pm10) as sensor_max_pm10,

        avg(carbon_monoxide) as sensor_avg_carbon_monoxide,
        avg(nitrogen_dioxide) as sensor_avg_nitrogen_dioxide,

        min(event_time_utc) as first_sensor_event_at,
        max(event_time_utc) as last_sensor_event_at,
        max(processed_at) as sensor_data_updated_at

    from sensor_events

    group by
        city_id,
        date_trunc(
            'hour',
            event_time_utc
        )

)

select
    concat(
        cast(city_id as varchar),
        '|',
        cast(measured_at_utc as varchar)
    ) as city_hour_key,

    city_id,
    city_name,
    country_code,
    measured_at_utc,

    sensor_event_count,
    sensor_device_count,

    sensor_avg_temperature_2m,
    sensor_avg_relative_humidity_2m,

    sensor_avg_pm2_5,
    sensor_min_pm2_5,
    sensor_max_pm2_5,

    sensor_avg_pm10,
    sensor_min_pm10,
    sensor_max_pm10,

    sensor_avg_carbon_monoxide,
    sensor_avg_nitrogen_dioxide,

    first_sensor_event_at,
    last_sensor_event_at,
    sensor_data_updated_at

from hourly