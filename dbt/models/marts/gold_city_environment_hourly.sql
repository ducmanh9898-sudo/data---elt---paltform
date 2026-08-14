with city_environment_hourly as (

    select *
    from {{ ref('int_city_environment_hourly') }}

),

final as (

    select
        -- Business key
        city_hour_key,

        -- Location
        city_id,
        city_name,
        country_code,
        country_name,
        latitude,
        longitude,
        timezone,

        -- Time dimensions
        measured_at_utc,
        measured_at_local,
        measurement_date_utc,
        measurement_date_local,
        measurement_year_local,
        measurement_month_local,
        measurement_day_local,
        measurement_day_of_week_local,
        measurement_hour_local,

        -- Batch air quality
        pm2_5,
        pm10,
        carbon_monoxide,
        nitrogen_dioxide,
        sulphur_dioxide,
        ozone,
        us_aqi,

        air_quality_status,
        air_quality_status_rank,
        air_quality_alert_level,
        is_air_quality_alert,

        -- Batch weather
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        rain,
        surface_pressure,
        cloud_cover,
        wind_speed_10m,
        wind_direction_10m,
        visibility,
        weather_code,

        case
            when coalesce(precipitation, 0) > 0
                or coalesce(rain, 0) > 0
                then true
            else false
        end as has_precipitation,

        -- Streaming sensor availability
        has_sensor_data,
        sensor_event_count,
        sensor_device_count,

        -- Streaming sensor weather
        sensor_avg_temperature_2m,
        sensor_avg_relative_humidity_2m,

        -- Streaming sensor air quality
        sensor_avg_pm2_5,
        sensor_min_pm2_5,
        sensor_max_pm2_5,

        sensor_avg_pm10,
        sensor_min_pm10,
        sensor_max_pm10,

        sensor_avg_carbon_monoxide,
        sensor_avg_nitrogen_dioxide,

        -- Sensor minus batch differences
        pm2_5_sensor_minus_batch,
        pm10_sensor_minus_batch,
        temperature_2m_sensor_minus_batch,
        relative_humidity_2m_sensor_minus_batch,

        -- Streaming lineage
        first_sensor_event_at,
        last_sensor_event_at,
        sensor_data_updated_at,

        -- Combined freshness
        case
            when sensor_data_updated_at is null
                then latest_crawled_at_utc

            when latest_crawled_at_utc is null
                then sensor_data_updated_at

            when sensor_data_updated_at > latest_crawled_at_utc
                then sensor_data_updated_at

            else latest_crawled_at_utc
        end as data_updated_at_utc

    from city_environment_hourly

)

select *
from final