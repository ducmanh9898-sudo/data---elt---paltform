with threshold_status as (

    select *
    from {{ ref('int_air_quality_threshold_status') }}

),

sensor_hourly as (

    select *
    from {{ ref('int_sensor_environment_hourly') }}

),

final as (

    select
        -- Business key
        t.city_hour_key,

        -- Location
        t.city_id,
        t.city_name,
        t.country_code,
        t.country_name,
        t.latitude,
        t.longitude,
        t.timezone,
        t.utc_offset_seconds,

        -- Time
        t.measured_at_utc,
        t.measured_at_local,
        t.measurement_date_utc,
        t.measurement_date_local,

        year(t.measurement_date_local)
            as measurement_year_local,

        month(t.measurement_date_local)
            as measurement_month_local,

        day(t.measurement_date_local)
            as measurement_day_local,

        day_of_week(t.measurement_date_local)
            as measurement_day_of_week_local,

        hour(t.measured_at_local)
            as measurement_hour_local,

        -- Batch air quality
        t.pm2_5,
        t.pm10,
        t.carbon_monoxide,
        t.nitrogen_dioxide,
        t.sulphur_dioxide,
        t.ozone,
        t.us_aqi,

        -- Air-quality classification
        t.air_quality_status,
        t.air_quality_status_rank,
        t.air_quality_alert_level,
        t.is_air_quality_alert,

        -- Batch weather
        t.temperature_2m,
        t.relative_humidity_2m,
        t.precipitation,
        t.rain,
        t.surface_pressure,
        t.cloud_cover,
        t.wind_speed_10m,
        t.wind_direction_10m,
        t.visibility,
        t.weather_code,

        -- Streaming sensor availability
        case
            when s.city_id is not null then true
            else false
        end as has_sensor_data,

        -- Streaming sensor coverage
        s.sensor_event_count,
        s.sensor_device_count,

        -- Streaming sensor weather
        s.sensor_avg_temperature_2m,
        s.sensor_avg_relative_humidity_2m,

        -- Streaming sensor air quality
        s.sensor_avg_pm2_5,
        s.sensor_min_pm2_5,
        s.sensor_max_pm2_5,

        s.sensor_avg_pm10,
        s.sensor_min_pm10,
        s.sensor_max_pm10,

        s.sensor_avg_carbon_monoxide,
        s.sensor_avg_nitrogen_dioxide,

        -- Sensor minus batch differences
        case
            when s.sensor_avg_pm2_5 is not null
             and t.pm2_5 is not null
                then s.sensor_avg_pm2_5 - t.pm2_5
        end as pm2_5_sensor_minus_batch,

        case
            when s.sensor_avg_pm10 is not null
             and t.pm10 is not null
                then s.sensor_avg_pm10 - t.pm10
        end as pm10_sensor_minus_batch,

        case
            when s.sensor_avg_temperature_2m is not null
             and t.temperature_2m is not null
                then s.sensor_avg_temperature_2m - t.temperature_2m
        end as temperature_2m_sensor_minus_batch,

        case
            when s.sensor_avg_relative_humidity_2m is not null
             and t.relative_humidity_2m is not null
                then s.sensor_avg_relative_humidity_2m
                     - t.relative_humidity_2m
        end as relative_humidity_2m_sensor_minus_batch,

        -- Streaming event lineage
        s.first_sensor_event_at,
        s.last_sensor_event_at,
        s.sensor_data_updated_at,

        -- Batch source lineage
        t.air_quality_source_system,
        t.weather_source_system,
        t.air_quality_crawled_at_utc,
        t.weather_crawled_at_utc,
        t.latest_crawled_at_utc

    from threshold_status t

    left join sensor_hourly s
      on t.city_id = s.city_id
     and t.measured_at_utc = s.measured_at_utc

)

select *
from final