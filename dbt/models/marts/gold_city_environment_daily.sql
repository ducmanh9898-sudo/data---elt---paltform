{{
    config(
        materialized='table',
        on_table_exists='replace'
    )
}}

with hourly as (

    select *
    from {{ ref('gold_city_environment_hourly') }}

),

daily_aggregated as (

    select
        concat(
            cast(city_id as varchar),
            '|',
            cast(measurement_date_local as varchar)
        ) as city_date_key,

        -- Business grain
        city_id,
        measurement_date_local,

        -- Latest descriptive values for the city
        max_by(
            city_name,
            measured_at_utc
        ) as city_name,

        max_by(
            country_code,
            measured_at_utc
        ) as country_code,

        max_by(
            country_name,
            measured_at_utc
        ) as country_name,

        max_by(
            latitude,
            measured_at_utc
        ) as latitude,

        max_by(
            longitude,
            measured_at_utc
        ) as longitude,

        max_by(
            timezone,
            measured_at_utc
        ) as timezone,

        -- Date dimensions
        year(measurement_date_local)
            as measurement_year_local,

        month(measurement_date_local)
            as measurement_month_local,

        day(measurement_date_local)
            as measurement_day_local,

        day_of_week(measurement_date_local)
            as measurement_day_of_week_local,

        -- Data coverage
        count(*) as observed_hour_count,

        -- Air quality
        avg(pm2_5) as avg_pm2_5,
        min(pm2_5) as min_pm2_5,
        max(pm2_5) as max_pm2_5,

        avg(pm10) as avg_pm10,
        min(pm10) as min_pm10,
        max(pm10) as max_pm10,

        avg(us_aqi) as avg_us_aqi,
        min(us_aqi) as min_us_aqi,
        max(us_aqi) as max_us_aqi,

        sum(
            case
                when is_air_quality_alert then 1
                else 0
            end
        ) as air_quality_alert_hour_count,

        case
            when sum(
                case
                    when is_air_quality_alert then 1
                    else 0
                end
            ) > 0 then true
            else false
        end as has_air_quality_alert,

        max(air_quality_status_rank)
            as worst_air_quality_status_rank,

        max_by(
            air_quality_status,
            air_quality_status_rank
        ) as worst_air_quality_status,

        max_by(
            air_quality_alert_level,
            air_quality_status_rank
        ) as worst_air_quality_alert_level,

        -- Weather
        avg(temperature_2m)
            as avg_temperature_2m,

        min(temperature_2m)
            as min_temperature_2m,

        max(temperature_2m)
            as max_temperature_2m,

        avg(relative_humidity_2m)
            as avg_relative_humidity_2m,

        sum(coalesce(precipitation, 0))
            as total_precipitation,

        sum(coalesce(rain, 0))
            as total_rain,

        avg(surface_pressure)
            as avg_surface_pressure,

        avg(cloud_cover)
            as avg_cloud_cover,

        avg(wind_speed_10m)
            as avg_wind_speed_10m,

        max(wind_speed_10m)
            as max_wind_speed_10m,

        avg(visibility)
            as avg_visibility,

        sum(
            case
                when has_precipitation then 1
                else 0
            end
        ) as precipitation_hour_count,

        -- Lineage
        max(data_updated_at_utc)
            as data_updated_at_utc

    from hourly

    group by
        city_id,
        measurement_date_local

)

select *
from daily_aggregated
