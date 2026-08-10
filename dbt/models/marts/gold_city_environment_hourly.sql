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

        -- Air quality
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

        -- Weather
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

        -- Data lineage
        latest_crawled_at_utc as data_updated_at_utc

    from city_environment_hourly

)

select *
from final

