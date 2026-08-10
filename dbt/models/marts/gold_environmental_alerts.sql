with hourly_environment as (

    select *
    from {{ ref('gold_city_environment_hourly') }}

),

alerts as (

    select
        city_hour_key as alert_id,

        city_id,
        city_name,
        country_code,
        country_name,
        latitude,
        longitude,
        timezone,

        measured_at_utc,
        measured_at_local,
        measurement_date_utc,
        measurement_date_local,
        measurement_hour_local,

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

        case
            when air_quality_status_rank >= 6 then 'critical'
            when air_quality_status_rank = 5 then 'severe'
            when air_quality_status_rank = 4 then 'high'
            else 'warning'
        end as alert_severity,

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
        has_precipitation,

        data_updated_at_utc

    from hourly_environment

    where is_air_quality_alert = true

)

select *
from alerts
