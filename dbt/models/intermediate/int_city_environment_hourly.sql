with threshold_status as (

    select *
    from {{ ref('int_air_quality_threshold_status') }}

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
        utc_offset_seconds,

        -- Time
        measured_at_utc,
        measured_at_local,
        measurement_date_utc,
        measurement_date_local,

        year(measurement_date_local)
            as measurement_year_local,

        month(measurement_date_local)
            as measurement_month_local,

        day(measurement_date_local)
            as measurement_day_local,

        day_of_week(measurement_date_local)
            as measurement_day_of_week_local,

        hour(measured_at_local)
            as measurement_hour_local,

        -- Air quality
        pm2_5,
        pm10,
        carbon_monoxide,
        nitrogen_dioxide,
        sulphur_dioxide,
        ozone,
        us_aqi,

        -- Air-quality classification
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

        -- Source lineage
        air_quality_source_system,
        weather_source_system,
        air_quality_crawled_at_utc,
        weather_crawled_at_utc,
        latest_crawled_at_utc

    from threshold_status

)

select *
from final
