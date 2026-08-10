with air_quality as (

    select *
    from {{ ref('stg_air_quality_hourly') }}

),

weather as (

    select *
    from {{ ref('stg_weather_hourly') }}

),

joined as (

    select
        concat(
            cast(air.city_id as varchar),
            '|',
            cast(air.measured_at_utc as varchar)
        ) as city_hour_key,

        air.city_id,

        coalesce(
            air.city_name,
            weather.city_name
        ) as city_name,

        coalesce(
            air.country_code,
            weather.country_code
        ) as country_code,

        coalesce(
            air.country_name,
            weather.country_name
        ) as country_name,

        coalesce(
            air.latitude,
            weather.latitude
        ) as latitude,

        coalesce(
            air.longitude,
            weather.longitude
        ) as longitude,

        coalesce(
            air.timezone,
            weather.timezone
        ) as timezone,

        coalesce(
            air.utc_offset_seconds,
            weather.utc_offset_seconds
        ) as utc_offset_seconds,

        air.measured_at_utc,

        coalesce(
            air.measured_at_local,
            weather.measured_at_local
        ) as measured_at_local,

        coalesce(
            air.measurement_date_utc,
            weather.measurement_date_utc
        ) as measurement_date_utc,

        coalesce(
            air.measurement_date_local,
            weather.measurement_date_local
        ) as measurement_date_local,

        -- Air quality
        air.pm2_5,
        air.pm10,
        air.carbon_monoxide,
        air.nitrogen_dioxide,
        air.sulphur_dioxide,
        air.ozone,
        air.us_aqi,

        -- Weather
        weather.temperature_2m,
        weather.relative_humidity_2m,
        weather.precipitation,
        weather.rain,
        weather.surface_pressure,
        weather.cloud_cover,
        weather.wind_speed_10m,
        weather.wind_direction_10m,
        weather.visibility,
        weather.weather_code,

        -- Lineage
        air.source_system
            as air_quality_source_system,

        weather.source_system
            as weather_source_system,

        air.crawled_at_utc
            as air_quality_crawled_at_utc,

        weather.crawled_at_utc
            as weather_crawled_at_utc,

        case
            when air.crawled_at_utc is null
                then weather.crawled_at_utc
            when weather.crawled_at_utc is null
                then air.crawled_at_utc
            else greatest(
                air.crawled_at_utc,
                weather.crawled_at_utc
            )
        end as latest_crawled_at_utc

    from air_quality as air

    inner join weather
        on air.city_id = weather.city_id
        and air.measured_at_utc
            = weather.measured_at_utc

)

select *
from joined
