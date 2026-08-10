with source_data as (

    select *
    from {{ source('silver', 'weather_hourly') }}

)

select
    cast(city_id as bigint) as city_id,
    trim(city_name) as city_name,
    upper(trim(country_code)) as country_code,
    trim(country_name) as country_name,

    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    trim(timezone) as timezone,
    cast(utc_offset_seconds as integer) as utc_offset_seconds,

    trim(source) as source,
    trim(source_system) as source_system,
    trim(dataset_name) as dataset_name,
    trim(dataset_kind) as dataset_kind,
    trim(record_type) as record_type,

    cast(requested_past_days as integer) as requested_past_days,
    cast(requested_forecast_days as integer) as requested_forecast_days,

    source_file,

    cast(crawled_at_utc as timestamp(6) with time zone) as crawled_at_utc,
    cast(measured_at_local as timestamp(6) with time zone) as measured_at_local,
    cast(measured_at_utc as timestamp(6) with time zone) as measured_at_utc,

    cast(measurement_date_local as date) as measurement_date_local,
    cast(measurement_date_utc as date) as measurement_date_utc,

    cast(temperature_2m as double) as temperature_2m,
    cast(relative_humidity_2m as double) as relative_humidity_2m,
    cast(precipitation as double) as precipitation,
    cast(rain as double) as rain,
    cast(surface_pressure as double) as surface_pressure,
    cast(cloud_cover as double) as cloud_cover,
    cast(wind_speed_10m as double) as wind_speed_10m,
    cast(wind_direction_10m as double) as wind_direction_10m,
    cast(visibility as double) as visibility,
    cast(weather_code as integer) as weather_code

from source_data
