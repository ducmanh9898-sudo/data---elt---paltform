with source_data as (

    select *
    from {{ source('silver', 'air_quality_hourly') }}

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

    cast(pm10 as double) as pm10,
    cast(pm2_5 as double) as pm2_5,
    cast(carbon_monoxide as double) as carbon_monoxide,
    cast(nitrogen_dioxide as double) as nitrogen_dioxide,
    cast(sulphur_dioxide as double) as sulphur_dioxide,
    cast(ozone as double) as ozone,
    cast(us_aqi as integer) as us_aqi

from source_data
