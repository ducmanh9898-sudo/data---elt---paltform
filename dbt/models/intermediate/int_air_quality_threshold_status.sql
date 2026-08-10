with environment_hourly as (

    select *
    from {{ ref('int_weather_air_quality_joined') }}

),

classified as (

    select
        *,

        case
            when us_aqi is null then 'unknown'
            when us_aqi < 0 then 'invalid'
            when us_aqi <= 50 then 'good'
            when us_aqi <= 100 then 'moderate'
            when us_aqi <= 150 then 'unhealthy_for_sensitive_groups'
            when us_aqi <= 200 then 'unhealthy'
            when us_aqi <= 300 then 'very_unhealthy'
            else 'hazardous'
        end as air_quality_status,

        case
            when us_aqi is null then 0
            when us_aqi < 0 then -1
            when us_aqi <= 50 then 1
            when us_aqi <= 100 then 2
            when us_aqi <= 150 then 3
            when us_aqi <= 200 then 4
            when us_aqi <= 300 then 5
            else 6
        end as air_quality_status_rank,

        case
            when us_aqi is null or us_aqi < 0 then 'unknown'
            when us_aqi <= 100 then 'none'
            when us_aqi <= 150 then 'warning'
            when us_aqi <= 200 then 'alert'
            when us_aqi <= 300 then 'severe'
            else 'critical'
        end as air_quality_alert_level,

        case
            when us_aqi > 100 then true
            else false
        end as is_air_quality_alert

    from environment_hourly

)

select *
from classified
