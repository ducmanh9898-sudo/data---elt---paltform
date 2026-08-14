{{ config(
    materialized='view',
    schema='realtime'
) }}

with sensor_events as (

    select *
    from {{ ref('stg_sensor_readings') }}

),

ranked as (

    select
        *,

        row_number() over (
            partition by city_id

            order by
                event_time_utc desc,
                processed_at desc
        ) as event_rank

    from sensor_events

)

select
    city_id,
    city_name,
    country_code,

    device_id,
    sequence_number,
    event_id,

    event_time_utc,
    processed_at,

    temperature_2m,
    relative_humidity_2m,

    pm2_5,
    pm10,
    carbon_monoxide,
    nitrogen_dioxide,

    greatest(
        date_diff(
            'second',
            processed_at,
            current_timestamp
        ),
        0
    ) as seconds_since_processed,

    case
        when processed_at
            >= current_timestamp - interval '30' second
            then true
        else false
    end as is_live

from ranked

where event_rank = 1