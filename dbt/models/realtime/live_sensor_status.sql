{{ config(
    materialized='view',
    schema='realtime'
) }}

with sensor_events as (

    select *
    from {{ ref('stg_sensor_readings') }}

),

status as (

    select
        max(event_time_utc)
            as latest_event_at,

        max(processed_at)
            as latest_processed_at,

        count_if(
            processed_at >= current_timestamp - interval '1' minute
        ) as events_last_1m,

        count_if(
            processed_at >= current_timestamp - interval '5' minute
        ) as events_last_5m,

        count(
            distinct case
                when processed_at
                    >= current_timestamp - interval '5' minute
                    then city_id
            end
        ) as active_cities_last_5m

    from sensor_events

)

select
    latest_event_at,
    latest_processed_at,

    greatest(
        date_diff(
            'second',
            latest_processed_at,
            current_timestamp
        ),
        0
    ) as seconds_since_last_processed,

    events_last_1m,
    events_last_5m,
    active_cities_last_5m,

    case
        when latest_processed_at
            >= current_timestamp - interval '30' second
            then true
        else false
    end as is_live

from status