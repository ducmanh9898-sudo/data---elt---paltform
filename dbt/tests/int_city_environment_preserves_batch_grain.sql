with batch_keys as (

    select city_hour_key
    from {{ ref('int_air_quality_threshold_status') }}

),

integrated_keys as (

    select city_hour_key
    from {{ ref('int_city_environment_hourly') }}

),

missing_from_integrated as (

    select city_hour_key
    from batch_keys

    except

    select city_hour_key
    from integrated_keys

),

unexpected_in_integrated as (

    select city_hour_key
    from integrated_keys

    except

    select city_hour_key
    from batch_keys

)

select
    city_hour_key,
    'missing_from_integrated' as failure_reason
from missing_from_integrated

union all

select
    city_hour_key,
    'unexpected_in_integrated' as failure_reason
from unexpected_in_integrated