select
    city_id,
    measured_at_utc,
    source_system,
    dataset_name,
    count(*) as duplicate_count

from {{ ref('stg_air_quality_hourly') }}

group by
    city_id,
    measured_at_utc,
    source_system,
    dataset_name

having count(*) > 1

