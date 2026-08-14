select *
from {{ ref('int_city_environment_hourly') }}

where
    (
        has_sensor_data = true

        and (
            sensor_event_count is null
            or sensor_device_count is null
            or sensor_data_updated_at is null
        )
    )

    or

    (
        has_sensor_data = false

        and (
            sensor_event_count is not null
            or sensor_device_count is not null
            or sensor_data_updated_at is not null
        )
    )